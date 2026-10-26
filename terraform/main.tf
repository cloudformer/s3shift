data "aws_caller_identity" "me" {}

locals {
  role         = "${var.name}-lambda-role"
  bucket       = "${var.name}-${var.env}-${data.aws_caller_identity.me.account_id}"
  requirements = "${path.module}/../src/server/requirements.txt"
  layer_dir    = "${path.module}/.build/layer"
}

# ---------------- S3: catalog, managed tables, large results (expire after a day) ----------------
resource "aws_s3_bucket" "data" {
  bucket        = local.bucket
  force_destroy = true # destroy deletes managed tables too; UNLOAD anything you want to keep first
}

resource "aws_s3_bucket_public_access_block" "data" {
  bucket                  = aws_s3_bucket.data.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "data" {
  bucket = aws_s3_bucket.data.id
  rule {
    id     = "expire-large-results"
    status = "Enabled"
    filter { prefix = "results/" }
    expiration { days = 1 }
  }
}

# ---------------- IAM: own bucket read/write, your data buckets read-only ----------------
resource "aws_iam_role" "lambda" {
  name = local.role
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "lambda" {
  name = "${local.role}-policy"
  role = aws_iam_role.lambda.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = concat([
      {
        Effect   = "Allow"
        Action   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "arn:aws:logs:*:*:*"
      },
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
        Resource = "${aws_s3_bucket.data.arn}/*"
      },
      {
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = aws_s3_bucket.data.arn
      }
      ], length(var.read_buckets) == 0 ? [] : [
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = [for b in var.read_buckets : "arn:aws:s3:::${b}/*"]
      },
      {
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = [for b in var.read_buckets : "arn:aws:s3:::${b}"]
      }
    ])
  })
}

resource "aws_iam_role_policy_attachment" "vpc" {
  count      = length(var.vpc_subnet_ids) > 0 ? 1 : 0
  role       = aws_iam_role.lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
}

# ---------------- layer: duckdb + the RDS clients, downloaded for Lambda's Linux by pip ----------------
resource "terraform_data" "layer" {
  triggers_replace = filesha256(local.requirements)

  provisioner "local-exec" {
    command = <<-EOT
      rm -rf ${local.layer_dir} && mkdir -p ${local.layer_dir}/python &&
      python3 -m pip install --quiet --target ${local.layer_dir}/python -r ${local.requirements} \
        --platform manylinux_2_28_x86_64 --platform manylinux2014_x86_64 --implementation cp \
        --python-version 3.12 --only-binary=:all:
    EOT
  }
}

data "archive_file" "layer" {
  type        = "zip"
  source_dir  = local.layer_dir
  output_path = "${path.module}/.build/layer.zip"
  depends_on  = [terraform_data.layer]
}

resource "aws_lambda_layer_version" "deps" {
  layer_name          = "${var.name}-deps"
  filename            = data.archive_file.layer.output_path
  source_code_hash    = data.archive_file.layer.output_base64sha256
  compatible_runtimes = ["python3.12"]
}

# ---------------- the service: one Lambda + Function URL (console + API) ----------------
data "archive_file" "server" {
  type        = "zip"
  source_dir  = "${path.module}/../src/server"
  excludes    = ["**/__pycache__/**", "**/.DS_Store", "requirements.txt"]
  output_path = "${path.module}/.build/server.zip"
}

resource "aws_lambda_function" "server" {
  function_name    = "${var.name}-lambda"
  role             = aws_iam_role.lambda.arn
  runtime          = "python3.12"
  handler          = "lambda_function.lambda_handler"
  filename         = data.archive_file.server.output_path
  source_code_hash = data.archive_file.server.output_base64sha256
  layers           = [aws_lambda_layer_version.deps.arn]
  timeout          = 900           # Lambda's maximum: a query has 15 minutes
  memory_size      = var.memory_mb # more memory = more CPU = faster scans
  ephemeral_storage {
    size = 10240 # /tmp: the S3 file cache and DuckDB's spill space
  }

  dynamic "vpc_config" {
    for_each = length(var.vpc_subnet_ids) > 0 ? [1] : []
    content {
      subnet_ids         = var.vpc_subnet_ids
      security_group_ids = var.vpc_security_group_ids
    }
  }

  environment {
    variables = {
      BUCKET    = aws_s3_bucket.data.bucket
      USER_NAME = var.user
      PASSWORD  = var.password
      CONSOLE   = var.console ? "1" : "0"
    }
  }
}

# endpoint_auth = NONE:    public URL, our user/password is the only gate. A public URL
#                          needs BOTH permission statements (AWS requirement since Oct 2025).
# endpoint_auth = AWS_IAM: AWS verifies SigV4 + IAM first; no public permissions are created.
resource "aws_lambda_function_url" "server" {
  function_name      = aws_lambda_function.server.function_name
  authorization_type = var.endpoint_auth
  cors {
    allow_origins = ["*"]
    allow_methods = ["*"]
    allow_headers = ["content-type", "authorization"]
  }
}

resource "aws_lambda_permission" "url" {
  count                  = var.endpoint_auth == "NONE" ? 1 : 0
  statement_id           = "AllowPublicFunctionUrl"
  action                 = "lambda:InvokeFunctionUrl"
  function_name          = aws_lambda_function.server.function_name
  principal              = "*"
  function_url_auth_type = "NONE"
}

resource "aws_lambda_permission" "url_invoke" {
  count                    = var.endpoint_auth == "NONE" ? 1 : 0
  statement_id             = "AllowPublicFunctionUrlInvoke"
  action                   = "lambda:InvokeFunction"
  function_name            = aws_lambda_function.server.function_name
  principal                = "*"
  invoked_via_function_url = true
}
