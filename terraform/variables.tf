# ---- deploy credentials ----
variable "region" {
  type    = string
  default = "us-east-2"
}

variable "aws_profile" {
  description = "Local AWS CLI profile to use; \"\" = default credential chain"
  type        = string
  default     = ""
}

variable "assume_role_arn" {
  description = "Deploy role to assume; \"\" = none. Pick this OR aws_profile."
  type        = string
  default     = ""

  validation {
    condition     = !(var.assume_role_arn != "" && var.aws_profile != "")
    error_message = "Set either aws_profile or assume_role_arn, not both."
  }
}

# ---- app ----
variable "env" {
  description = "Environment label (tags, resource names)"
  type        = string
  default     = "dev"
}

variable "name" {
  description = "Project name; resources are <name>-lambda, <name>-lambda-role"
  type        = string
  default     = "s3shift"
}

# ---- the service's credentials (HTTP Basic on the Function URL) ----
variable "user" {
  type = string
}

variable "password" {
  type      = string
  sensitive = true
}

# ---- the endpoint's gate ----
variable "endpoint_auth" {
  description = <<-EOT
    How the Function URL is protected:
      NONE    = public URL; only user/password (HTTP Basic) is checked      <- default
      AWS_IAM = AWS checks a SigV4 signature + IAM permission first. The browser console
                cannot sign requests, so it is unusable in this mode.
  EOT
  type        = string
  default     = "NONE"

  validation {
    condition     = contains(["NONE", "AWS_IAM"], var.endpoint_auth)
    error_message = "endpoint_auth must be NONE or AWS_IAM."
  }
}

variable "console" {
  description = "Serve the built-in web console at GET /. false = API only."
  type        = bool
  default     = true
}

# ---- s3shift ----
variable "memory_mb" {
  description = "Lambda memory: also the CPU share and the largest query DuckDB can hold (up to 10240)"
  type        = number
  default     = 3008
}

variable "read_buckets" {
  description = "Your data buckets s3shift may read (external tables, COPY sources), e.g. [\"my-data-lake\"]"
  type        = list(string)
  default     = []
}

variable "vpc_subnet_ids" {
  description = "Private subnets, only for CREATE EXTERNAL SCHEMA against a private RDS. The Lambda then needs an S3 gateway endpoint (or NAT) to reach S3."
  type        = list(string)
  default     = []
}

variable "vpc_security_group_ids" {
  description = "Security groups for the VPC config (allowed by the RDS security group on 3306 / 5432)"
  type        = list(string)
  default     = []
}
