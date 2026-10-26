output "console_url" {
  description = "Open in a browser (query editor); the same URL is the host for redshift_connector.connect(host=...)"
  value       = aws_lambda_function_url.server.function_url
}

output "bucket" {
  description = "Catalog, managed tables and large results live here"
  value       = aws_s3_bucket.data.bucket
}
