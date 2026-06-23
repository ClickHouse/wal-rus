output "public_ip" {
  description = "Public IP of the bench box (SSH + scp target)."
  value       = aws_instance.bench.public_ip
}

output "bucket_name" {
  description = "S3 bucket name (BUCKET in config.env; setup default WALG_S3_PREFIX = s3://<bucket>/walg-bench, runs scope it per tool+run)."
  value       = aws_s3_bucket.bench.id
}

output "ssh_key_path" {
  description = "Local path to the generated SSH private key (mode 0600)."
  value       = local_sensitive_file.ssh_key.filename
}

output "ssh_user" {
  description = "SSH login user for the Ubuntu 24.04 AMI."
  value       = "ubuntu"
}

output "region" {
  description = "AWS region of the bench resources."
  value       = var.region
}
