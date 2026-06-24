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

output "walrus_src_remote" {
  description = "Where walrus_src_tarball landed on the box (unpacked tree + kept tarball), or a hint when disabled."
  value       = var.walrus_src_tarball != "" ? "unpacked to /home/ubuntu/walrus; tarball at /home/ubuntu/walrus-src.tar.gz (point WALRUS_SRC_TARBALL there)" : "(set -var walrus_src_tarball=... to upload source)"
}

output "next_steps" {
  description = "What to do after apply."
  value = var.run_setup ? "Box bootstrapped. SSH in: ssh -i ${local_sensitive_file.ssh_key.filename} ubuntu@${aws_instance.bench.public_ip}; then cd walrus/bench && bash scripts/sut/40_smoke_test.sh, seed the DB, run matrix.sh." : (
    var.walrus_src_tarball != "" ? "Source uploaded. SSH in, cd walrus/bench, fill config.env, run sudo ./setup.sh (or re-apply with -var run_setup=true)." : "Bare box. Get the harness onto it (set walrus_src_tarball, or scp/clone), then run setup.sh."
  )
}
