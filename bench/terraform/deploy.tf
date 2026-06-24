# Ship the walrus source tarball to the box over SSH and unpack it.
#
# The git-archive tarball is self-contained: the whole repo (walrus source +
# this bench harness) under a walrus/ prefix, so the box needs no git checkout.
# After apply: cd ~/walrus/bench, set WALRUS_SRC_TARBALL=~/walrus-src.tar.gz in
# config.env (03_build_walrus.sh recovers the commit id from it), run setup.sh.
#
# Enabled only when var.walrus_src_tarball is set; re-runs when its bytes change.
resource "terraform_data" "walrus_src" {
  count = var.walrus_src_tarball != "" ? 1 : 0

  triggers_replace = {
    instance = aws_instance.bench.id
    tarball  = filesha256(var.walrus_src_tarball)
  }

  connection {
    type        = "ssh"
    host        = aws_instance.bench.public_ip
    user        = "ubuntu"
    private_key = tls_private_key.bench.private_key_pem
  }

  provisioner "file" {
    source      = var.walrus_src_tarball
    destination = "/home/ubuntu/walrus-src.tar.gz"
  }

  # Unpack for the harness; keep the tarball for the SHA-preserving build
  provisioner "remote-exec" {
    inline = [
      "rm -rf /home/ubuntu/walrus",
      "tar -xzf /home/ubuntu/walrus-src.tar.gz -C /home/ubuntu",
    ]
  }
}

# Bootstrap the unpacked box: write config.env, run setup.sh (PG18 + build all
# tools + systemd units). Opt-in via run_setup; re-runs when source or a config
# knob changes (a password-only change won't retrigger — taint to force).
resource "terraform_data" "bootstrap" {
  count = var.run_setup && var.walrus_src_tarball != "" ? 1 : 0

  lifecycle {
    precondition {
      condition     = var.pg_password != ""
      error_message = "run_setup requires pg_password (PGPASSWORD for the bench role)."
    }
  }

  triggers_replace = {
    src                = terraform_data.walrus_src[0].id
    bucket             = aws_s3_bucket.bench.id
    region             = var.region
    pg_user            = var.pg_user
    upload_concurrency = var.upload_concurrency
  }

  connection {
    type        = "ssh"
    host        = aws_instance.bench.public_ip
    user        = "ubuntu"
    private_key = tls_private_key.bench.private_key_pem
  }

  provisioner "file" {
    content = templatefile("${path.module}/config.env.tftpl", {
      bucket             = aws_s3_bucket.bench.id
      region             = var.region
      pg_user            = var.pg_user
      pg_password        = var.pg_password
      upload_concurrency = var.upload_concurrency
    })
    destination = "/home/ubuntu/walrus/bench/config.env"
  }

  # m5d local NVMe is mounted by 00_mount_nvme.sh, so no SKIP_MOUNT
  provisioner "remote-exec" {
    inline = [
      "cd /home/ubuntu/walrus/bench && sudo bash setup.sh",
    ]
  }
}
