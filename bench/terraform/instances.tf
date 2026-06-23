data "aws_ami" "ubuntu_noble" {
  most_recent = true
  owners      = ["099720109477"] # Canonical

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-*"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }

  filter {
    name   = "architecture"
    values = ["x86_64"]
  }

  filter {
    name   = "root-device-type"
    values = ["ebs"]
  }
}

resource "tls_private_key" "bench" {
  algorithm = "RSA"
  rsa_bits  = 4096
}

resource "aws_key_pair" "bench" {
  key_name   = "walrus-bench-${random_id.suffix.hex}"
  public_key = tls_private_key.bench.public_key_openssh
}

resource "local_sensitive_file" "ssh_key" {
  content         = tls_private_key.bench.private_key_pem
  filename        = "${path.module}/walrus_bench_key.pem"
  file_permission = "0600"
}

# All-in-one bench box: PG18 + wal-g/walrus daemons + local pgbench driver.
# The 'd' instance family ships a local NVMe instance-store; 00_mount_nvme.sh
# detects the non-root NVMe, mkfs.ext4, and mounts it at /dat (PGDATA + WAL +
# restore). Root holds the Go/Rust toolchains + build trees.
resource "aws_instance" "bench" {
  ami                    = data.aws_ami.ubuntu_noble.id
  instance_type          = var.instance_type
  availability_zone      = local.az
  subnet_id              = aws_subnet.bench.id
  vpc_security_group_ids = [aws_security_group.bench.id]
  key_name               = aws_key_pair.bench.key_name
  iam_instance_profile   = aws_iam_instance_profile.bench.name

  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required"
    http_put_response_hop_limit = 1
  }

  root_block_device {
    volume_type = "gp3"
    volume_size = 60
  }

  tags = {
    Name = "walrus-bench"
    Role = "sut"
  }
}
