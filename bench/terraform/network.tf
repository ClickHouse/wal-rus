# Dedicated VPC + public subnet for bench box

data "aws_availability_zones" "available" {
  state = "available"

  filter {
    name   = "opt-in-status"
    values = ["opt-in-not-required"]
  }
}

locals {
  az = data.aws_availability_zones.available.names[0]
}

resource "aws_vpc" "bench" {
  cidr_block           = "10.78.0.0/16"
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = { Name = "walrus-bench" }
}

resource "aws_internet_gateway" "bench" {
  vpc_id = aws_vpc.bench.id
  tags   = { Name = "walrus-bench" }
}

resource "aws_subnet" "bench" {
  vpc_id                  = aws_vpc.bench.id
  cidr_block              = "10.78.1.0/24"
  availability_zone       = local.az
  map_public_ip_on_launch = true

  tags = { Name = "walrus-bench-public" }
}

resource "aws_route_table" "bench" {
  vpc_id = aws_vpc.bench.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.bench.id
  }

  tags = { Name = "walrus-bench" }
}

resource "aws_route_table_association" "bench" {
  subnet_id      = aws_subnet.bench.id
  route_table_id = aws_route_table.bench.id
}

resource "aws_security_group" "bench" {
  name        = "walrus-bench-${random_id.suffix.hex}"
  description = "walrus-bench single box: SSH from my_ip, all egress"
  vpc_id      = aws_vpc.bench.id
}

resource "aws_vpc_security_group_ingress_rule" "ssh" {
  security_group_id = aws_security_group.bench.id
  description       = "SSH from operator IP"
  cidr_ipv4         = var.my_ip
  from_port         = 22
  to_port           = 22
  ip_protocol       = "tcp"
}

resource "aws_vpc_security_group_egress_rule" "all_out" {
  security_group_id = aws_security_group.bench.id
  description       = "Allow all egress (S3, apt/PGDG, IMDS, etc.)"
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
}
