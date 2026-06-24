# Single-box IaC for in-repo bench

variable "region" {
  description = "AWS region for all bench resources."
  type        = string
  default     = "us-east-1"
}

variable "profile" {
  description = "AWS CLI named profile used for provisioning. Override per your setup with terraform.tfvars."
  type        = string
  default     = "default"
}

variable "instance_type" {
  description = "All-in-one bench box: PG18 + wal-g/walrus daemons + local pgbench driver. Needs a local NVMe instance-store (the 'd' family) for /dat (PGDATA, WAL, restore)."
  type        = string
  default     = "m5d.2xlarge"
}

variable "my_ip" {
  description = "Your public IP in CIDR form (e.g. 203.0.113.4/32) allowed to SSH on port 22."
  type        = string

  validation {
    condition     = can(cidrhost(var.my_ip, 0))
    error_message = "my_ip must be a valid CIDR, e.g. 203.0.113.4/32."
  }
}

variable "walrus_src_tarball" {
  description = "Local path to a walrus source tarball (from bench/scripts/make_source_tarball.sh) to upload + unpack on the box, so it needs no git checkout. Empty disables upload (build from an in-place checkout instead)."
  type        = string
  default     = ""

  validation {
    condition     = var.walrus_src_tarball == "" || fileexists(var.walrus_src_tarball)
    error_message = "walrus_src_tarball must be empty or point at an existing file; build it with bench/scripts/make_source_tarball.sh."
  }
}

variable "run_setup" {
  description = "After uploading the tarball, generate config.env and run setup.sh to bootstrap the box (PG18 + build all tools + systemd units). Requires walrus_src_tarball and pg_password. S3 creds come from the instance profile via IMDS, so no AWS keys are written."
  type        = bool
  default     = false
}

variable "pg_user" {
  description = "Bench Postgres role created by setup.sh (PGUSER in config.env)."
  type        = string
  default     = "walbench"
}

variable "pg_password" {
  description = "Password for the bench Postgres role (PGPASSWORD). Required when run_setup is true; never echoed."
  type        = string
  default     = ""
  sensitive   = true
}

variable "upload_concurrency" {
  description = "UPLOAD_CONCURRENCY in config.env (wal-g concurrency / pgbackrest process-max)."
  type        = number
  default     = 4
}
