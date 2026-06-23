# Single-box IaC for the in-repo bench (driver == SUT == one host). The external
# multi-replica fleet lives elsewhere; this provisions exactly one all-in-one box
# that runs PG18 + the archivers + pgbench locally, per the bench/ README.

variable "region" {
  description = "AWS region for all bench resources."
  type        = string
  default     = "us-east-1"
}

variable "profile" {
  description = "AWS CLI named profile used for provisioning."
  type        = string
  default     = "pg-dev-postgresqladmindev"
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
