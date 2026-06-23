# Instance role scoped to the bench bucket. 11_write_walg_env.sh bridges these
# IMDSv2 creds into wal-g.env (walrus has no IMDS credential chain); the aws CLI
# and pgbackrest read the instance role directly.

data "aws_iam_policy_document" "assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "bench" {
  name               = "walrus-bench-${random_id.suffix.hex}"
  assume_role_policy = data.aws_iam_policy_document.assume_role.json
}

data "aws_iam_policy_document" "bench_s3" {
  statement {
    effect = "Allow"

    actions = [
      "s3:PutObject",
      "s3:GetObject",
      "s3:ListBucket",
      "s3:DeleteObject",
      "s3:AbortMultipartUpload",
    ]

    resources = [
      aws_s3_bucket.bench.arn,
      "${aws_s3_bucket.bench.arn}/*",
    ]
  }
}

resource "aws_iam_role_policy" "bench_s3" {
  name   = "walrus-bench-s3"
  role   = aws_iam_role.bench.id
  policy = data.aws_iam_policy_document.bench_s3.json
}

resource "aws_iam_instance_profile" "bench" {
  name = "walrus-bench-${random_id.suffix.hex}"
  role = aws_iam_role.bench.name
}
