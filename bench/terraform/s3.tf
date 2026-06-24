resource "random_id" "suffix" {
  byte_length = 4
}

# Private bench bucket with short object lifecycle
resource "aws_s3_bucket" "bench" {
  bucket        = "walrus-bench-${random_id.suffix.hex}"
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "bench" {
  bucket = aws_s3_bucket.bench.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "bench" {
  bucket = aws_s3_bucket.bench.id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "bench" {
  bucket = aws_s3_bucket.bench.id

  rule {
    id     = "expire-bench-objects"
    status = "Enabled"

    filter {}

    expiration {
      days = 7
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }
}
