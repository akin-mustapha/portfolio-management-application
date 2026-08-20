terraform {
  required_version = "~> 1.15.8"
  required_providers {
    aws = {
      source = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

# Bronze S3 bucket for raw data storage
import {
  to = aws_s3_bucket.bronze
  id = "t212-asset"
}

# import {
#   to = module.storage.aws_s3_bucket.bronze
#   id = "t212-asset"   # this is where the real AWS bucket name belongs
# }

# module "storage" {
#   source = "../../modules/storage"
# }
import {
  to = aws_lambda_function.ingestion
  id = "t212-ingestion"   # your actual function name
}
import {
  to = aws_iam_role.ingestion
  id = "<role-name>"
}
import {
  to = aws_iam_role_policy.ingestion
  id = "<role-name>:<policy-name>"   # note the colon-separated format for inline policies
}
import {
  to = aws_cloudwatch_log_group.ingestion
  id = "/aws/lambda/t212-ingestion"
}