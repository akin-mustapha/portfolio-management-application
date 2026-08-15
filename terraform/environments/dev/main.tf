terraform {
  required_version = "~> 1.15.8"
  required_providers {
    aws = {
      source = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}


import {
  to = aws_s3_bucket.bronze
  id = "t212-asset"
}


import {
  to = module.storage.aws_s3_bucket.bronze
  id = "t212-asset"   # this is where the real AWS bucket name belongs
}

module "storage" {
  source = "../../modules/storage"
}