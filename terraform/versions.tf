terraform {

  required_version = ">= 1.15"

  required_providers {
    aws = {
      source = "hashicorp/aws"
      version = "~> 6.0"
    }

    archive = {
      source = "hashicorp/archive"
      version = "~> 2.4"
    }
  }

  backend "s3" {
    bucket = "t212-terraform-state-861580917950"
    key    = "financial_dataflow/terraform.tfstate"
    region = "eu-west-1"
    encrypt = true
    use_lockfile = true
  }
}

provider "aws" {
  region = var.aws_region
}