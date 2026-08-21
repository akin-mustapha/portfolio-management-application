terraform {
  backend "s3" {
    bucket = "t212-terraform-state-861580917950"
    key = "dev/terraform.tfstate"
    region = "eu-west-1"
    use_lockfile = true
    encrypt = true
  }
}