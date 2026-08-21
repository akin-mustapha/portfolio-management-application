resource "aws_glue_catalog_database" "financial_dataflow" {
  name = "financial_dataflow"
}

resource "aws_s3_object" "bronze_to_silver_script" {
  bucket = "aws-glue-assets-${var.aws_account_id}-${var.aws_region}"
  key    = "scripts/financial-dataflow-bronze-to-silver.py"
  source = "${path.module}/../src/transform/t212-bronze-to-silver.py"
  etag   = filemd5("${path.module}/../src/transform/t212-bronze-to-silver.py")
}

resource "aws_s3_object" "silver_to_gold_script" {
  bucket = "aws-glue-assets-${var.aws_account_id}-${var.aws_region}"
  key    = "scripts/financial-dataflow-silver-to-gold.py"
  source = "${path.module}/../src/transform/t212-silver-to-gold.py"
  etag   = filemd5("${path.module}/../src/transform/t212-silver-to-gold.py")
}

resource "aws_glue_job" "bronze_to_silver" {
  name     = "financial-dataflow-bronze-to-silver"
  role_arn = aws_iam_role.financial_dataflow.arn

  command {
    name            = "pythonshell"
    script_location = "s3://${aws_s3_object.bronze_to_silver_script.bucket}/${aws_s3_object.bronze_to_silver_script.key}"
    python_version  = "3.9"
  }

  default_arguments = {
    "--job-language" = "python"
    "--TempDir"      = "s3://aws-glue-assets-${var.aws_account_id}-${var.aws_region}/temporary/"
  }

  glue_version = "3.0"
  max_capacity = 0.0625
  timeout      = 2880
}

resource "aws_glue_job" "silver_to_gold" {
  name     = "financial-dataflow-silver-to-gold"
  role_arn = aws_iam_role.financial_dataflow.arn

  command {
    name            = "pythonshell"
    script_location = "s3://${aws_s3_object.silver_to_gold_script.bucket}/${aws_s3_object.silver_to_gold_script.key}"
    python_version  = "3.9"
  }

  default_arguments = {
    "--job-language" = "python"
    "--TempDir"      = "s3://aws-glue-assets-${var.aws_account_id}-${var.aws_region}/temporary/"
  }

  glue_version = "3.0"
  max_capacity = 0.0625
  timeout      = 2880
}
