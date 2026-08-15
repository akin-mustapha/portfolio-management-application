resource aws_glue_job "bronze_to_silver" {
  name     = "t212-bronze-to-silver"
  role_arn = aws_iam_role.glue.arn

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.bronze.bucket}/scripts/t212-bronze-to-silver.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"              = "python"
    "--additional-python-modules" = "awswrangler==3.*,pandas,pyarrow"
    "--TempDir"                   = "s3://${aws_s3_bucket.bronze.bucket}/temp/"
  }

  max_capacity = 0.0625
}


resource aws_glue_job "silver_to_gold" {
  name     = "t212-silver-to-gold"
  role_arn = aws_iam_role.glue.arn

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.bronze.bucket}/scripts/t212-silver-to-gold.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"              = "python"
    "--additional-python-modules" = "awswrangler==3.*,pandas,pyarrow"
    "--TempDir"                   = "s3://${aws_s3_bucket.bronze.bucket}/temp/"
  }

  max_capacity = 0.0625
}