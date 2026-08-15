resource aws_lambda "ingestion" {
  function_name = "t212-ingestion"
  role          = aws_iam_role.ingestion.arn
  handler       = "lambda_function.lambda_handler"
  runtime       = "python3.9"

  filename      = "src/ingest/t212-ingestion.py"

  source_code_hash = filebase64sha256("src/ingest/t212-ingestion.py")

  environment {
    variables = {
      S3_BUCKET_NAME = aws_s3_bucket.bronze.bucket
    }
  }
}