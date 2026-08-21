data "archive_file" "ingestion" {
  type             = "zip"
  source_file      = "${path.module}/../src/ingestion/ingestion.py"
  output_path      = "${path.module}/build/financial-dataflow-ingestion.zip"
  output_file_mode = "0666"
}

resource "aws_lambda_function" "ingestion" {
  function_name = "financial-dataflow-ingestion"
  role          = aws_iam_role.financial_dataflow.arn
  handler       = "ingestion.lambda_handler"
  runtime       = "python3.14"
  timeout       = 30
  memory_size   = 128

  filename         = data.archive_file.ingestion.output_path
  source_code_hash = data.archive_file.ingestion.output_base64sha256
}
