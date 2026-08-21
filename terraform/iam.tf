resource "aws_iam_role" "financial_dataflow" {
  name = "financial-dataflow-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = [
            "glue.amazonaws.com",
            "scheduler.amazonaws.com"
          ]
        }
        Action = "sts:AssumeRole"
        Condition = {
          StringEquals = { "aws:SourceAccount" = var.aws_account_id }
        }
      }
    ]
  })
}

resource "aws_iam_role_policy" "financial_dataflow" {
  name = "financial-dataflow-policy"
  role = aws_iam_role.financial_dataflow.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.financial_dataflow.arn,
          "${aws_s3_bucket.financial_dataflow.arn}/*",
          "arn:aws:s3:::aws-glue-assets-${var.aws_account_id}-${var.aws_region}",
          "arn:aws:s3:::aws-glue-assets-${var.aws_account_id}-${var.aws_region}/*"
        ]
      },
      {
        Sid      = "InvokeIngestionLambda"
        Effect   = "Allow"
        Action   = ["lambda:InvokeFunction"]
        Resource = [aws_lambda_function.ingestion.arn]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service_role" {
  role       = aws_iam_role.financial_dataflow.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# Separate from financial_dataflow: Lambda Destinations requires the
# invoking function's execution role to trust only lambda.amazonaws.com
# ("Role trusts too many services" if it's shared with Glue/Scheduler),
# so this role cannot be folded into the shared one above.
resource "aws_iam_role" "lambda_ingestion" {
  name = "financial-dataflow-lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = { Service = "lambda.amazonaws.com" }
        Action    = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy" "lambda_ingestion" {
  name = "financial-dataflow-lambda-policy"
  role = aws_iam_role.lambda_ingestion.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "Secrets"
        Effect   = "Allow"
        Action   = ["secretsmanager:GetSecretValue"]
        Resource = [aws_secretsmanager_secret.trading212.arn]
      },
      {
        Sid      = "S3"
        Effect   = "Allow"
        Action   = ["s3:PutObject"]
        Resource = ["${aws_s3_bucket.financial_dataflow.arn}/*"]
      },
      {
        # Lambda Destinations (OnFailure) needs the invoking function's
        # own role to allow Publish, in addition to the SNS topic's
        # resource policy granting lambda.amazonaws.com -- both sides
        # are required.
        Sid      = "PublishFailureAlerts"
        Effect   = "Allow"
        Action   = ["sns:Publish"]
        Resource = [aws_sns_topic.pipeline_alerts.arn]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_basic_execution" {
  role       = aws_iam_role.lambda_ingestion.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}
