resource "aws_secretsmanager_secret" "trading212" {
  name = "prod/financial-dataflow/trading212"
}
