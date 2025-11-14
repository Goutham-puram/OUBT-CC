# AWS Step Functions State Machine for ETL Pipeline

resource "aws_sfn_state_machine" "etl_pipeline" {
  name     = "${var.project_name}-etl-pipeline"
  role_arn = aws_iam_role.step_functions_role.arn

  definition = jsonencode({
    Comment = "ETL Pipeline for NYC Taxi Data"
    StartAt = "StartCrawler"
    States = {
      StartCrawler = {
        Type     = "Task"
        Resource = "arn:aws:states:::aws-sdk:glue:startCrawler"
        Parameters = {
          Name = aws_glue_crawler.raw_taxi_data.name
        }
        Next = "WaitForCrawler"
        Catch = [
          {
            ErrorEquals = ["States.ALL"]
            Next        = "CrawlerFailed"
          }
        ]
      }

      WaitForCrawler = {
        Type    = "Wait"
        Seconds = 30
        Next    = "CheckCrawlerStatus"
      }

      CheckCrawlerStatus = {
        Type     = "Task"
        Resource = "arn:aws:states:::aws-sdk:glue:getCrawler"
        Parameters = {
          Name = aws_glue_crawler.raw_taxi_data.name
        }
        Next = "IsCrawlerComplete"
      }

      IsCrawlerComplete = {
        Type = "Choice"
        Choices = [
          {
            Variable      = "$.Crawler.State"
            StringEquals  = "READY"
            Next          = "StartRawToProcessed"
          }
        ]
        Default = "WaitForCrawler"
      }

      StartRawToProcessed = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.raw_to_processed.name
        }
        Next = "StartProcessedToCurated"
        Catch = [
          {
            ErrorEquals = ["States.ALL"]
            Next        = "JobFailed"
          }
        ]
      }

      StartProcessedToCurated = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.processed_to_curated.name
        }
        Next = "PipelineSuccess"
        Catch = [
          {
            ErrorEquals = ["States.ALL"]
            Next        = "JobFailed"
          }
        ]
      }

      PipelineSuccess = {
        Type = "Succeed"
      }

      CrawlerFailed = {
        Type  = "Fail"
        Error = "CrawlerFailed"
        Cause = "The Glue Crawler failed to complete"
      }

      JobFailed = {
        Type  = "Fail"
        Error = "GlueJobFailed"
        Cause = "One or more Glue jobs failed to complete"
      }
    }
  })

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.step_functions.arn}:*"
    include_execution_data = true
    level                  = "ALL"
  }

  tags = local.common_tags
}

# CloudWatch Log Group for Step Functions
resource "aws_cloudwatch_log_group" "step_functions" {
  name              = "/aws/stepfunctions/${var.project_name}-etl-pipeline"
  retention_in_days = 7

  tags = local.common_tags
}

# EventBridge Rule to trigger Step Functions (optional - scheduled execution)
resource "aws_cloudwatch_event_rule" "daily_etl" {
  name                = "${var.project_name}-daily-etl"
  description         = "Trigger ETL pipeline daily"
  schedule_expression = "cron(0 2 * * ? *)" # 2 AM UTC daily
  is_enabled          = false # Disabled by default

  tags = local.common_tags
}

resource "aws_cloudwatch_event_target" "step_functions" {
  rule      = aws_cloudwatch_event_rule.daily_etl.name
  target_id = "StepFunctionsTarget"
  arn       = aws_sfn_state_machine.etl_pipeline.arn
  role_arn  = aws_iam_role.eventbridge_role.arn
}
