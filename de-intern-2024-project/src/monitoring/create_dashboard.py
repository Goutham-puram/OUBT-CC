"""
Create comprehensive CloudWatch Dashboard for DE Intern Pipeline monitoring.

Dashboard Name: DE-Intern-Pipeline-Monitor
Widgets:
- S3 uploads (count/size)
- Lambda invocations and errors
- Glue job success/failure
- Step Functions execution status
- Redshift query performance
"""

import sys
import json
from typing import Dict, List, Any
import boto3
from botocore.exceptions import ClientError

# Add parent directory to path for imports
sys.path.append('/home/user/OUBT-CC/de-intern-2024-project/src')
from de_intern_2024.utils.logger import get_logger
from de_intern_2024.utils.aws_helpers import get_boto3_client

logger = get_logger(__name__)


class CloudWatchDashboardCreator:
    """
    Creates and manages CloudWatch Dashboard for pipeline monitoring.
    """

    def __init__(
        self,
        dashboard_name: str = "DE-Intern-Pipeline-Monitor",
        region: str = 'us-east-1'
    ):
        """
        Initialize CloudWatch Dashboard Creator.

        Args:
            dashboard_name: Name for the CloudWatch dashboard
            region: AWS region
        """
        self.dashboard_name = dashboard_name
        self.region = region

        # Initialize AWS clients
        self.cloudwatch_client = get_boto3_client('cloudwatch', region=region)
        self.sts_client = get_boto3_client('sts', region=region)
        self.s3_client = get_boto3_client('s3', region=region)
        self.lambda_client = get_boto3_client('lambda', region=region)
        self.glue_client = get_boto3_client('glue', region=region)

        # Get account information
        self.account_id = self.sts_client.get_caller_identity()['Account']
        self.bucket_name = f"{self.account_id}-oubt-datalake"

        logger.info(f"Initialized CloudWatchDashboardCreator")
        logger.info(f"Dashboard: {self.dashboard_name}")
        logger.info(f"Region: {self.region}")
        logger.info(f"Account ID: {self.account_id}")

    def create_s3_widgets(self) -> List[Dict[str, Any]]:
        """
        Create widgets for S3 monitoring.

        Returns:
            List of widget configurations
        """
        widgets = [
            # S3 Upload Count
            {
                "type": "metric",
                "x": 0,
                "y": 0,
                "width": 12,
                "height": 6,
                "properties": {
                    "metrics": [
                        [
                            "AWS/S3",
                            "NumberOfObjects",
                            "BucketName",
                            self.bucket_name,
                            "StorageType",
                            "AllStorageTypes",
                            {"stat": "Average", "label": "Total Objects"}
                        ]
                    ],
                    "view": "timeSeries",
                    "stacked": False,
                    "region": self.region,
                    "title": "S3 - Total Objects in Datalake",
                    "period": 300,
                    "yAxis": {
                        "left": {
                            "label": "Count"
                        }
                    }
                }
            },
            # S3 Bucket Size
            {
                "type": "metric",
                "x": 12,
                "y": 0,
                "width": 12,
                "height": 6,
                "properties": {
                    "metrics": [
                        [
                            "AWS/S3",
                            "BucketSizeBytes",
                            "BucketName",
                            self.bucket_name,
                            "StorageType",
                            "StandardStorage",
                            {"stat": "Average", "label": "Bucket Size (GB)"}
                        ]
                    ],
                    "view": "timeSeries",
                    "stacked": False,
                    "region": self.region,
                    "title": "S3 - Datalake Storage Size",
                    "period": 86400,  # Daily
                    "yAxis": {
                        "left": {
                            "label": "Bytes"
                        }
                    }
                }
            }
        ]

        return widgets

    def create_lambda_widgets(self) -> List[Dict[str, Any]]:
        """
        Create widgets for Lambda monitoring.

        Returns:
            List of widget configurations
        """
        # Get Lambda function names
        lambda_functions = []
        try:
            response = self.lambda_client.list_functions()
            lambda_functions = [
                f['FunctionName'] for f in response.get('Functions', [])
                if 'etl' in f['FunctionName'].lower() or 's3-notification' in f['FunctionName'].lower()
            ]
        except Exception as e:
            logger.warning(f"Could not list Lambda functions: {e}")
            lambda_functions = ["s3-notification-handler"]  # Default

        widgets = [
            # Lambda Invocations
            {
                "type": "metric",
                "x": 0,
                "y": 6,
                "width": 8,
                "height": 6,
                "properties": {
                    "metrics": [
                        [
                            "AWS/Lambda",
                            "Invocations",
                            "FunctionName",
                            func_name,
                            {"stat": "Sum", "label": func_name}
                        ] for func_name in lambda_functions[:5]  # Limit to 5
                    ],
                    "view": "timeSeries",
                    "stacked": False,
                    "region": self.region,
                    "title": "Lambda - Invocations",
                    "period": 300,
                    "yAxis": {
                        "left": {
                            "label": "Count"
                        }
                    }
                }
            },
            # Lambda Errors
            {
                "type": "metric",
                "x": 8,
                "y": 6,
                "width": 8,
                "height": 6,
                "properties": {
                    "metrics": [
                        [
                            "AWS/Lambda",
                            "Errors",
                            "FunctionName",
                            func_name,
                            {"stat": "Sum", "label": func_name, "color": "#d62728"}
                        ] for func_name in lambda_functions[:5]
                    ],
                    "view": "timeSeries",
                    "stacked": False,
                    "region": self.region,
                    "title": "Lambda - Errors",
                    "period": 300,
                    "yAxis": {
                        "left": {
                            "label": "Count"
                        }
                    }
                }
            },
            # Lambda Duration
            {
                "type": "metric",
                "x": 16,
                "y": 6,
                "width": 8,
                "height": 6,
                "properties": {
                    "metrics": [
                        [
                            "AWS/Lambda",
                            "Duration",
                            "FunctionName",
                            func_name,
                            {"stat": "Average", "label": func_name}
                        ] for func_name in lambda_functions[:5]
                    ],
                    "view": "timeSeries",
                    "stacked": False,
                    "region": self.region,
                    "title": "Lambda - Average Duration",
                    "period": 300,
                    "yAxis": {
                        "left": {
                            "label": "Milliseconds"
                        }
                    }
                }
            }
        ]

        return widgets

    def create_glue_widgets(self) -> List[Dict[str, Any]]:
        """
        Create widgets for Glue job monitoring.

        Returns:
            List of widget configurations
        """
        glue_job_name = "job-process-taxi-data"

        widgets = [
            # Glue Job Success/Failure
            {
                "type": "metric",
                "x": 0,
                "y": 12,
                "width": 12,
                "height": 6,
                "properties": {
                    "metrics": [
                        [
                            "AWS/Glue",
                            "glue.driver.aggregate.numCompletedTasks",
                            "JobName",
                            glue_job_name,
                            "Type",
                            "count",
                            {"stat": "Sum", "label": "Completed Tasks", "color": "#2ca02c"}
                        ],
                        [
                            "AWS/Glue",
                            "glue.driver.aggregate.numFailedTasks",
                            "JobName",
                            glue_job_name,
                            "Type",
                            "count",
                            {"stat": "Sum", "label": "Failed Tasks", "color": "#d62728"}
                        ]
                    ],
                    "view": "timeSeries",
                    "stacked": False,
                    "region": self.region,
                    "title": "Glue - Job Tasks (Success/Failure)",
                    "period": 300,
                    "yAxis": {
                        "left": {
                            "label": "Count"
                        }
                    }
                }
            },
            # Glue Job Run Status
            {
                "type": "log",
                "x": 12,
                "y": 12,
                "width": 12,
                "height": 6,
                "properties": {
                    "query": f"""SOURCE '/aws-glue/jobs/output'
| fields @timestamp, @message
| filter jobName = '{glue_job_name}'
| filter @message like /SUCCEEDED|FAILED|TIMEOUT/
| stats count() by bin(5m)""",
                    "region": self.region,
                    "title": "Glue - Job Runs (Last 24h)",
                    "stacked": False
                }
            },
            # Glue Job Execution Time
            {
                "type": "metric",
                "x": 0,
                "y": 18,
                "width": 12,
                "height": 6,
                "properties": {
                    "metrics": [
                        [
                            "AWS/Glue",
                            "glue.driver.ExecutorAllocationManager.executors.numberAllExecutors",
                            "JobName",
                            glue_job_name,
                            "Type",
                            "gauge",
                            {"stat": "Average", "label": "Active Executors"}
                        ]
                    ],
                    "view": "timeSeries",
                    "stacked": False,
                    "region": self.region,
                    "title": "Glue - Active Executors",
                    "period": 300,
                    "yAxis": {
                        "left": {
                            "label": "Count"
                        }
                    }
                }
            },
            # Glue Data Processing Metrics
            {
                "type": "metric",
                "x": 12,
                "y": 18,
                "width": 12,
                "height": 6,
                "properties": {
                    "metrics": [
                        [
                            "AWS/Glue",
                            "glue.driver.aggregate.recordsRead",
                            "JobName",
                            glue_job_name,
                            "Type",
                            "count",
                            {"stat": "Sum", "label": "Records Read"}
                        ],
                        [
                            "AWS/Glue",
                            "glue.driver.aggregate.bytesRead",
                            "JobName",
                            glue_job_name,
                            "Type",
                            "count",
                            {"stat": "Sum", "label": "Bytes Read"}
                        ]
                    ],
                    "view": "timeSeries",
                    "stacked": False,
                    "region": self.region,
                    "title": "Glue - Data Processing Volume",
                    "period": 300,
                    "yAxis": {
                        "left": {
                            "label": "Count"
                        }
                    }
                }
            }
        ]

        return widgets

    def create_stepfunctions_widgets(self) -> List[Dict[str, Any]]:
        """
        Create widgets for Step Functions monitoring.

        Returns:
            List of widget configurations
        """
        state_machine_name = "etl-pipeline-workflow"
        state_machine_arn = f"arn:aws:states:{self.region}:{self.account_id}:stateMachine:{state_machine_name}"

        widgets = [
            # Step Functions Executions
            {
                "type": "metric",
                "x": 0,
                "y": 24,
                "width": 8,
                "height": 6,
                "properties": {
                    "metrics": [
                        [
                            "AWS/States",
                            "ExecutionsStarted",
                            "StateMachineArn",
                            state_machine_arn,
                            {"stat": "Sum", "label": "Started", "color": "#1f77b4"}
                        ],
                        [
                            ".",
                            "ExecutionsSucceeded",
                            ".",
                            ".",
                            {"stat": "Sum", "label": "Succeeded", "color": "#2ca02c"}
                        ],
                        [
                            ".",
                            "ExecutionsFailed",
                            ".",
                            ".",
                            {"stat": "Sum", "label": "Failed", "color": "#d62728"}
                        ],
                        [
                            ".",
                            "ExecutionsTimedOut",
                            ".",
                            ".",
                            {"stat": "Sum", "label": "Timed Out", "color": "#ff7f0e"}
                        ]
                    ],
                    "view": "timeSeries",
                    "stacked": False,
                    "region": self.region,
                    "title": "Step Functions - Execution Status",
                    "period": 300,
                    "yAxis": {
                        "left": {
                            "label": "Count"
                        }
                    }
                }
            },
            # Step Functions Execution Time
            {
                "type": "metric",
                "x": 8,
                "y": 24,
                "width": 8,
                "height": 6,
                "properties": {
                    "metrics": [
                        [
                            "AWS/States",
                            "ExecutionTime",
                            "StateMachineArn",
                            state_machine_arn,
                            {"stat": "Average", "label": "Average Duration"}
                        ],
                        [
                            "...",
                            {"stat": "Maximum", "label": "Max Duration"}
                        ]
                    ],
                    "view": "timeSeries",
                    "stacked": False,
                    "region": self.region,
                    "title": "Step Functions - Execution Duration",
                    "period": 300,
                    "yAxis": {
                        "left": {
                            "label": "Milliseconds"
                        }
                    }
                }
            },
            # Step Functions Throttling
            {
                "type": "metric",
                "x": 16,
                "y": 24,
                "width": 8,
                "height": 6,
                "properties": {
                    "metrics": [
                        [
                            "AWS/States",
                            "ExecutionThrottled",
                            "StateMachineArn",
                            state_machine_arn,
                            {"stat": "Sum", "label": "Throttled Executions", "color": "#d62728"}
                        ]
                    ],
                    "view": "timeSeries",
                    "stacked": False,
                    "region": self.region,
                    "title": "Step Functions - Throttling",
                    "period": 300,
                    "yAxis": {
                        "left": {
                            "label": "Count"
                        }
                    }
                }
            }
        ]

        return widgets

    def create_redshift_widgets(self) -> List[Dict[str, Any]]:
        """
        Create widgets for Redshift monitoring.

        Returns:
            List of widget configurations
        """
        # Redshift Serverless workgroup name
        workgroup_name = "de-intern-workgroup"

        widgets = [
            # Redshift Query Performance
            {
                "type": "metric",
                "x": 0,
                "y": 30,
                "width": 12,
                "height": 6,
                "properties": {
                    "metrics": [
                        [
                            "AWS/Redshift-Serverless",
                            "ComputeCapacity",
                            "WorkgroupName",
                            workgroup_name,
                            {"stat": "Average", "label": "Compute Capacity (RPUs)"}
                        ]
                    ],
                    "view": "timeSeries",
                    "stacked": False,
                    "region": self.region,
                    "title": "Redshift - Compute Capacity (RPUs)",
                    "period": 300,
                    "yAxis": {
                        "left": {
                            "label": "RPUs"
                        }
                    }
                }
            },
            # Redshift Query Duration
            {
                "type": "metric",
                "x": 12,
                "y": 30,
                "width": 12,
                "height": 6,
                "properties": {
                    "metrics": [
                        [
                            "AWS/Redshift-Serverless",
                            "QueryDuration",
                            "WorkgroupName",
                            workgroup_name,
                            {"stat": "Average", "label": "Avg Query Duration"}
                        ],
                        [
                            "...",
                            {"stat": "Maximum", "label": "Max Query Duration"}
                        ]
                    ],
                    "view": "timeSeries",
                    "stacked": False,
                    "region": self.region,
                    "title": "Redshift - Query Performance",
                    "period": 300,
                    "yAxis": {
                        "left": {
                            "label": "Milliseconds"
                        }
                    }
                }
            },
            # Redshift Data Scanned
            {
                "type": "metric",
                "x": 0,
                "y": 36,
                "width": 12,
                "height": 6,
                "properties": {
                    "metrics": [
                        [
                            "AWS/Redshift-Serverless",
                            "DataScannedInBytes",
                            "WorkgroupName",
                            workgroup_name,
                            {"stat": "Sum", "label": "Data Scanned"}
                        ]
                    ],
                    "view": "timeSeries",
                    "stacked": False,
                    "region": self.region,
                    "title": "Redshift - Data Scanned",
                    "period": 300,
                    "yAxis": {
                        "left": {
                            "label": "Bytes"
                        }
                    }
                }
            },
            # Redshift Query Count
            {
                "type": "metric",
                "x": 12,
                "y": 36,
                "width": 12,
                "height": 6,
                "properties": {
                    "metrics": [
                        [
                            "AWS/Redshift-Serverless",
                            "Queries",
                            "WorkgroupName",
                            workgroup_name,
                            {"stat": "Sum", "label": "Total Queries"}
                        ]
                    ],
                    "view": "timeSeries",
                    "stacked": False,
                    "region": self.region,
                    "title": "Redshift - Query Count",
                    "period": 300,
                    "yAxis": {
                        "left": {
                            "label": "Count"
                        }
                    }
                }
            }
        ]

        return widgets

    def create_dashboard_body(self) -> Dict[str, Any]:
        """
        Create complete dashboard body with all widgets.

        Returns:
            Dashboard body as dictionary
        """
        all_widgets = []

        # Add all widget sections
        all_widgets.extend(self.create_s3_widgets())
        all_widgets.extend(self.create_lambda_widgets())
        all_widgets.extend(self.create_glue_widgets())
        all_widgets.extend(self.create_stepfunctions_widgets())
        all_widgets.extend(self.create_redshift_widgets())

        # Add text widget header
        header_widget = {
            "type": "text",
            "x": 0,
            "y": 0,
            "width": 24,
            "height": 1,
            "properties": {
                "markdown": f"# DE Intern Pipeline Monitor\n**Account:** {self.account_id} | **Region:** {self.region} | **Last Updated:** Auto-refresh"
            }
        }

        # Insert header at the beginning and adjust y positions
        for widget in all_widgets:
            widget['y'] += 1

        all_widgets.insert(0, header_widget)

        dashboard_body = {
            "widgets": all_widgets
        }

        return dashboard_body

    def create_dashboard(self) -> bool:
        """
        Create or update CloudWatch dashboard.

        Returns:
            True if successful
        """
        try:
            logger.info(f"Creating CloudWatch dashboard: {self.dashboard_name}")

            # Create dashboard body
            dashboard_body = self.create_dashboard_body()

            # Create/update dashboard
            response = self.cloudwatch_client.put_dashboard(
                DashboardName=self.dashboard_name,
                DashboardBody=json.dumps(dashboard_body)
            )

            logger.info(f"Successfully created dashboard: {self.dashboard_name}")
            logger.info(f"Dashboard URL: https://console.aws.amazon.com/cloudwatch/home?region={self.region}#dashboards:name={self.dashboard_name}")

            return True

        except Exception as e:
            logger.error(f"Failed to create dashboard: {e}", exc_info=True)
            return False

    def delete_dashboard(self) -> bool:
        """
        Delete CloudWatch dashboard.

        Returns:
            True if successful
        """
        try:
            logger.info(f"Deleting CloudWatch dashboard: {self.dashboard_name}")

            self.cloudwatch_client.delete_dashboards(
                DashboardNames=[self.dashboard_name]
            )

            logger.info(f"Successfully deleted dashboard: {self.dashboard_name}")
            return True

        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code == 'ResourceNotFound':
                logger.warning(f"Dashboard does not exist: {self.dashboard_name}")
                return True
            else:
                logger.error(f"Failed to delete dashboard: {e}")
                return False

    def get_dashboard_info(self) -> Dict[str, Any]:
        """
        Get information about the dashboard.

        Returns:
            Dashboard information
        """
        try:
            response = self.cloudwatch_client.get_dashboard(
                DashboardName=self.dashboard_name
            )

            return {
                'name': response['DashboardName'],
                'arn': response['DashboardArn'],
                'body': json.loads(response['DashboardBody']),
                'widget_count': len(json.loads(response['DashboardBody'])['widgets'])
            }

        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code == 'ResourceNotFound':
                logger.warning(f"Dashboard does not exist: {self.dashboard_name}")
                return None
            else:
                raise


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(
        description='Create CloudWatch Dashboard for DE Intern Pipeline'
    )
    parser.add_argument(
        '--dashboard-name',
        type=str,
        default='DE-Intern-Pipeline-Monitor',
        help='Dashboard name (default: DE-Intern-Pipeline-Monitor)'
    )
    parser.add_argument(
        '--region',
        type=str,
        default='us-east-1',
        help='AWS region (default: us-east-1)'
    )
    parser.add_argument(
        '--delete',
        action='store_true',
        help='Delete the dashboard'
    )
    parser.add_argument(
        '--info',
        action='store_true',
        help='Get dashboard information'
    )

    args = parser.parse_args()

    try:
        creator = CloudWatchDashboardCreator(
            dashboard_name=args.dashboard_name,
            region=args.region
        )

        if args.delete:
            success = creator.delete_dashboard()
            sys.exit(0 if success else 1)
        elif args.info:
            info = creator.get_dashboard_info()
            if info:
                print(json.dumps(info, indent=2, default=str))
                sys.exit(0)
            else:
                logger.error("Dashboard does not exist")
                sys.exit(1)
        else:
            success = creator.create_dashboard()
            sys.exit(0 if success else 1)

    except Exception as e:
        logger.error(f"Failed to manage dashboard: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
