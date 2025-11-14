"""
CloudWatch Log Insights queries for DE Intern Pipeline analysis.

Queries:
- Failed record analysis
- Processing time trends
- Error pattern detection
- Lambda performance analysis
- Glue job metrics
"""

import sys
import json
from typing import Dict, List, Any, Optional
import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timedelta
import time

# Add parent directory to path for imports
sys.path.append('/home/user/OUBT-CC/de-intern-2024-project/src')
from de_intern_2024.utils.logger import get_logger
from de_intern_2024.utils.aws_helpers import get_boto3_client

logger = get_logger(__name__)


class CloudWatchLogQueries:
    """
    Manages CloudWatch Log Insights queries for pipeline analysis.
    """

    def __init__(self, region: str = 'us-east-1'):
        """
        Initialize CloudWatch Log Queries.

        Args:
            region: AWS region
        """
        self.region = region

        # Initialize AWS clients
        self.logs_client = get_boto3_client('logs', region=region)
        self.sts_client = get_boto3_client('sts', region=region)

        # Get account information
        self.account_id = self.sts_client.get_caller_identity()['Account']

        # Common log groups
        self.lambda_log_group = "/aws/lambda/s3-notification-handler"
        self.glue_log_group = "/aws-glue/jobs/output"
        self.stepfunctions_log_group = f"/aws/vendedlogs/states/etl-pipeline-workflow"

        logger.info(f"Initialized CloudWatchLogQueries")
        logger.info(f"Region: {self.region}")
        logger.info(f"Account ID: {self.account_id}")

    # Predefined query templates
    QUERIES = {
        'failed_records': {
            'name': 'Failed Records Analysis',
            'description': 'Analyze failed records and error patterns',
            'query': """
fields @timestamp, @message
| filter @message like /ERROR|FAIL|Exception/
| parse @message /(?<error_type>ERROR|FAIL|Exception): (?<error_message>.*)/
| stats count() by error_type, error_message
| sort count desc
| limit 20
""",
            'log_groups': ['/aws/lambda/*', '/aws-glue/*']
        },

        'processing_time_trends': {
            'name': 'Processing Time Trends',
            'description': 'Analyze processing time trends over time',
            'query': """
fields @timestamp, @message, @duration
| filter @type = "REPORT"
| stats avg(@duration), max(@duration), min(@duration), pct(@duration, 50), pct(@duration, 95) by bin(5m)
""",
            'log_groups': ['/aws/lambda/*']
        },

        'error_patterns': {
            'name': 'Error Pattern Detection',
            'description': 'Detect common error patterns and their frequency',
            'query': """
fields @timestamp, @message
| filter @message like /ERROR|Exception|Error/
| parse @message /(?<error_class>\\w+Exception): (?<error_detail>.*)/
| stats count() as error_count by error_class
| sort error_count desc
| limit 10
""",
            'log_groups': ['/aws/lambda/*', '/aws-glue/*']
        },

        'lambda_performance': {
            'name': 'Lambda Performance Analysis',
            'description': 'Analyze Lambda function performance metrics',
            'query': """
filter @type = "REPORT"
| fields @requestId, @billedDuration, @memorySize, @maxMemoryUsed
| stats
    avg(@billedDuration) as avg_duration,
    max(@billedDuration) as max_duration,
    avg(@maxMemoryUsed/@memorySize) * 100 as avg_memory_utilization
by bin(5m)
""",
            'log_groups': ['/aws/lambda/*']
        },

        'lambda_errors_detailed': {
            'name': 'Lambda Errors Detailed',
            'description': 'Detailed analysis of Lambda function errors',
            'query': """
fields @timestamp, @requestId, @message
| filter @message like /ERROR|Error|error/
| sort @timestamp desc
| limit 50
""",
            'log_groups': ['/aws/lambda/*']
        },

        'glue_job_metrics': {
            'name': 'Glue Job Metrics',
            'description': 'Analyze Glue job execution metrics',
            'query': """
fields @timestamp, @message
| filter @message like /Job run succeeded|Job run failed|records processed/
| parse @message /records processed: (?<records_count>\\d+)/
| parse @message /execution time: (?<exec_time>\\d+)/
| stats sum(records_count) as total_records, avg(exec_time) as avg_exec_time by bin(1h)
""",
            'log_groups': ['/aws-glue/*']
        },

        'glue_job_failures': {
            'name': 'Glue Job Failures',
            'description': 'Analyze Glue job failures and error messages',
            'query': """
fields @timestamp, @message
| filter @message like /FAILED|Exception|Error/
| parse @message /(?<error_message>.*)/
| stats count() as failure_count by error_message
| sort failure_count desc
| limit 20
""",
            'log_groups': ['/aws-glue/*']
        },

        's3_upload_tracking': {
            'name': 'S3 Upload Tracking',
            'description': 'Track S3 file uploads processed by Lambda',
            'query': """
fields @timestamp, @message
| filter @message like /S3_FILE_UPLOADED/
| parse @message /"key": "(?<file_key>[^"]+)"/
| parse @message /"size_mb": (?<size_mb>[\\d.]+)/
| stats sum(size_mb) as total_mb, count() as file_count by bin(1h)
""",
            'log_groups': ['/aws/lambda/s3-notification-handler']
        },

        'data_quality_issues': {
            'name': 'Data Quality Issues',
            'description': 'Identify data quality issues and validation failures',
            'query': """
fields @timestamp, @message
| filter @message like /validation|quality|invalid|missing/
| parse @message /(?<issue_type>validation|quality|invalid|missing)[^:]*: (?<issue_detail>.*)/
| stats count() as issue_count by issue_type
| sort issue_count desc
""",
            'log_groups': ['/aws-glue/*', '/aws/lambda/*']
        },

        'stepfunctions_execution': {
            'name': 'Step Functions Execution Analysis',
            'description': 'Analyze Step Functions workflow executions',
            'query': """
fields @timestamp, @message, execution_status, execution_arn
| filter @message like /ExecutionStarted|ExecutionSucceeded|ExecutionFailed/
| stats count() by execution_status, bin(1h)
""",
            'log_groups': ['/aws/vendedlogs/states/*']
        },

        'cost_optimization': {
            'name': 'Cost Optimization Insights',
            'description': 'Identify potential cost optimization opportunities',
            'query': """
filter @type = "REPORT"
| fields @requestId, @billedDuration, @memorySize, @maxMemoryUsed
| filter @maxMemoryUsed < (@memorySize * 0.5)
| stats count() as underutilized_invocations,
        avg(@maxMemoryUsed/@memorySize) * 100 as avg_memory_util
| sort underutilized_invocations desc
""",
            'log_groups': ['/aws/lambda/*']
        },

        'hourly_activity': {
            'name': 'Hourly Activity Summary',
            'description': 'Summary of pipeline activity by hour',
            'query': """
fields @timestamp, @message
| stats count() as event_count by bin(1h)
| sort @timestamp desc
""",
            'log_groups': ['/aws/lambda/*', '/aws-glue/*']
        }
    }

    def list_log_groups(self, prefix: str = '/aws/') -> List[str]:
        """
        List CloudWatch log groups.

        Args:
            prefix: Prefix to filter log groups

        Returns:
            List of log group names
        """
        try:
            logger.info(f"Listing log groups with prefix: {prefix}")

            log_groups = []
            paginator = self.logs_client.get_paginator('describe_log_groups')

            for page in paginator.paginate(logGroupNamePrefix=prefix):
                for log_group in page.get('logGroups', []):
                    log_groups.append(log_group['logGroupName'])

            logger.info(f"Found {len(log_groups)} log groups")
            return log_groups

        except Exception as e:
            logger.error(f"Failed to list log groups: {e}")
            return []

    def run_query(
        self,
        query_name: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 100
    ) -> Dict[str, Any]:
        """
        Run a predefined CloudWatch Insights query.

        Args:
            query_name: Name of the query from QUERIES dictionary
            start_time: Start time for query (default: 1 hour ago)
            end_time: End time for query (default: now)
            limit: Maximum number of results

        Returns:
            Query results dictionary
        """
        if query_name not in self.QUERIES:
            raise ValueError(f"Unknown query: {query_name}. Available: {list(self.QUERIES.keys())}")

        query_config = self.QUERIES[query_name]

        # Default time range: last 1 hour
        if start_time is None:
            start_time = datetime.now() - timedelta(hours=1)
        if end_time is None:
            end_time = datetime.now()

        # Convert to Unix timestamps
        start_timestamp = int(start_time.timestamp())
        end_timestamp = int(end_time.timestamp())

        try:
            logger.info(f"Running query: {query_config['name']}")
            logger.info(f"Time range: {start_time} to {end_time}")

            # Get log groups for this query
            log_groups = []
            for pattern in query_config['log_groups']:
                if '*' in pattern:
                    # Expand wildcard
                    prefix = pattern.replace('*', '')
                    log_groups.extend(self.list_log_groups(prefix))
                else:
                    log_groups.append(pattern)

            if not log_groups:
                logger.warning(f"No log groups found for query: {query_name}")
                return {
                    'query_name': query_name,
                    'status': 'no_log_groups',
                    'results': []
                }

            logger.info(f"Querying log groups: {log_groups}")

            # Start query
            response = self.logs_client.start_query(
                logGroupNames=log_groups[:20],  # Limit to 20 log groups
                startTime=start_timestamp,
                endTime=end_timestamp,
                queryString=query_config['query'],
                limit=limit
            )

            query_id = response['queryId']
            logger.info(f"Query started: {query_id}")

            # Poll for results
            max_attempts = 30
            attempt = 0

            while attempt < max_attempts:
                time.sleep(1)
                attempt += 1

                result = self.logs_client.get_query_results(queryId=query_id)
                status = result['status']

                if status == 'Complete':
                    logger.info(f"Query completed successfully")

                    return {
                        'query_name': query_name,
                        'query_id': query_id,
                        'status': status,
                        'statistics': result.get('statistics', {}),
                        'results': result.get('results', []),
                        'result_count': len(result.get('results', []))
                    }
                elif status == 'Failed':
                    logger.error(f"Query failed")
                    return {
                        'query_name': query_name,
                        'query_id': query_id,
                        'status': status,
                        'error': 'Query execution failed'
                    }
                elif status in ['Running', 'Scheduled']:
                    logger.debug(f"Query status: {status} (attempt {attempt}/{max_attempts})")
                    continue
                else:
                    logger.warning(f"Unknown query status: {status}")
                    break

            # Timeout
            logger.warning(f"Query timed out after {max_attempts} attempts")
            return {
                'query_name': query_name,
                'query_id': query_id,
                'status': 'Timeout',
                'error': 'Query execution timed out'
            }

        except Exception as e:
            logger.error(f"Failed to run query: {e}", exc_info=True)
            return {
                'query_name': query_name,
                'status': 'Error',
                'error': str(e)
            }

    def run_custom_query(
        self,
        query_string: str,
        log_groups: List[str],
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 100
    ) -> Dict[str, Any]:
        """
        Run a custom CloudWatch Insights query.

        Args:
            query_string: CloudWatch Insights query string
            log_groups: List of log group names
            start_time: Start time for query (default: 1 hour ago)
            end_time: End time for query (default: now)
            limit: Maximum number of results

        Returns:
            Query results dictionary
        """
        # Default time range: last 1 hour
        if start_time is None:
            start_time = datetime.now() - timedelta(hours=1)
        if end_time is None:
            end_time = datetime.now()

        # Convert to Unix timestamps
        start_timestamp = int(start_time.timestamp())
        end_timestamp = int(end_time.timestamp())

        try:
            logger.info(f"Running custom query on {len(log_groups)} log group(s)")

            # Start query
            response = self.logs_client.start_query(
                logGroupNames=log_groups[:20],  # Limit to 20 log groups
                startTime=start_timestamp,
                endTime=end_timestamp,
                queryString=query_string,
                limit=limit
            )

            query_id = response['queryId']
            logger.info(f"Query started: {query_id}")

            # Poll for results
            max_attempts = 30
            attempt = 0

            while attempt < max_attempts:
                time.sleep(1)
                attempt += 1

                result = self.logs_client.get_query_results(queryId=query_id)
                status = result['status']

                if status == 'Complete':
                    logger.info(f"Query completed successfully")
                    return {
                        'query_id': query_id,
                        'status': status,
                        'statistics': result.get('statistics', {}),
                        'results': result.get('results', []),
                        'result_count': len(result.get('results', []))
                    }
                elif status == 'Failed':
                    logger.error(f"Query failed")
                    return {
                        'query_id': query_id,
                        'status': status,
                        'error': 'Query execution failed'
                    }
                elif status in ['Running', 'Scheduled']:
                    continue

            # Timeout
            return {
                'query_id': query_id,
                'status': 'Timeout',
                'error': 'Query execution timed out'
            }

        except Exception as e:
            logger.error(f"Failed to run custom query: {e}", exc_info=True)
            return {
                'status': 'Error',
                'error': str(e)
            }

    def format_results(self, results: List[List[Dict[str, str]]]) -> str:
        """
        Format query results for display.

        Args:
            results: Query results from CloudWatch Insights

        Returns:
            Formatted results string
        """
        if not results:
            return "No results found"

        output = []

        for row in results:
            row_dict = {}
            for field in row:
                row_dict[field['field']] = field['value']

            output.append(json.dumps(row_dict, indent=2))

        return "\n".join(output)

    def list_available_queries(self) -> List[Dict[str, str]]:
        """
        List all available predefined queries.

        Returns:
            List of query information dictionaries
        """
        queries = []

        for query_name, query_config in self.QUERIES.items():
            queries.append({
                'name': query_name,
                'display_name': query_config['name'],
                'description': query_config['description']
            })

        return queries


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(
        description='Run CloudWatch Log Insights queries for DE Intern Pipeline'
    )
    parser.add_argument(
        '--region',
        type=str,
        default='us-east-1',
        help='AWS region (default: us-east-1)'
    )
    parser.add_argument(
        '--query',
        type=str,
        help='Query name to run'
    )
    parser.add_argument(
        '--list-queries',
        action='store_true',
        help='List all available queries'
    )
    parser.add_argument(
        '--hours',
        type=int,
        default=1,
        help='Number of hours to look back (default: 1)'
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=100,
        help='Maximum number of results (default: 100)'
    )
    parser.add_argument(
        '--output',
        type=str,
        choices=['json', 'text'],
        default='text',
        help='Output format (default: text)'
    )

    args = parser.parse_args()

    try:
        query_runner = CloudWatchLogQueries(region=args.region)

        if args.list_queries:
            queries = query_runner.list_available_queries()

            logger.info("=" * 80)
            logger.info("Available CloudWatch Log Insights Queries")
            logger.info("=" * 80)

            for query in queries:
                logger.info(f"\n{query['name']}")
                logger.info(f"  Name: {query['display_name']}")
                logger.info(f"  Description: {query['description']}")

            sys.exit(0)

        if not args.query:
            logger.error("Please specify --query or use --list-queries to see available queries")
            sys.exit(1)

        # Run query
        start_time = datetime.now() - timedelta(hours=args.hours)
        end_time = datetime.now()

        logger.info(f"Running query: {args.query}")
        logger.info(f"Time range: Last {args.hours} hour(s)")

        result = query_runner.run_query(
            query_name=args.query,
            start_time=start_time,
            end_time=end_time,
            limit=args.limit
        )

        if result['status'] == 'Complete':
            logger.info(f"\nQuery completed successfully")
            logger.info(f"Results: {result['result_count']} rows")
            logger.info(f"Statistics: {json.dumps(result.get('statistics', {}), indent=2)}")

            logger.info("\n" + "=" * 80)
            logger.info("Query Results")
            logger.info("=" * 80)

            if args.output == 'json':
                print(json.dumps(result['results'], indent=2))
            else:
                print(query_runner.format_results(result['results']))

            sys.exit(0)
        else:
            logger.error(f"Query failed: {result.get('error', 'Unknown error')}")
            sys.exit(1)

    except Exception as e:
        logger.error(f"Failed to run query: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
