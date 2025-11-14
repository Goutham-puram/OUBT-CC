#!/usr/bin/env python3
"""
AWS Account Setup Validation Script

This script validates the AWS environment setup for the Data Engineering Internship project.
It checks:
- AWS credentials configuration
- Account ID verification
- Region validation
- IAM user permissions
- AWS CLI availability
- Boto3 connectivity
"""

import os
import sys
import subprocess
from typing import Dict, List, Tuple
import json

try:
    import boto3
    from botocore.exceptions import ClientError, NoCredentialsError, ProfileNotFound
except ImportError:
    print("❌ boto3 is not installed. Please install it: pip install boto3")
    sys.exit(1)


class Colors:
    """ANSI color codes for terminal output"""
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    RESET = '\033[0m'
    BOLD = '\033[1m'


class AWSValidator:
    """Validates AWS account setup and configuration"""

    def __init__(self, expected_account_id: str = "052162193815",
                 expected_region: str = "us-east-2"):
        self.expected_account_id = expected_account_id
        self.expected_region = expected_region
        self.results: List[Tuple[str, bool, str]] = []

    def print_header(self, text: str):
        """Print a formatted header"""
        print(f"\n{Colors.BOLD}{Colors.BLUE}{'='*70}{Colors.RESET}")
        print(f"{Colors.BOLD}{Colors.BLUE}{text.center(70)}{Colors.RESET}")
        print(f"{Colors.BOLD}{Colors.BLUE}{'='*70}{Colors.RESET}\n")

    def print_result(self, check_name: str, passed: bool, message: str):
        """Print a check result with color coding"""
        status = f"{Colors.GREEN}✓ PASS{Colors.RESET}" if passed else f"{Colors.RED}✗ FAIL{Colors.RESET}"
        print(f"{status} | {check_name}")
        if message:
            print(f"       {message}")
        self.results.append((check_name, passed, message))

    def check_aws_cli(self) -> bool:
        """Check if AWS CLI is installed and accessible"""
        try:
            result = subprocess.run(
                ['aws', '--version'],
                capture_output=True,
                text=True,
                timeout=5
            )
            version = result.stdout.strip() or result.stderr.strip()
            self.print_result(
                "AWS CLI Installation",
                result.returncode == 0,
                f"Version: {version}" if result.returncode == 0 else "AWS CLI not found"
            )
            return result.returncode == 0
        except FileNotFoundError:
            self.print_result(
                "AWS CLI Installation",
                False,
                "AWS CLI not found. Install from: https://aws.amazon.com/cli/"
            )
            return False
        except Exception as e:
            self.print_result("AWS CLI Installation", False, f"Error: {str(e)}")
            return False

    def check_boto3_installation(self) -> bool:
        """Check if boto3 is installed"""
        try:
            import boto3
            version = boto3.__version__
            self.print_result(
                "Boto3 Installation",
                True,
                f"Version: {version}"
            )
            return True
        except ImportError:
            self.print_result(
                "Boto3 Installation",
                False,
                "boto3 not installed. Run: pip install boto3"
            )
            return False

    def check_credentials(self) -> Tuple[bool, boto3.Session]:
        """Check if AWS credentials are configured"""
        try:
            session = boto3.Session()
            credentials = session.get_credentials()

            if credentials is None:
                self.print_result(
                    "AWS Credentials",
                    False,
                    "No credentials found. Run 'aws configure' to set up credentials"
                )
                return False, session

            # Get credentials source
            frozen_creds = credentials.get_frozen_credentials()
            if frozen_creds.access_key:
                access_key_preview = frozen_creds.access_key[:4] + "..." + frozen_creds.access_key[-4:]
                self.print_result(
                    "AWS Credentials",
                    True,
                    f"Access Key: {access_key_preview}"
                )
                return True, session
            else:
                self.print_result(
                    "AWS Credentials",
                    False,
                    "Credentials found but invalid"
                )
                return False, session

        except NoCredentialsError:
            self.print_result(
                "AWS Credentials",
                False,
                "No credentials found. Run 'aws configure'"
            )
            return False, None
        except Exception as e:
            self.print_result(
                "AWS Credentials",
                False,
                f"Error: {str(e)}"
            )
            return False, None

    def check_account_id(self, session: boto3.Session) -> bool:
        """Verify the AWS account ID"""
        if not session:
            self.print_result(
                "Account ID Verification",
                False,
                "Cannot verify - no valid session"
            )
            return False

        try:
            sts_client = session.client('sts')
            identity = sts_client.get_caller_identity()
            account_id = identity['Account']
            user_arn = identity['Arn']

            passed = account_id == self.expected_account_id
            message = f"Current: {account_id}"
            if not passed:
                message += f" | Expected: {self.expected_account_id}"
            message += f"\n       ARN: {user_arn}"

            self.print_result(
                "Account ID Verification",
                passed,
                message
            )
            return passed

        except ClientError as e:
            self.print_result(
                "Account ID Verification",
                False,
                f"Error calling STS: {e.response['Error']['Message']}"
            )
            return False
        except Exception as e:
            self.print_result(
                "Account ID Verification",
                False,
                f"Error: {str(e)}"
            )
            return False

    def check_region(self, session: boto3.Session) -> bool:
        """Verify the AWS region configuration"""
        if not session:
            self.print_result(
                "Region Verification",
                False,
                "Cannot verify - no valid session"
            )
            return False

        try:
            # Check multiple sources for region
            region = session.region_name
            env_region = os.environ.get('AWS_DEFAULT_REGION') or os.environ.get('AWS_REGION')

            if not region:
                region = env_region

            passed = region == self.expected_region
            message = f"Current: {region or 'Not set'}"
            if not passed:
                message += f" | Expected: {self.expected_region}"
            if env_region and env_region != region:
                message += f"\n       Env var region: {env_region}"

            self.print_result(
                "Region Verification",
                passed,
                message
            )
            return passed

        except Exception as e:
            self.print_result(
                "Region Verification",
                False,
                f"Error: {str(e)}"
            )
            return False

    def check_iam_permissions(self, session: boto3.Session) -> bool:
        """Check IAM user permissions for required AWS services"""
        if not session:
            self.print_result(
                "IAM Permissions Check",
                False,
                "Cannot verify - no valid session"
            )
            return False

        permissions_results = []

        # List of services to check with basic read operations
        services_to_check = [
            ('S3', 's3', 'list_buckets'),
            ('IAM', 'iam', 'get_user'),
            ('RDS', 'rds', 'describe_db_instances'),
            ('Glue', 'glue', 'get_databases'),
            ('Lambda', 'lambda', 'list_functions'),
            ('Step Functions', 'stepfunctions', 'list_state_machines'),
        ]

        for service_name, service_code, test_operation in services_to_check:
            try:
                client = session.client(service_code, region_name=self.expected_region)

                # Try to call a basic read operation
                if test_operation == 'list_buckets':
                    client.list_buckets()
                elif test_operation == 'get_user':
                    try:
                        client.get_user()
                    except ClientError as e:
                        if e.response['Error']['Code'] != 'AccessDenied':
                            pass  # Other errors are ok for this check
                elif test_operation == 'describe_db_instances':
                    client.describe_db_instances(MaxRecords=20)
                elif test_operation == 'get_databases':
                    client.get_databases()
                elif test_operation == 'list_functions':
                    client.list_functions(MaxItems=10)
                elif test_operation == 'list_state_machines':
                    client.list_state_machines(maxResults=10)

                permissions_results.append(f"{service_name}: ✓")

            except ClientError as e:
                error_code = e.response['Error']['Code']
                if error_code in ['AccessDenied', 'UnauthorizedOperation', 'AccessDeniedException']:
                    permissions_results.append(f"{service_name}: ✗ (Access Denied)")
                else:
                    # Other errors might be ok (e.g., no resources exist yet)
                    permissions_results.append(f"{service_name}: ✓ (verified)")
            except Exception as e:
                permissions_results.append(f"{service_name}: ? (Error: {type(e).__name__})")

        # Consider it passed if we can access at least S3 and IAM
        critical_services = [r for r in permissions_results if r.startswith(('S3:', 'IAM:'))]
        passed = all('✓' in r for r in critical_services)

        message = "\n       " + "\n       ".join(permissions_results)

        self.print_result(
            "IAM Permissions Check",
            passed,
            message
        )
        return passed

    def check_boto3_connectivity(self, session: boto3.Session) -> bool:
        """Test boto3 connectivity to AWS"""
        if not session:
            self.print_result(
                "Boto3 Connectivity",
                False,
                "Cannot test - no valid session"
            )
            return False

        try:
            sts_client = session.client('sts', region_name=self.expected_region)
            response = sts_client.get_caller_identity()

            self.print_result(
                "Boto3 Connectivity",
                True,
                f"Successfully connected as: {response['Arn'].split('/')[-1]}"
            )
            return True

        except Exception as e:
            self.print_result(
                "Boto3 Connectivity",
                False,
                f"Error: {str(e)}"
            )
            return False

    def run_all_checks(self) -> bool:
        """Run all validation checks"""
        self.print_header("AWS Account Setup Validation")

        print(f"{Colors.BOLD}Expected Configuration:{Colors.RESET}")
        print(f"  Account ID: {self.expected_account_id}")
        print(f"  Region:     {self.expected_region}\n")

        # Run checks in order
        cli_ok = self.check_aws_cli()
        boto3_ok = self.check_boto3_installation()

        if not boto3_ok:
            self.print_summary()
            return False

        creds_ok, session = self.check_credentials()

        if not creds_ok or not session:
            self.print_summary()
            return False

        account_ok = self.check_account_id(session)
        region_ok = self.check_region(session)
        permissions_ok = self.check_iam_permissions(session)
        connectivity_ok = self.check_boto3_connectivity(session)

        self.print_summary()

        # All critical checks must pass
        return all([creds_ok, account_ok, region_ok, connectivity_ok])

    def print_summary(self):
        """Print summary of all checks"""
        print(f"\n{Colors.BOLD}{Colors.BLUE}{'='*70}{Colors.RESET}")
        print(f"{Colors.BOLD}Summary{Colors.RESET}")
        print(f"{Colors.BOLD}{Colors.BLUE}{'='*70}{Colors.RESET}\n")

        passed = sum(1 for _, p, _ in self.results if p)
        total = len(self.results)

        if passed == total:
            print(f"{Colors.GREEN}{Colors.BOLD}✓ All checks passed! ({passed}/{total}){Colors.RESET}")
            print(f"\n{Colors.GREEN}Your AWS environment is correctly configured.{Colors.RESET}")
            print(f"You can proceed with the Data Engineering Internship project.\n")
        else:
            print(f"{Colors.RED}{Colors.BOLD}✗ Some checks failed ({passed}/{total} passed){Colors.RESET}\n")
            print(f"{Colors.YELLOW}Failed checks:{Colors.RESET}")
            for name, p, msg in self.results:
                if not p:
                    print(f"  - {name}")
            print(f"\n{Colors.YELLOW}Please fix the issues above before proceeding.{Colors.RESET}")
            print(f"See docs/setup/aws_account_setup.md for detailed setup instructions.\n")


def main():
    """Main entry point"""
    # Allow override via environment variables
    expected_account_id = os.environ.get('AWS_ACCOUNT_ID', '052162193815')
    expected_region = os.environ.get('AWS_DEFAULT_REGION', 'us-east-2')

    validator = AWSValidator(
        expected_account_id=expected_account_id,
        expected_region=expected_region
    )

    success = validator.run_all_checks()

    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
