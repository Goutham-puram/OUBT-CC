"""
Setup SNS notifications for CloudWatch alarms and pipeline events.

Features:
- Email alerts for critical alarms
- Slack integration (optional)
- SMS notifications (optional)
- Custom notification filters
"""

import sys
import json
from typing import Dict, List, Any, Optional
import boto3
from botocore.exceptions import ClientError

# Add parent directory to path for imports
sys.path.append('/home/user/OUBT-CC/de-intern-2024-project/src')
from de_intern_2024.utils.logger import get_logger
from de_intern_2024.utils.aws_helpers import get_boto3_client

logger = get_logger(__name__)


class SNSNotificationSetup:
    """
    Manages SNS topics and subscriptions for pipeline notifications.
    """

    def __init__(
        self,
        topic_name: str = "etl-pipeline-notifications",
        region: str = 'us-east-1'
    ):
        """
        Initialize SNS Notification Setup.

        Args:
            topic_name: Name for the SNS topic
            region: AWS region
        """
        self.topic_name = topic_name
        self.region = region

        # Initialize AWS clients
        self.sns_client = get_boto3_client('sns', region=region)
        self.sts_client = get_boto3_client('sts', region=region)

        # Get account information
        self.account_id = self.sts_client.get_caller_identity()['Account']

        logger.info(f"Initialized SNSNotificationSetup")
        logger.info(f"Topic: {self.topic_name}")
        logger.info(f"Region: {self.region}")
        logger.info(f"Account ID: {self.account_id}")

    def create_topic(self, display_name: Optional[str] = None) -> str:
        """
        Create SNS topic for notifications.

        Args:
            display_name: Display name for the topic

        Returns:
            SNS topic ARN
        """
        try:
            logger.info(f"Creating SNS topic: {self.topic_name}")

            if display_name is None:
                display_name = "DE Intern Pipeline Notifications"

            response = self.sns_client.create_topic(
                Name=self.topic_name,
                Attributes={
                    'DisplayName': display_name,
                    'FifoTopic': 'false'
                },
                Tags=[
                    {'Key': 'Project', 'Value': 'OUBT-DataEngineering'},
                    {'Key': 'Purpose', 'Value': 'PipelineNotifications'},
                    {'Key': 'ManagedBy', 'Value': 'Automation'}
                ]
            )

            topic_arn = response['TopicArn']
            logger.info(f"Created SNS topic: {topic_arn}")

            return topic_arn

        except ClientError as e:
            if 'already exists' in str(e).lower():
                # Get existing topic ARN
                topic_arn = f"arn:aws:sns:{self.region}:{self.account_id}:{self.topic_name}"
                logger.info(f"SNS topic already exists: {topic_arn}")
                return topic_arn
            else:
                logger.error(f"Failed to create SNS topic: {e}")
                raise

    def subscribe_email(self, topic_arn: str, email: str) -> Dict[str, Any]:
        """
        Subscribe email address to SNS topic.

        Args:
            topic_arn: SNS topic ARN
            email: Email address to subscribe

        Returns:
            Subscription information
        """
        try:
            logger.info(f"Subscribing email {email} to topic")

            response = self.sns_client.subscribe(
                TopicArn=topic_arn,
                Protocol='email',
                Endpoint=email,
                ReturnSubscriptionArn=True
            )

            subscription_arn = response.get('SubscriptionArn', 'pending confirmation')

            logger.info(f"Email subscription created: {subscription_arn}")
            logger.info(f"Please check {email} and confirm the subscription")

            return {
                'protocol': 'email',
                'endpoint': email,
                'subscription_arn': subscription_arn,
                'status': 'pending confirmation' if subscription_arn == 'pending confirmation' else 'confirmed'
            }

        except Exception as e:
            logger.error(f"Failed to subscribe email: {e}")
            raise

    def subscribe_sms(self, topic_arn: str, phone_number: str) -> Dict[str, Any]:
        """
        Subscribe phone number to SNS topic for SMS notifications.

        Args:
            topic_arn: SNS topic ARN
            phone_number: Phone number in E.164 format (e.g., +1234567890)

        Returns:
            Subscription information
        """
        try:
            logger.info(f"Subscribing SMS {phone_number} to topic")

            # Validate phone number format
            if not phone_number.startswith('+'):
                raise ValueError("Phone number must be in E.164 format (e.g., +1234567890)")

            response = self.sns_client.subscribe(
                TopicArn=topic_arn,
                Protocol='sms',
                Endpoint=phone_number,
                ReturnSubscriptionArn=True
            )

            subscription_arn = response.get('SubscriptionArn')

            logger.info(f"SMS subscription created: {subscription_arn}")

            return {
                'protocol': 'sms',
                'endpoint': phone_number,
                'subscription_arn': subscription_arn,
                'status': 'confirmed'
            }

        except Exception as e:
            logger.error(f"Failed to subscribe SMS: {e}")
            raise

    def subscribe_slack_webhook(self, topic_arn: str, webhook_url: str) -> Dict[str, Any]:
        """
        Subscribe Slack webhook to SNS topic via Lambda.
        Note: This requires a Lambda function to transform SNS messages to Slack format.

        Args:
            topic_arn: SNS topic ARN
            webhook_url: Slack incoming webhook URL

        Returns:
            Subscription information
        """
        try:
            logger.info("Setting up Slack webhook integration")

            # Note: For Slack integration, you would typically:
            # 1. Create a Lambda function that receives SNS messages
            # 2. Transform them to Slack format
            # 3. POST to the webhook URL
            # 4. Subscribe the Lambda to the SNS topic

            logger.warning("Slack webhook integration requires a Lambda function")
            logger.warning("Please see documentation for complete setup instructions")

            # For now, we'll just log the webhook URL
            # In production, you'd create the Lambda and subscription here

            return {
                'protocol': 'lambda',
                'endpoint': 'slack-webhook-lambda (to be created)',
                'webhook_url': webhook_url,
                'status': 'manual_setup_required'
            }

        except Exception as e:
            logger.error(f"Failed to setup Slack integration: {e}")
            raise

    def subscribe_https_endpoint(self, topic_arn: str, endpoint_url: str) -> Dict[str, Any]:
        """
        Subscribe HTTPS endpoint to SNS topic.

        Args:
            topic_arn: SNS topic ARN
            endpoint_url: HTTPS endpoint URL

        Returns:
            Subscription information
        """
        try:
            logger.info(f"Subscribing HTTPS endpoint to topic")

            if not endpoint_url.startswith('https://'):
                raise ValueError("Endpoint must use HTTPS protocol")

            response = self.sns_client.subscribe(
                TopicArn=topic_arn,
                Protocol='https',
                Endpoint=endpoint_url,
                ReturnSubscriptionArn=True
            )

            subscription_arn = response.get('SubscriptionArn', 'pending confirmation')

            logger.info(f"HTTPS subscription created: {subscription_arn}")
            logger.info("The endpoint will receive a subscription confirmation request")

            return {
                'protocol': 'https',
                'endpoint': endpoint_url,
                'subscription_arn': subscription_arn,
                'status': 'pending confirmation' if subscription_arn == 'pending confirmation' else 'confirmed'
            }

        except Exception as e:
            logger.error(f"Failed to subscribe HTTPS endpoint: {e}")
            raise

    def set_topic_policy(self, topic_arn: str) -> bool:
        """
        Set SNS topic policy to allow CloudWatch alarms to publish.

        Args:
            topic_arn: SNS topic ARN

        Returns:
            True if successful
        """
        try:
            logger.info("Setting topic policy for CloudWatch alarms")

            policy = {
                "Version": "2012-10-17",
                "Id": "CloudWatchAlarmsPolicy",
                "Statement": [
                    {
                        "Sid": "AllowCloudWatchAlarmsPublish",
                        "Effect": "Allow",
                        "Principal": {
                            "Service": "cloudwatch.amazonaws.com"
                        },
                        "Action": [
                            "SNS:Publish"
                        ],
                        "Resource": topic_arn,
                        "Condition": {
                            "StringEquals": {
                                "AWS:SourceAccount": self.account_id
                            }
                        }
                    },
                    {
                        "Sid": "AllowStepFunctionsPublish",
                        "Effect": "Allow",
                        "Principal": {
                            "Service": "states.amazonaws.com"
                        },
                        "Action": [
                            "SNS:Publish"
                        ],
                        "Resource": topic_arn,
                        "Condition": {
                            "StringEquals": {
                                "AWS:SourceAccount": self.account_id
                            }
                        }
                    },
                    {
                        "Sid": "AllowLambdaPublish",
                        "Effect": "Allow",
                        "Principal": {
                            "Service": "lambda.amazonaws.com"
                        },
                        "Action": [
                            "SNS:Publish"
                        ],
                        "Resource": topic_arn,
                        "Condition": {
                            "StringEquals": {
                                "AWS:SourceAccount": self.account_id
                            }
                        }
                    }
                ]
            }

            self.sns_client.set_topic_attributes(
                TopicArn=topic_arn,
                AttributeName='Policy',
                AttributeValue=json.dumps(policy)
            )

            logger.info("Topic policy set successfully")
            return True

        except Exception as e:
            logger.error(f"Failed to set topic policy: {e}")
            return False

    def set_filter_policy(
        self,
        subscription_arn: str,
        filter_policy: Dict[str, Any]
    ) -> bool:
        """
        Set message filter policy for a subscription.

        Args:
            subscription_arn: ARN of the subscription
            filter_policy: Filter policy dictionary

        Returns:
            True if successful
        """
        try:
            logger.info(f"Setting filter policy for subscription: {subscription_arn}")

            self.sns_client.set_subscription_attributes(
                SubscriptionArn=subscription_arn,
                AttributeName='FilterPolicy',
                AttributeValue=json.dumps(filter_policy)
            )

            logger.info("Filter policy set successfully")
            return True

        except Exception as e:
            logger.error(f"Failed to set filter policy: {e}")
            return False

    def list_subscriptions(self, topic_arn: str) -> List[Dict[str, Any]]:
        """
        List all subscriptions for a topic.

        Args:
            topic_arn: SNS topic ARN

        Returns:
            List of subscription information
        """
        try:
            logger.info(f"Listing subscriptions for topic: {topic_arn}")

            response = self.sns_client.list_subscriptions_by_topic(
                TopicArn=topic_arn
            )

            subscriptions = []
            for sub in response.get('Subscriptions', []):
                subscriptions.append({
                    'subscription_arn': sub['SubscriptionArn'],
                    'protocol': sub['Protocol'],
                    'endpoint': sub['Endpoint'],
                    'owner': sub['Owner']
                })

            logger.info(f"Found {len(subscriptions)} subscription(s)")
            return subscriptions

        except Exception as e:
            logger.error(f"Failed to list subscriptions: {e}")
            return []

    def delete_subscription(self, subscription_arn: str) -> bool:
        """
        Delete a subscription.

        Args:
            subscription_arn: ARN of the subscription to delete

        Returns:
            True if successful
        """
        try:
            logger.info(f"Deleting subscription: {subscription_arn}")

            self.sns_client.unsubscribe(
                SubscriptionArn=subscription_arn
            )

            logger.info("Subscription deleted successfully")
            return True

        except Exception as e:
            logger.error(f"Failed to delete subscription: {e}")
            return False

    def publish_test_message(self, topic_arn: str, message: str = None) -> bool:
        """
        Publish a test message to the topic.

        Args:
            topic_arn: SNS topic ARN
            message: Custom test message (optional)

        Returns:
            True if successful
        """
        try:
            if message is None:
                message = "This is a test notification from the DE Intern Pipeline monitoring system."

            logger.info("Publishing test message to topic")

            response = self.sns_client.publish(
                TopicArn=topic_arn,
                Subject='Test Notification - DE Intern Pipeline',
                Message=message,
                MessageAttributes={
                    'notification_type': {
                        'DataType': 'String',
                        'StringValue': 'test'
                    },
                    'severity': {
                        'DataType': 'String',
                        'StringValue': 'info'
                    }
                }
            )

            message_id = response['MessageId']
            logger.info(f"Test message published successfully: {message_id}")

            return True

        except Exception as e:
            logger.error(f"Failed to publish test message: {e}")
            return False

    def setup_notifications(
        self,
        emails: Optional[List[str]] = None,
        phone_numbers: Optional[List[str]] = None,
        slack_webhook: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Complete setup of SNS notifications.

        Args:
            emails: List of email addresses to subscribe
            phone_numbers: List of phone numbers to subscribe (E.164 format)
            slack_webhook: Slack webhook URL for integration

        Returns:
            Setup summary dictionary
        """
        logger.info("=" * 80)
        logger.info("SNS Notification Setup")
        logger.info("=" * 80)

        try:
            # Step 1: Create SNS topic
            logger.info("\n[STEP 1] Creating SNS topic")
            topic_arn = self.create_topic()

            # Step 2: Set topic policy
            logger.info("\n[STEP 2] Setting topic policy")
            self.set_topic_policy(topic_arn)

            # Step 3: Subscribe endpoints
            subscriptions = []

            if emails:
                logger.info(f"\n[STEP 3] Subscribing {len(emails)} email address(es)")
                for email in emails:
                    try:
                        sub = self.subscribe_email(topic_arn, email)
                        subscriptions.append(sub)
                    except Exception as e:
                        logger.error(f"Failed to subscribe {email}: {e}")

            if phone_numbers:
                logger.info(f"\n[STEP 4] Subscribing {len(phone_numbers)} phone number(s)")
                for phone in phone_numbers:
                    try:
                        sub = self.subscribe_sms(topic_arn, phone)
                        subscriptions.append(sub)
                    except Exception as e:
                        logger.error(f"Failed to subscribe {phone}: {e}")

            if slack_webhook:
                logger.info("\n[STEP 5] Setting up Slack webhook integration")
                try:
                    sub = self.subscribe_slack_webhook(topic_arn, slack_webhook)
                    subscriptions.append(sub)
                except Exception as e:
                    logger.error(f"Failed to setup Slack: {e}")

            # Step 4: Publish test message
            logger.info("\n[STEP 6] Publishing test notification")
            self.publish_test_message(topic_arn)

            # Summary
            logger.info("\n" + "=" * 80)
            logger.info("Setup Summary")
            logger.info("=" * 80)

            logger.info(f"\nSNS Topic ARN: {topic_arn}")
            logger.info(f"Total Subscriptions: {len(subscriptions)}")

            logger.info("\nSubscriptions:")
            for sub in subscriptions:
                logger.info(f"  - {sub['protocol']}: {sub['endpoint']} [{sub['status']}]")

            logger.info("\n" + "=" * 80)
            logger.info("SUCCESS: SNS notification setup completed!")
            logger.info("=" * 80)

            logger.info("\nNext Steps:")
            logger.info("  1. Check email inbox(es) and confirm subscription(s)")
            logger.info("  2. Verify test message was received")
            logger.info("  3. Configure CloudWatch alarms to use this topic")

            return {
                'topic_arn': topic_arn,
                'subscriptions': subscriptions,
                'status': 'success'
            }

        except Exception as e:
            logger.error(f"\nSetup failed: {e}", exc_info=True)
            return {
                'status': 'failed',
                'error': str(e)
            }


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(
        description='Setup SNS notifications for DE Intern Pipeline'
    )
    parser.add_argument(
        '--topic-name',
        type=str,
        default='etl-pipeline-notifications',
        help='SNS topic name (default: etl-pipeline-notifications)'
    )
    parser.add_argument(
        '--region',
        type=str,
        default='us-east-1',
        help='AWS region (default: us-east-1)'
    )
    parser.add_argument(
        '--emails',
        type=str,
        nargs='+',
        help='Email addresses to subscribe'
    )
    parser.add_argument(
        '--phone-numbers',
        type=str,
        nargs='+',
        help='Phone numbers to subscribe (E.164 format, e.g., +1234567890)'
    )
    parser.add_argument(
        '--slack-webhook',
        type=str,
        help='Slack incoming webhook URL'
    )
    parser.add_argument(
        '--test-message',
        action='store_true',
        help='Send a test message to the topic'
    )

    args = parser.parse_args()

    try:
        setup = SNSNotificationSetup(
            topic_name=args.topic_name,
            region=args.region
        )

        if args.test_message:
            # Just send a test message
            topic_arn = f"arn:aws:sns:{args.region}:{setup.account_id}:{args.topic_name}"
            success = setup.publish_test_message(topic_arn)
            sys.exit(0 if success else 1)
        else:
            # Full setup
            result = setup.setup_notifications(
                emails=args.emails,
                phone_numbers=args.phone_numbers,
                slack_webhook=args.slack_webhook
            )

            sys.exit(0 if result['status'] == 'success' else 1)

    except Exception as e:
        logger.error(f"Failed to setup notifications: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
