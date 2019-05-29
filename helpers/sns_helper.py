

import boto3


class SnsHelper:
    sns_client = boto3.client('sns')

    def create_sns_topic(self, topicname):
        """
                Create SNS Topic
                :param topicname: Topic name, you want to create.
        """
        try:
            response = self.sns_client.create_topic(Name=topicname)
            return response.get('TopicArn')
        except Exception as e:
            print(e)

    def send_notification(self, topic_arn, subject, message):
        """
                Send SNS Notification
                :param topic_arn: Topic ARN, you want to send notification email thorugh.
                :param subject: Subject of the notification email.
                :param message: Message of the notification email.
        """
        try:
            response = self.sns_client.publish(TopicArn=topic_arn, Subject=subject, Message=message)
            return response
        except Exception as e:
            print(e)

    def delete_sns_topic(self, arn):
        """
                Delete SNS Topic
                :param arn: Topic ARN, you want to delete.
        """
        try:
            self.sns_client.delete_topic(TopicArn=arn)
        except Exception as e:
            print(e)
