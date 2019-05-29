""""
----------------------------------------------------------------------------------------------------------
Description:

usage: SES Helper Methods

Modification Log:

How to execute:
-----------------------------------------------------------------------------------------------------------
Date                Author              Story               Description
-----------------------------------------------------------------------------------------------------------
05/10/2019        Taimoor Pasha                          Initial draft.

-----------------------------------------------------------------------------------------------------------
"""

import boto3
from botocore.exceptions import ClientError
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import os


class SesHelper:
    def __init__(self):
        self.ses = boto3.client('ses')

    def send_email(self, recipient, sender, body_text, subject):
        """Sending an email by using SES Helper"""
        """:params receipient : Pass Recipient Email Address Here"""
        """:params sender : Pass Sender Email Address Here"""
        """:params body_text : Pass what message needs to be display at Message body"""
        """:params subject : Pass Email's Subject Here"""

        BODY_TEXT = body_text
        SUBJECT = subject
        CHARSET = 'UTF-8'
        try:
            response = self.ses.send_email(
                Destination={
                    'ToAddresses': [
                        recipient,
                    ],
                },
                Message={
                    'Body': {
                        'Text': {
                            'Charset': CHARSET,
                            'Data': BODY_TEXT,
                        },
                    },
                    'Subject': {
                        'Charset': CHARSET,
                        'Data': SUBJECT,
                    },
                },
                Source=sender,
            )
        except ClientError as e:
            print(e.response['Error']['Message'])
        else:
            print("Email sent! Message ID:"),
            print(response['MessageId'])

    def send_email_to_multiple_recipients(self,mail_sender,mail_receivers_list, mail_subject, mail_content):
        """Sending an email by using SES Helper to multiple users"""
        """:params sender : Pass Sender Email Address Here"""
        """:params mail_receivers_list : Pass list of Recipient Emails Address in List Data Type"""
        """:params mail_subject : Pass Email's Subject Here"""
        """:params mail_content : Pass what message needs to be display at Message body"""

        message_dict = {'Data':
                        'From: ' + mail_sender + '\n'
                        'To: ' + mail_receivers_list + '\n'
                        'Subject: ' + mail_subject + '\n'
                        'MIME-Version: 1.0\n'
                        'Content-Type: text/html;\n\n' +
                        mail_content}

        try:
            response = self.ses.send_raw_email(
                RawMessage=message_dict
            )
        except ClientError as e:
            print(e.response['Error']['Message'])
        else:
            print("Email sent! Message ID:"),
            print(response['MessageId'])

    def send_email_with_attachment(self, attachment, subject, sender, recipient, body_text):
        """Sending an email with an attachment"""
        """:params attachment : Path of the Attachment"""
        """:params subject : Pass the subject of the mail"""
        """:params receipient : Pass Recipient Email Address Here"""
        """:params sender : Pass Sender Email Address Here"""
        """:params body_text : Pass what message needs to be display at Message body"""

        charset = "utf-8"
        msg = MIMEMultipart('mixed')
        msg['Subject'] = subject
        msg['From'] = sender
        msg['To'] = recipient
        msg_body = MIMEMultipart('alternative')
        textpart = MIMEText(body_text.encode(charset), 'plain', charset)
        msg_body.attach(textpart)
        att = MIMEApplication(open(attachment, 'rb').read())
        att.add_header('Content-Disposition', 'attachment', filename=os.path.basename(attachment))
        msg.attach(msg_body)
        msg.attach(att)
        try:
            response = self.ses.send_raw_email(
                Source=sender,
                Destinations=[
                    recipient
                ],
                RawMessage={
                    'Data': msg.as_string(),
                },
            )
        except ClientError as e:
            print(e.response['Error']['Message'])
        else:
            print("Email sent! Message ID:"),
            print(response['MessageId'])

