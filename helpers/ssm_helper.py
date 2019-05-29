

import boto3


class SSMHelper:
    ssm_client = boto3.client('ssm')

    # This method is created to read SSM keys from SSM
    def get_ssm_keys(self, ssm_key):
        """
        Execute query on Athena.
        :param ssm_key: Name of the SSM Parameter.
        """
        try:
            response = self.ssm_client.get_parameter(
                Name=ssm_key, WithDecryption=True)
            value = response['Parameter']['Value']
            return value
        except Exception as e:
            print('There is some error, kindly check the SSM keys to get it fix ' + str(e))