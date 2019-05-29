"""
----------------------------------------------------------------------------------------------------------
Description:

usage: EC2 Helper Methods

Author  : Adil Qayyum
Release : 1

Modification Log:

How to execute:
-----------------------------------------------------------------------------------------------------------
Date                Author              Story               Description
-----------------------------------------------------------------------------------------------------------
05/7/2019         Adil Qayyum                              Initial draft.
-----------------------------------------------------------------------------------------------------------
"""

import paramiko


def ec2_setup_connection(pem_key, host, username):
    """
    Execute query on Athena.
    :param pem_key: Name of the pem key for the EC2 instance.
    :param host: Host for the EC2 instance.
    :param username: Username for the EC2 Instance.
    """

    key = paramiko.RSAKey.from_private_key_file(pem_key)
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    # Connect/ssh to an instance
    try:
        client.connect(hostname="'"+host+"'", username="'"+username+"'", pkey=key)
        return client

    except Exception as e:
        print(e)


def ec2_execute_command(client, command):
    """
    Execute query on Athena.
    :param client: Connection client of EC2.
    :param command: Command to execute on EC2.
    """

    try:
        # Execute a command(cmd) after connecting/ssh to an instance
        client.exec_command(command)
    except Exception as e:
        print(e)


def ec2_close_connection(client):
    """
    Execute query on Athena.
    :param client: Connection client of EC2.
    """

    try:
        # close the client connection once the job is done
        client.close()
    except Exception as e:
        print(e)
