from helpers.sns_helper import SnsHelper as snsHelper


def sampleTest1():

    sns = snsHelper()
    topic = sns.create_sns_topic("Test")
    sns.delete_sns_topic(topic)


sampleTest1()