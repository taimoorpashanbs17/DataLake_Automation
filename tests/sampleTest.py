from helpers.s3_helper import S3Helper as s3Helper


def sampleTest1():
    s3_helper_obj = s3Helper()
    # folders = s3_helper_obj.get_bucket_all_folders("atif-training")
    # objects = s3_helper_obj.get_bucket_all_folders_objects("atif-training")
    # keys = s3_helper_obj.get_all_s3_keys("atif-training", '')
    # readfile = s3_helper_obj.read_s3_file("atif-training", "greenlight_scans_87923e7e-dc76-11e8-929c-079cae96f9ce.csv")
    # s3_helper_obj.get_metadata("testqabuck", 'test')
    result = s3_helper_obj.get_count_exclusion('testqabuck', 'test', 'csv')
    print(result)
    # print(readfile)

sampleTest1()