

import boto3
import json
import io
import pandas as pd


class S3Helper:

    s3_resource = boto3.resource('s3')
    s3_client = boto3.client('s3')
    is_file = False
    
    # Check bucket exist
    def get_bucket_status(self, bucket):
        """
        Get bucket status.
        :param bucket: Name of the S3 bucket.
        """
        try:
            if self.s3_resource.Bucket(bucket).creation_date is not None:
                return True
            else:
                return False
        except Exception as e:
            print('Bucket not found', e)

    # Getting only Folders
    def get_bucket_all_folders(self, bucket):
        """
        Get all the folders from S3 bucket.
        :param bucket: Name of the S3 bucket.
        """
        if self.get_bucket_status(bucket) is True:
            try:
                bucket_obj = self.s3_resource.Bucket(bucket)
                obj_list = []
                for obj in bucket_obj.objects.all():
                    if obj.key.endswith("/"):
                        obj_list.append(obj.key)
                return obj_list
            except Exception as e:
                print(e)

    # Getting only objects in all folders
    def get_bucket_all_folders_objects(self, bucket):
        """
        Get all objects from the S3 bucket.
        :param bucket: Name of the S3 bucket.
        """
        if self.get_bucket_status(bucket) is True:
            try:
                bucket_obj = self.s3_resource.Bucket(bucket)
                obj_list = []
                for obj in bucket_obj.objects.all():
                    if obj.key.endswith("/"):
                        pass
                    else:
                        obj_list.append(obj.key)
                return obj_list
            except Exception as e:
                print(e)
                return None

    # Get objects in given folder(no sub folder objects will be returned)
    def get_folder_objects(self, bucket, folder):
        """
        Get all objects from the specified folder from S3 bucket.
        :param bucket: Name of the S3 bucket.
        :param folder: Name of the folder in S3 bucket.
        """
        if self.get_bucket_status(bucket) is True:
            try:
                bucket_obj = self.s3_resource.Bucket(bucket)
                obj_list = []
                for obj in bucket_obj.objects.filter(Prefix=folder+"/", Delimiter="/"):
                    if obj.key.endswith("/"):
                        pass
                    else:
                        obj_list.append(obj.key)
                return obj_list
            except Exception as e:
                print(e)

    # Read contents from s3 file
    def read_s3_file(self, bucket, filename):
        """
        Read a file from S3 bucket
        :param bucket: Name of the S3 bucket.
        :param filename: Name of the file.
        """
        if self.get_bucket_status(bucket) is True:
            bucket_obj = self.s3_resource.Bucket(bucket)
            try:
                for obj in bucket_obj.objects.all():
                    key = obj.key
                    if filename in key:
                        if filename.endswith('.csv') or filename.endswith('.txt'):
                            obj = self.s3_client.get_object(Bucket=bucket, Key=key)
                            df = pd.read_csv(io.BytesIO(obj['Body'].read()))
                            return df
                        elif filename.endswith('.json'):
                            body = obj.get()['Body'].read()
                            body = str(body, 'utf-8').strip('b''')
                            file_body = json.loads(body)
                            return file_body
                        else:
                            body = obj.get()['Body'].read()
                            body = str(body, 'utf-8').strip('b''')
                            return body

            except Exception as e:
                print(e)

    def read_s3_all_files(self, bucket):
        """
        Read all file from S3 bucket
        :param bucket: Name of the S3 bucket.
        """
        if self.get_bucket_status(bucket) is True:
            bucket_obj = self.s3_resource.Bucket(bucket)
            try:
                files = []
                for obj in bucket_obj.objects.all():
                    key = obj.key
                    print(key)
                    if key.endswith('.csv') or key.endswith('.txt'):
                        obj = self.s3_client.get_object(Bucket=bucket, Key=key)
                        df = pd.read_csv(io.BytesIO(obj['Body'].read()))
                        files.append(df)
                    elif key.endswith('.json'):
                        body = obj.get()['Body'].read()
                        body = str(body, 'utf-8').strip('b''')
                        file_body = json.loads(body)
                        files.append(file_body)
                return files
            except Exception as e:
                print(e)

    # Delete a file from S3 Bucket
    def delete_s3_file(self, bucket, file_name):
        """
        Delete a file from S3 bucket.
        :param bucket: Name of the S3 bucket.
        :param file_name: Name of the file to be deleted.
        """
        if self.get_bucket_status(bucket) is True:
            bucket_obj = self.s3_resource.Bucket(bucket)
            counter = 0
            response = 0
            for obj in bucket_obj.objects.all():
                key = obj.key
                if file_name in key:
                    response = self.s3_client.delete_object(Bucket=bucket, Key=key)
                    counter += 1
            if counter == 0:
                print('File not found')
            return response
        else:
            print('Bucket not found')

    # Delete all files from a key from  S3 Bucket
    def delete_all_files_directory(self, bucket_name, folder_name):
        """
        Delete all files from a key from  S3 Bucket.
        :param bucket_name: Name of the S3 bucket.
        :param folder_name: Name of the file to be deleted.
        """
        try:
            s3 = self.s3_resource
            bucket = s3.Bucket(bucket_name)
            for i in folder_name:
                bucket.objects.filter(Prefix=format(i)+"/").delete()
        except Exception as e:
            print(e)

    # Get all the keys in a bucket
    def get_all_s3_keys(self, bucket, prefix='', suffix=''):
        """
        Generate objects in an S3 bucket.
        :param bucket: Name of the S3 bucket.
        :param exclusion: Exclusion flag.
        :param prefix: Only fetch objects whose key starts with
            this prefix (optional).
        :param suffix: Only fetch objects whose keys end with
            this suffix (optional).
        """
        if self.get_bucket_status(bucket) is True:

            kwargs = {'Bucket': bucket}
            keys = []
            count = 0
            # If the prefix is a single string (not a tuple of strings), we can
            # do the filtering directly in the S3 API.
            if isinstance(prefix, str):
                kwargs['Prefix'] = prefix
            while True:
                # The S3 API response is a large blob of metadata.
                # 'Contents' contains information about the listed objects.
                resp = self.s3_client.list_objects_v2(**kwargs)
                if 'Contents' not in resp:
                    return []
                for obj in resp['Contents']:
                    key = obj['Key']
                    # print(key)
                    storage_class = obj['StorageClass']
                    if key.startswith(prefix) and key.endswith(suffix):
                        count += 1
                        keys.append({key: storage_class})
                try:
                    kwargs['ContinuationToken'] = resp['NextContinuationToken']
                except KeyError:
                    break
            return keys
        else:
            print('Bucket not found')

    def get_metadata(self, bucket, key):
        """
        Get a file metadata S3 bucket.
        :param bucket: Name of the S3 bucket.
        :param key: Name of the file whose metadata is required.
        """
        try:
            response =(self.s3_client.list_objects(Bucket=bucket))
            meta = response.get('Contents')
            for obj in meta:
                if key in obj['Key']:
                    self.is_file = True
                    return obj

            if self.is_file is False:
                print("File Not Found")
        except Exception as e:
            print(e)

    # Upload file to S3
    def upload_file(self, file_path, bucket_name, key):
        """
        Get a file metadata S3 bucket.
        :param file_path: Path with name of the file to upload
        :param bucket_name: Name of the bucket to upload the file to
        :param key: Key in the bucket where the file to be uploaded.
        """
        try:
            response = self.s3_client.upload_file(file_path, bucket_name, key)
            return response
        except Exception as e:
            print(e)

    # Get object count from folder excluding some key param
    def get_count_exclusion(self, bucket, key, exclusion_param):
        """
        Get all objects from the specified folder from S3 bucket.
        :param bucket: Name of the S3 bucket.
        :param key: Name of the key in S3 bucket.
        :param exclusion_param: Parameter to exclude files while getting count.
        """
        if self.get_bucket_status(bucket) is True:
            try:
                bucket_obj = self.s3_resource.Bucket(bucket)
                obj_list = []
                for obj in bucket_obj.objects.filter(Prefix=key+"/", Delimiter="/"):
                    if not (obj.key.endswith("/") or exclusion_param in obj.key):
                        obj_list.append(obj.key)
                return obj_list, len(obj_list)
            except Exception as e:
                print(e)

    # Get object count from folder including some key param
    def get_count_inclusion(self, bucket, key, inclusion_param):
        """
        Get all objects from the specified folder from S3 bucket.
        :param bucket: Name of the S3 bucket.
        :param key: Name of the key in S3 bucket.
        :param inclusion_param: Parameter to include files while getting count.
        """
        if self.get_bucket_status(bucket) is True:
            try:
                bucket_obj = self.s3_resource.Bucket(bucket)
                obj_list = []
                for obj in bucket_obj.objects.filter(Prefix=key+"/", Delimiter="/"):
                    if not (obj.key.endswith("/")) and inclusion_param in obj.key:
                        obj_list.append(obj.key)
                return obj_list, len(obj_list)
            except Exception as e:
                print(e)


