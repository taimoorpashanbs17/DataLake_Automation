

import boto3


class GlueHelper:
    glue_client = boto3.client('glue')
    iam_client = boto3.client('iam')

    def get_glue_table_column_names(self, database, table):
        """
        Getting Glue table clounm names.
        :param database: Name of the database.
        :param table: Name of the table.
        """
        try:
            table_data = self.glue_client.get_table(DatabaseName=database, Name=table)
            columns = table_data['Table']['StorageDescriptor']['Columns']
            column_details = {}
            for idx, i in enumerate(columns):
                column_details[(columns[idx]['Name'])] = (columns[idx]['Type'])
            return column_details

        except Exception as e:
            print(e)

    def get_glue_table_names(self, database):
        """
        Getting glue table names.
        :param database: Name of the database.
        """
        try:
            table_list = self.glue_client.get_tables(DatabaseName=database)
            table_names = []
            for idx, i in enumerate(table_list['TableList']):
                table_names.append(table_list['TableList'][idx]['Name'])
                return table_names

        except Exception as e:
            print(e)

    def delete_glue_table(self, database, table):
        """
        Deleting glue table.
        :param database: Name of the database.
        :param table: Name of the table.
        """
        try:
            self.glue_client.delete_table(DatabaseName=database, Name=table)
            print(table+" table deleted")
        except Exception as e:
            print(table+" couldn't be deleted\n" + str(e))

    def trigger_glue_job(self, jobname):
        """
        Starting a specific job
        :param jobname: Name of the job
        """
        try:
            self.glue_client.start_job_run(JobName=jobname)
        except Exception as e:
            print("Couldn't start " + jobname + " job.\n" + str(e))

    def get_glue_job_status(self, jobname):
        """
        Getting the status of a running job.
        :param  jobname: Name of the job.
        """
        try:
            self.response = self.glue_client.get_job_runs(JobName=jobname)
            status = self.glue_client.get_job_run(JobName=jobname, JobRunIds=[self.response['JobRuns'][0].get('Id')])
            print("The status of the job is " + status['JobRun']['JobRunState'])
        except Exception as e:
            print("Couldn't get job status.\n" + str(e))

    def stop_glue_job(self, jobname):
        """
        Stopping the job in running state
        :param jobname: Name of the job
        """
        try:
            self.response = self.glue_client.get_job_runs(JobName=jobname)
            if self.response['JobRuns'][0].get('JobRunState')== 'STOPPED':
                print('The Job was already Stopped.')
            else:
                self.glue_client.batch_stop_job_run(JobName=jobname, RunId=self.response['JobRuns'][0].get('Id'))
                print("The "+jobname+" job has stopped successfully.")
        except Exception as e:
            print("The was a error while stopping "+jobname+" job.\n" + str(e))

    def get_glue_database_names(self):
        """
        Getting all the databases in glue
        :return: List of all the databases in the glue
        """
        try:
            self.response = self.glue_client.get_databases()
            database_names = []
            for idx, i in enumerate(self.response['DatabaseList']):
                database_names.append(self.response['DatabaseList'][idx]['Name'])
                return database_names
        except Exception as e:
            print(e)

    def get_role_arn(self, role):
        """
         Get the ARN for role.
         :param role: Name of the Role.
         """
        try:
            response = self.iam_client.get_role(
                RoleName=role
            )
            return response['Role']['Arn']
        except Exception as e:
            print(e)

    def create_crawler(self, name, role, path, db):
        """
          Create a new crawler.
          :param name: Name of the crawler.
          :param role: Name of the role.
          :param path: Target path for s3 file.
          :param db: Name of the database.
          """
        try:
            role_arn = self.get_role_arn(role)
            s3_path = 's3.amazonaws.com/' + path
            self.glue_client.create_crawler(Name=name, Role=role_arn, Targets={
                                                      'S3Targets': [
                                                          {
                                                              'Path': s3_path
                                                          },
                                                      ]}, DatabaseName=db)
        except Exception as e:
            print(e)

    def run_crawler(self, name):
        """
          Start the Crawler.
          :param name: Name of the crawler.
          """
        try:
            response = self.glue_client.start_crawler(name)
            return response
        except Exception as e:
            print(e)
