
import boto3
import pandas as pd
import json


class ApiGatewayHelper:

    def __init__(self):
        try:
            self.api_client = boto3.client('apigateway')
        except Exception as e:
            print(e)

    def get_all_apis(self):
        """
               Get all API's in a dataframe.

        """
        try:
            response = self.api_client.get_rest_apis()
            df = pd.DataFrame(response.get('items'))
            return df
        except Exception as e:
            print(e)

    def get_api_id(self, name):
        """
               Get API id by name.
               :param name: API name whose id is required.
        """
        try:
            response = self.api_client.get_rest_apis()
            df = pd.DataFrame(response.get('items'))
            rec = df[df['name'] == name]
            if rec.empty:
                print("Name Not Found")
                return ""
            else:
                return rec['id'].values[0]

        except Exception as e:
            print(e)

    def get_resource_id(self, path, api_id):
        """
                Get resource id by particular url path and api id.
                :param path: API path whose resource id is required.
                :param api_id: ID of api whose resource id is required.
         """
        try:
            response = self.api_client.get_resources(restApiId=api_id)
            df = pd.DataFrame(response.get('items'))
            rec = df[df['path'] == path]
            if rec.empty:
                print("Path Not Found")
                return ""
            else:
                return rec['id'].values[0]
        except Exception as e:
            print(e)

    def test_get_api(self, name, path):
        """
                Test API by GET request method.
                :param name: API name needs to be tested.
                :param path: Particular path of API which needs to be tested.
         """
        try:
            api_id = self.get_api_id(name)
            resource_id = self.get_resource_id(path, api_id)
            response = self.api_client.test_invoke_method(restApiId=api_id, resourceId=resource_id, httpMethod='GET')
            return response['body']
        except Exception as e:
            print(e)

    def test_post_api(self, name, path, body):
        """
                 Test API by POST request method.
                 :param name: API name needs to be tested.
                 :param path: Particular path of API which needs to be tested.
                :param body: Request body sent along with API.
          """
        try:
            api_id = self.get_api_id(name)
            resource_id = self.get_resource_id(path, api_id)
            result = json.dumps(body)
            response = self.api_client.test_invoke_method(restApiId=api_id, resourceId=resource_id, httpMethod='POST', body=result)
            return response['body']
        except Exception as e:
            print(e)


