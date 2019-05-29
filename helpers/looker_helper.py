
import lookerapi
import ast
import pandas as pd


def looker_connection(base_url, client_id, client_secret):
    """
    Execute query on Athena.
    :param base_url: URL for looker.
    :param client_id: API3 client id for looker.
    :param client_secret: API3 client secret for looker.
    """
    try:
        # instantiate Auth API
        unauthenticated_client = lookerapi.ApiClient(base_url)
        unauthenticated_authapi = lookerapi.ApiAuthApi(unauthenticated_client)

        # authenticate client
        token = unauthenticated_authapi.login(client_id=client_id, client_secret=client_secret)
        client = lookerapi.ApiClient(base_url, 'Authorization', 'token ' + token.access_token)

        # instantiate Look API
        looks = lookerapi.LookApi(client)
        return looks
    except Exception as e:
        print("There is some issue with the API3 credentials. " + str(e))


def get_look_sql(looks, look_id):
    """
    Execute query on Athena.
    :param looks: looks object returned by looker connection method.
    :param look_id: ID of the look to be accessed.
    """
    try:
        look_result = looks.run_look(look_id, 'sql')
        return look_result
    except Exception as e:
        print("There is some issue with the looks or the look id. " + str(e))


def get_look_data(looks, look_id):
    """
    Execute query on Athena.
    :param looks: looks object returned by looker connection method.
    :param look_id: ID of the look to be accessed.
    """
    try:
        look_result = looks.run_look(look_id, 'json')
        look_result = ast.literal_eval(look_result)
        df = pd.DataFrame.from_records(look_result)
        return df
    except Exception as e:
        print("There is some issue with the looks or the look id. " + str(e))
