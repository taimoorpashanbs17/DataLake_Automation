from helpers.api_gateway_helper import ApiGatewayHelper

name = 'PetStore'
path = '/pets'
body = {
    "type": "Animal",
    "price": 130
}
api = ApiGatewayHelper()
print(api.test_post_api(name, path, body))
