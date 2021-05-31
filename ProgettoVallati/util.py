import json
import os
import requests


def print_things(parameter, str_token):
    url = "http://252.3.243.35:8041/v1/" + parameter
    headers = {'X-AUTH-TOKEN': str_token}
    r = requests.get(url, headers=headers)
    if str(r.status_code) == "200":
        parsed = json.loads(r.text)
        print(json.dumps(parsed, indent=4, sort_keys=True))
    else:
        print("Error code: " + str(r.status_code))


def connection():
    # eseguo i comandi di admin-openrc.sh
    os.environ["OS_PROJECT_ID"] = "9009edd7c5104c628d8ed9b16bf5ec31"
    os.environ["OS_AUTH_URL"] = "http://252.3.243.14:5000/v3"
    os.environ["OS_PROJECT_NAME"] = "admin"
    os.environ["OS_USER_DOMAIN_NAME"] = "admin_domain"
    os.environ["OS_PROJECT_DOMAIN_ID"] = "5227438031ac4bf2a8f1c3fcba908e94"
    os.environ["OS_USERNAME"] = "admin"
    os.environ["OS_PASSWORD"] = "openstack"
    os.environ["OS_REGION_NAME"] = "RegionOne"
    os.environ["OS_INTERFACE"] = "public"
    os.environ["OS_IDENTITY_API_VERSION"] = "3"

    os.system("openstack token issue > output_token.txt")  # salva il token di accesso in file di testo
    os.system(
        "cat output_token.txt | grep \"gAAA[^[:space:]]*\" -o > token.txt")  # recupera solamente il token e lo salva su un altro file

    with open("token.txt") as token_file:
        token = token_file.readlines()
        str_token = token.pop().replace('\n', '')  # per sicurezza se leggo anche il terminatore di stringa

        headers = {'content-type': 'application/json',
                   'X-AUTH-TOKEN': str_token}  # compongo il messaggio da inviare a gnocchi

    return headers, str_token
