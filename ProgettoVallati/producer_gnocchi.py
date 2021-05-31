import sys
import os
import requests
import json
import pandas as pd
import glob
files = glob.glob("data/csv/*.csv")


def admin_openrc():
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


if __name__ == "__main__":

    admin_openrc()
    os.system("openstack token issue > output_token.txt")  # salva il token di accesso in file di testo
    os.system(
        "cat output_token.txt | grep \"gAAA[^[:space:]]*\" -o > token.txt")  # recupera solamente il token e lo salva su un altro file

    with open("token.txt") as token_file:
        token = token_file.readlines()
        str_token = token.pop().replace('\n', '')  # per sicurezza se leggo anche il terminatore di stringa
        headers = {'content-type': 'application/json',
                   'X-AUTH-TOKEN': str_token}  # compongo il messaggio da inviare a gnocchi

    url = "http://252.3.243.35:8041/v1/metric/"+sys.argv[2]+"/measures"
    headers = {'content-type': 'application/json', 'X-AUTH-TOKEN': token}

    df = pd.DataFrame()
    for f in files:
        csv = pd.read_csv(f)

        rng = pd.date_range('20210531 19:00', '20210531 23:59', freq='30S')
        index = 0

        for line in df.itertuples():
            timestamp, value = [], []
            value.append(float(line[2]))
            timestamp.append(str(rng.to_series()[index]))
            index = index + 1
            measures = [{"timestamp": t, "value": v} for t, v in zip(timestamp, value)]
            print(measures)
            r = requests.post(url, data=json.dumps(measures), headers=headers)
            if str(r.status_code) == "202":
                print("inserted measure")
            else:
                print(str(r.status_code))
            if index == len(rng):
                break
            print(index)

#ID METRIC 67d112e8-4d3c-4ae6-8577-0175ac48da1d
