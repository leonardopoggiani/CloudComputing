import sys
import os
import requests
import json
import pandas as pd
from keystoneauth1.identity import v3
from keystoneauth1 import session

if __name__ == "__main__":

    if len(sys.argv) < 4:
        print("Usage: producer_gnocchi  <token> <id_metrica> <dati_input>", file=sys.stderr)
        sys.exit(-1)

    auth = v3.Password(auth_url='http://172.16.50.247:5000/v3',
                       username='admin',
                       password='openstack',
                       project_name='admin',
                       user_domain_id='default',
                       project_domain_id='default')

    sess = session.Session(auth=auth)
    token = sess.get_token()

    url = "http://252.3.243.35:8041/v1/metric/"+sys.argv[2]+"/measures"
    headers = {'content-type': 'application/json', 'X-AUTH-TOKEN': token}

    df = pd.read_csv("data/csv/temperature_2017.csv")

    rng = pd.date_range('20210301 00:00', '20210610 23:59', freq='1S')
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
