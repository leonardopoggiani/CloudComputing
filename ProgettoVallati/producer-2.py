import os
import requests
import json
import pandas as pd

if __name__ == "__main__":
    '''
    auth = v3.Password(auth_url='http://172.16.50.247:5000/v3',
                       username='admin',
                       password='openstack',
                       project_name='admin',
                       user_domain_id='default',
                       project_domain_id='default')
    sess = session.Session(auth=auth)
    token = sess.get_token()
    '''

    os.system("sh admin-openrc.sh")
    os.system("openstack token issue > output_token.txt")
    os.system("cat output_token.txt | grep \"gAAA[^[:space:]]*\" -o > token.txt")
    metric = input("Please, insert metric ID: ")
    file = input("Please, insert input file path: ")

    with open("token.txt") as token_file:
        token = token_file.readlines()

        str_token = token.pop().replace('\n', '')

        url = "http://252.3.243.35:8041/v1/metric/" + metric + "/measures"

        headers = {'content-type': 'application/json', 'X-AUTH-TOKEN': str_token}

    df = pd.read_csv(file)

    rng = pd.date_range('20210528 19:00', '20210528 23:59', freq='30S')
    index = 0
    for line in df.itertuples():
        timestamp, value = [], []
        value.append(float(line[2]))
        timestamp.append(str(rng.to_series()[index]))
        index = index + 1
        measures = [{"timestamp": t, "value": v} for t, v in zip(timestamp, value)]
        print(measures)
        r = requests.post(url, data=json.dumps(measures), headers=headers)
        print(r)
        # if str(r.status_code) == "202":
        # print("inserted measure")
        # else:
        # print(str(r.status_code))
        if index == len(rng):
            break

    # ID METRIC f35014af-aaaa-4734-9def-b0ab8303ffa1