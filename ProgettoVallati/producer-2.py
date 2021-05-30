import os
import requests
import json
import pandas as pd

if __name__ == "__main__":

    os.system("sh admin-openrc.sh")
    os.system("openstack token issue > output_token.txt")
    os.system("cat output_token.txt | grep \"gAAA[^[:space:]]*\" -o > token.txt")

    with open("token.txt") as token_file:
        token = token_file.readlines()

        str_token = token.pop().replace('\n', '')

        url = "http://252.3.243.35:8041/v1/metric/c2eb7ef2-e198-4b07-a68a-a196d114f351/measures"

        headers = {'content-type': 'application/json', 'X-AUTH-TOKEN': str_token}

    df = pd.read_csv("data/csv/temperature_2017.csv")

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

    # ID METRIC 67d112e8-4d3c-4ae6-8577-0175ac48da1d