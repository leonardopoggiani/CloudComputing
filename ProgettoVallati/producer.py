import sys

import requests
import json
import pandas as pd

if __name__ == "__main__":

    if len(sys.argv) < 4:
        print("Usage: producer_gnocchi  <token> <id_metrica> <dati_input>", file=sys.stderr)
        sys.exit(-1)

    url = "http://252.3.243.35:8041/v1/metric/"+sys.argv[2]+"/measures"
    headers = {'content-type': 'application/json', 'X-AUTH-TOKEN': sys.argv[1]}

    with open(sys.argv[3], "r") as lines:
        rng = pd.date_range('20210301 00:00', '20210610 23:59', freq='1S')
        index = 0
        for line in lines:
            timestamp, value = [], []
            value.append(float(line.split(" ")[2]))
            timestamp.append(str(rng.to_series()[index]))
            index = index+1
            measures = [{"timestamp": t, "value": v} for t, v in zip(timestamp, value)]
            r = requests.post(url, data=json.dumps(measures), headers=headers)
            if str(r.status_code) == "202":
                print("inserted measure")
            else:
                print(str(r.status_code))
            if index == len(rng):
                break
            print(index)

#ID METRIC 67d112e8-4d3c-4ae6-8577-0175ac48da1d
