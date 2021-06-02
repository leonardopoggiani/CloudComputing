import pprint

import requests
import json
import pandas as pd
import glob
import util
files = glob.glob("data/csv/*.csv")


if __name__ == "__main__":

    headers, str_token = util.connection()
    print(headers)
    print(str_token)

    df = pd.DataFrame()

    util.print_things("archive_policy", str_token)  # stampo le policy per vedere quale inserire

    policy = input("\n\n Insert the policy name -> ")

    for f in files:
        df = pd.read_csv(f)

        rng = pd.date_range('20210602 19:00', '20210603 12:00', freq='30S')

        url = "http://252.3.243.35:8041/v1/metric"
        data = {"archive_policy_name": str(policy)}
        r = requests.post(url, data=json.dumps(data), headers=headers)
        if str(r.status_code) == "201":
            print("Metric successfully created!")

        else:
            print("Error code " + str(r.status_code))

        index = 0

        util.print_things('metric', str_token)
        metric = input("Insert metricID -> ")

        for line in df.itertuples():

            timestamp, value = [], []
            value.append(float(line[2]))
            timestamp.append(str(rng.to_series()[index]))
            index = index + 1
            measures = [{"timestamp": t, "value": v} for t, v in zip(timestamp, value)]
            r = requests.post(url + "/" + metric + "/measures", data=json.dumps(measures), headers=headers)
            if str(r.status_code) == "202":
                print("inserted measure")
            else:
                print("Error code " + str(r.status_code))
            if index == len(rng):
                break


#ID METRIC 67d112e8-4d3c-4ae6-8577-0175ac48da1d
