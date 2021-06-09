import os
import datetime
import requests
import json
import pandas as pd
import util
from time import sleep


# with this program you can insert data periodically simulating a sensor measure
if __name__ == "__main__":

    headers, str_token = util.connection()

    print("Token: " + str_token)
    util.print_things("metric", str_token)

    metric = input("Choose the metricID -> ")
    url = "http://252.3.243.35:8041/v1/metric/" + metric + "/measures"

    os.system("ls data/")
    file_input = input("Insert the path of the .csv file -> ")
    df = pd.read_csv("data/" + file_input)

    threshold = input("How many data do you want to insert? (number of values) -> ")
    if int(threshold) < 0:
        print("threshold value not valid..\n Closing..\n")
        exit(-1)

    how_many = 0

    for row in df.itertuples():
        current_date = datetime.datetime.now().isoformat()
        timestamp, value = [], []
        value.append(float(row[2]))
        timestamp.append(current_date)
        how_many = how_many + 1
        measures = [{"timestamp": t, "value": v} for t, v in zip(timestamp, value)]
        r = requests.post(url, data=json.dumps(measures), headers=headers)
        if str(r.status_code) == "202":
            print("current_date: " + current_date)
        else:
            print("Error code: " + str(r.status_code))

        if how_many == int(threshold):
            break
        else:
            sleep(2)



