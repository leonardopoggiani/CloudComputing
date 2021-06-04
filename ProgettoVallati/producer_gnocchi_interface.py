import requests
import json
import pandas as pd
import util

# with this program you can choose the actions to perform with gnocchi
if __name__ == "__main__":

    commands = "This is the Gnocchi database interface, choose an action to perform:\n" \
               "1) Show all metrics\n" + \
               "2) Show all policies\n" + \
               "3) Insert a new metric\n" + \
               "4) Delete a metric\n" + \
               "5) Insert measures in a metric\n" + \
               "6) Exit\n "

    headers, str_token = util.connection()

    print(headers)
    print(str_token)

    while True:
        print(commands)
        what = input("What do you want to do? -> ")

        if what.isnumeric() is False or int(what) > 6 or int(what) < 1:
            print("Please select a valid option ->\n\n\n" + commands)
            continue

        # just print metrics or policies
        if int(what) == 1 or int(what) == 2:
            if int(what) == 1:
                command = "metric"
            else:
                command = "archive_policy"
            util.print_things(command, str_token)

        # create a new metric
        if int(what) == 3:
            util.print_things("archive_policy", str_token)  # stampo le policy per vedere quale inserire

            policy = input("\n\n Insert the policy name -> ")
            url = "http://252.3.243.35:8041/v1/metric"
            data = {"archive_policy_name": str(policy)}
            r = requests.post(url, data=json.dumps(data), headers=headers)
            if str(r.status_code) == "201":
                print("Metric successfully created!")
            else:
                print("Error code" + str(r.status_code))

        # delete a metric by ID
        if int(what) == 4:

            util.print_things("metric", str_token)

            metric = input("Insert metric ID: ")
            url = "http://252.3.243.35:8041/v1/metric/" + metric
            r = requests.delete(url, headers=headers)
            if str(r.status_code) == "204":
                print("Metric successfully deleted!")
            else:
                print("Error code: " + r.status_code)

        # insert a measure in a metric by ID
        if int(what) == 5:

            util.print_things("metric", str_token)

            metric = input("Insert metric ID: ")
            file = input("Insert input file path (must be a .csv): ")

            df = pd.read_csv(file)

            url = "http://252.3.243.35:8041/v1/metric/" + metric + "/measures"

            rng = pd.date_range('20210602 19:00', '20210603 12:00', freq='30S')
            index = 0

            for line in df.itertuples():
                timestamp, value = [], []
                value.append(float(line[2]))
                timestamp.append(str(rng.to_series()[index]))
                index = index + 1
                measures = [{"timestamp": t, "value": v} for t, v in zip(timestamp, value)]
                r = requests.post(url, data=json.dumps(measures), headers=headers)
                if str(r.status_code) == "202":
                    print("Measures successfully inserted!")
                else:
                    print("Error code" + str(r.status_code))

                if index == len(rng):
                    break

        if int(what) == 6:
            print("Closing..")
            break
