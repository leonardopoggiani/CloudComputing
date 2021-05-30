import os
import requests
import json
import pandas as pd

if __name__ == "__main__":
<<<<<<< Updated upstream
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

    #os.system("sh admin-openrc.sh")
=======

    os.system("sh admin-openrc.sh")
>>>>>>> Stashed changes
    os.system("openstack token issue > output_token.txt")
    os.system("cat output_token.txt | grep \"gAAA[^[:space:]]*\" -o > token.txt")

    with open("token.txt") as token_file:
        token = token_file.readlines()
<<<<<<< Updated upstream
        str_token = token.pop().replace('\n', '')
        headers = {'content-type': 'application/json', 'X-AUTH-TOKEN': str_token}
    while True:
        print("1) Show all metrics\n"
              "2) Show all policies\n"
              "3) Insert a new metric\n"
              "4) Delete a metric\n"
              "5) Insert measures in a metric\n")
        what = input("What do you want to do?")

        if int(what) == 1 or int(what) == 2:
            if int(what) == 1:
                finalWord = "metric"
            else:
                finalWord = "archive_policy"

            #Lista delle policies o delle metriche
            url = "http://252.3.243.35:8041/v1/" + finalWord
            headers = {'X-AUTH-TOKEN': str_token}
            r = requests.get(url, headers=headers)
            if str(r.status_code) == "200":
                parsed = json.loads(r.text)
                print(json.dumps(parsed, indent=4, sort_keys=True))
            else:
                print(str(r.status_code))

        #Creare una nuova metrica

        if int(what) == 3:
            policy = input("Insert the policy name: ")
            url = "http://252.3.243.35:8041/v1/metric"
            data = {"archive_policy_name": str(policy)}
            r = requests.post(url, data=json.dumps(data), headers=headers)
            if str(r.status_code) == "201":
                print("Metric successfully created!")
            else:
                print(str(r.status_code))

        #Eliminare una metrica

        if int(what) == 4:
            metric = input("Insert metric ID: ")
            url = "http://252.3.243.35:8041/v1/metric/" + metric
            r = requests.delete(url, headers=headers)
            if str(r.status_code) == "204":
                print("Metric successfully deleted!")
            else:
                print(r.status_code)

        #Inserire misure in una metrica
        if int(what) == 5:
            metric = input("Insert metric ID: ")
            file = input("Insert input file path: ")

            df = pd.read_csv(file)

            url = "http://252.3.243.35:8041/v1/metric/" + metric + "/measures"

            rng = pd.date_range('20210531 19:00', '20210531 23:59', freq='30S')
            index = 0
            for line in df.itertuples():
                timestamp, value = [], []
                value.append(float(line[2]))
                timestamp.append(str(rng.to_series()[index]))
                index = index + 1
                measures = [{"timestamp": t, "value": v} for t, v in zip(timestamp, value)]
                r = requests.post(url, data=json.dumps(measures), headers=headers)
                print(r)
                if str(r.status_code) == "202":
                    print("Measures successfully inserted!")
                else:
                    print(str(r.status_code))
                if index == len(rng):
                    break

    # ID METRIC f35014af-aaaa-4734-9def-b0ab8303ffa1

    #Ottenere la lista delle policy
    #curl - H "X-AUTH-TOKEN:"http: // 252.3.243.35: 8041 / v1 / archive_policy
=======

        str_token = token.pop().replace('\n', '')

        url = "http://252.3.243.35:8041/v1/metric/67d112e8-4d3c-4ae6-8577-0175ac48da1d/measures"

        headers = {'content-type': 'application/json', 'X-AUTH-TOKEN': str_token}

    df = pd.read_csv("data/csv/temperature_2017.csv")

    rng = pd.date_range('20210501 00:00', '20210701 23:59', freq='5S')
    index = 0

    for line in df.itertuples():
        timestamp, value = [], []
        value.append(float(line[2]))
        timestamp.append(str(rng.to_series()[index]))
        index = index + 1
        measures = [{"timestamp": t, "value": v} for t, v in zip(timestamp, value)]
        r = requests.post(url, data=json.dumps(measures), headers=headers)
        # if str(r.status_code) == "202":
            # print("inserted measure")
        # else:
            # print(str(r.status_code))
        if index == len(rng):
             break

# ID METRIC 67d112e8-4d3c-4ae6-8577-0175ac48da1d

>>>>>>> Stashed changes
