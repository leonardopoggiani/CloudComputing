import os
import requests
import json
import pandas as pd


def print_things(parameter):
    url = "http://252.3.243.35:8041/v1/" + parameter
    headers = {'X-AUTH-TOKEN': str_token}
    r = requests.get(url, headers=headers)
    if str(r.status_code) == "200":
        parsed = json.loads(r.text)
        print(json.dumps(parsed, indent=4, sort_keys=True))
    else:
        print("Error code: " + str(r.status_code))

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

    commands = "1) Show all metrics\n" + \
               "2) Show all policies\n" + \
               "3) Insert a new metric\n" + \
               "4) Delete a metric\n" + \
               "5) Insert measures in a metric\n" + \
               "6) Exit\n "

    admin_openrc()
    os.system("openstack token issue > output_token.txt")  # salva il token di accesso in file di testo
    os.system(
        "cat output_token.txt | grep \"gAAA[^[:space:]]*\" -o > token.txt")  # recupera solamente il token e lo salva su un altro file

    with open("token.txt") as token_file:
        token = token_file.readlines()
        str_token = token.pop().replace('\n', '')  # per sicurezza se leggo anche il terminatore di stringa
        headers = {'content-type': 'application/json',
                   'X-AUTH-TOKEN': str_token}  # compongo il messaggio da inviare a gnocchi

    while True:

        # interfaccia utente
        print(commands)
        what = input("What do you want to do? -> ")

        # comando inserito non valido
        if what.isnumeric() is False or int(what) > 6 or int(what) < 1:
            print("Please select a valid option ->\n\n\n" + commands)
            continue

        if int(what) == 1 or int(what) == 2:
            if int(what) == 1:
                command = "metric"
            else:
                command = "archive_policy"

            # Lista delle policies o delle metriche
            print_things(command)

        # Creare una nuova metrica

        if int(what) == 3:

            print_things("archive_policy")  # stampo le policy per vedere quale inserire

            policy = input("\n\n Insert the policy name -> ")
            url = "http://252.3.243.35:8041/v1/metric"
            data = {"archive_policy_name": str(policy)}
            r = requests.post(url, data=json.dumps(data), headers=headers)
            if str(r.status_code) == "201":
                print("Metric successfully created!")
            else:
                print("Error code" + str(r.status_code))

        # Eliminare una metrica

        if int(what) == 4:

            print_things("metric")

            metric = input("Insert metric ID: ")
            url = "http://252.3.243.35:8041/v1/metric/" + metric
            r = requests.delete(url, headers=headers)
            if str(r.status_code) == "204":
                print("Metric successfully deleted!")
            else:
                print("Error code: " + r.status_code)

        # Inserire misure in una metrica
        if int(what) == 5:

            print_things("metric")

            metric = input("Insert metric ID: ")
            file = input("Insert input file path (must be a .csv): ")

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
                    print("Error code" + str(r.status_code))

                if index == len(rng):
                    break

        if int(what) == 6:
            print("Closing..")
            break

    # ID METRIC f35014af-aaaa-4734-9def-b0ab8303ffa1

    # Ottenere la lista delle policy
    # curl - H "X-AUTH-TOKEN:"http: // 252.3.243.35: 8041 / v1 / archive_policy

# ID METRIC 67d112e8-4d3c-4ae6-8577-0175ac48da1d
