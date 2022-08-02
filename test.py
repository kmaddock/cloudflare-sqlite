from urllib import response
import requests
import os

if not os.path.exists("northwind.sql"):
    test = "https://raw.githubusercontent.com/jpwhite3/northwind-SQLite3/master/src/create.sql"
    response = requests.get(test)
    with open("northwind.sql", "w") as f:
        f.write(response.text)

sql = []
with open("northwind.sql", "r") as f:
    for line in f.readlines():
        if line.startswith("--"):
            if len(sql) != 0:
                sql = ['PRAGMA foreign_keys=off;', 'BEGIN TRANSACTION;'] + sql + ['COMMIT;']
                response = requests.post("http://localhost:8787/query", data="\n".join(sql))
                print(response.text)
                if response.status_code != 200:
                    break

                sql = []
        else:
            sql.append(line.strip())

requests.get("http://localhost:8787/gc")
