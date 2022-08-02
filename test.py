from urllib import response
import requests

sql = """
BEGIN TRANSACTION;
CREATE TABLE IF NOT EXISTS test(val);
INSERT INTO test VALUES (666);
COMMIT;
SELECT * FROM test

"""

response = requests.post("http://localhost:8787/query", data=sql)

print(response.text)
