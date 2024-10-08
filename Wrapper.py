import subprocess
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys
import time

repository_id = 'UMASS.UMA'
password = 'SCh0lC0mm1967'
fileName = sys.argv[1]
now = time.time()

doi_data = subprocess.run(['./csvReaderOP', fileName], capture_output=True, text=True, check=True)

print(doi_data.stdout)
print(type(doi_data.stdout))

try:

    clean_data = '[' + ','.join(line.strip() for line in doi_data.stdout.strip().split('\n') if line.strip()) + ']'
    json_data = json.loads(clean_data)
    print(json.dumps(json_data, indent=4))
except Exception as e:
    print(f"An error occurred: {e}")
    clean_data = '[]'
doi_data = json.loads(clean_data)

def make_request(doi_id, idUrl):
    print("DOI ID from CSV:", doi_id)
    idUrl = f"https://scholarworks.umass.edu/entities/publication/{idUrl}"
    print("Constructed URL:", idUrl)

    realIdArray = doi_id.split('/')
    realId = realIdArray[-1]
    realId = realId.replace(',','')
    doi_id = "10.7275/" + realId

    payload = {
        "data": {
            "type": "dois",
            "attributes": {
                "url": idUrl
            }
        }
    }

    json_file_name = f'my_doi_update_{doi_id.split("/")[-1]}.json'
    with open(json_file_name, 'w') as json_file:
        json.dump(payload, json_file, indent=4)

    command = f'curl -X PUT -H "Content-Type: application/vnd.api+json" --user {repository_id}:{password} -d @{json_file_name} https://api.datacite.org/dois/{doi_id}'
    result = subprocess.run(command, shell=True, text=True, capture_output=True)
    response_code = result.stdout.strip()
    return doi_id, response_code


with ThreadPoolExecutor(max_workers=200) as executor:
    futures = {executor.submit(make_request, data['doi'], data['id']): data for data in doi_data if isinstance(data, dict)}
    for future in as_completed(futures):
        doi_id, response_code = future.result()
        print(f"DOI ID: {doi_id}, Response Code: {response_code}")

end = time.time()
print (end-now)
