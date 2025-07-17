import requests
import secret
import pandas as pd
from datetime import datetime
import time

start_time = time.time()

# Gather login info and authenticate session in ArchivesSpace.
secretVersion = input('To edit production server, enter secret file name: ')
if secretVersion != '':
    try:
        secret = __import__(secretVersion)
        print('Editing Production')
    except ImportError:
        print('Editing Development')
else:
    print('Editing Development')

base_url = secret.base_url
user = secret.user
password = secret.password

auth = requests.post(base_url+'/users/'+user+'/login?password='+password).json()
session = auth["session"]
headers = {'X-ArchivesSpace-Session': session, 'Content_Type': 'application/json'}
print('authenticated')

# Get list of all person identifiers from ArchivesSpace.
endpoint = '/agents/people?all_ids=true'
person_ids = requests.get(base_url+endpoint, headers=headers).json()
total_persons = len(person_ids)
print('Total of {} persons.'.format(total_persons))

# Get properties from each person agent and put in all_items.
all_items = []
for count, person_id in enumerate(person_ids):
    endpoint = '/agents/people/'+str(person_id)
    print(count, endpoint)
    output = requests.get(base_url+endpoint, headers=headers).json()
    person_dict = {}
    uri = output['uri']
    sort_name = output['names'][0]['sort_name']
    authority_id = output['names'][0].get('authority_id', '')
    person_dict['uri'] = uri
    person_dict['sort_name'] = sort_name
    person_dict['authority_id'] = authority_id
    all_items.append(person_dict)

# Convert all_items to CSV and save.
df = pd.DataFrame.from_records(all_items)
print(df.head(15))
dt = datetime.now().strftime('%Y-%m-%d%H.%M.%S')
df.to_csv('allPersons_'+dt+'.csv', index=False)

# Calculate total time of script and print to terminal.
elapsed_time = time.time() - start_time
m, s = divmod(elapsed_time, 60)
h, m = divmod(m, 60)
print('Total script run time: ', '%d:%02d:%02d' % (h, m, s))
