import requests
import secret
import pandas as pd
from datetime import datetime
import time

# Start script timer.
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
endpoint = '/agents/families?all_ids=true'
family_ids = requests.get(base_url+endpoint, headers=headers).json()
total_families = len(family_ids)
print('Total of {} families.'.format(total_families))

# Get properties from each person agent and put in all_items log.
all_items = []
for count, family_id in enumerate(family_ids):
    endpoint = '/agents/families/'+str(family_id)
    print(count, endpoint)
    output = requests.get(base_url+endpoint, headers=headers).json()
    person_dict = {}
    uri = output['uri']
    primary_name = output['names'][0]
    sort_name = primary_name['sort_name']
    authority_id = primary_name.get('authority_id', '')
    person_dict['uri'] = uri
    person_dict['sort_name'] = sort_name
    person_dict['authority_id'] = authority_id
    all_items.append(person_dict)

# Convert all_items to DataFrame, then to CSV and save.
df = pd.DataFrame.from_records(all_items)
print(df.head(15))
dt = datetime.now().strftime('%Y-%m-%d%H.%M.%S')
df.to_csv('allFamilies_'+dt+'.csv', index=False)

# Calculate total time of script and print to terminal.
elapsed_time = time.time() - start_time
m, s = divmod(elapsed_time, 60)
h, m = divmod(m, 60)
print('Total script run time: ', '%d:%02d:%02d' % (h, m, s))
