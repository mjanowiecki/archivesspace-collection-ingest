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

# Get list of all corporate entities identifiers from ArchivesSpace.
endpoint = '/agents/corporate_entities?all_ids=true'
corporate_ids = requests.get(base_url+endpoint, headers=headers).json()
total_corporate_entities = len(corporate_ids)
print('Total of {} corporate_entities.'.format(total_corporate_entities))

# Get properties from each corporate entity and put in all_items.
all_items = []
for count, corporate_id in enumerate(corporate_ids):
    print(count, corporate_id)
    endpoint = '/agents/corporate_entities/'+str(corporate_id)
    output = requests.get(base_url+endpoint, headers=headers).json()
    corporate_dict = {}
    uri = output['uri']
    names = output['names'][0]
    for key, value in names.items():
        corporate_dict[key] = value
    corporate_dict['uri'] = uri
    all_items.append(corporate_dict)

# Convert all_items to CSV and save.
df = pd.DataFrame.from_records(all_items)
print(df.head(15))
dt = datetime.now().strftime('%Y-%m-%d%H.%M.%S')
df.to_csv('allCorporateEntities_'+dt+'.csv', index=False)

# Calculate total time of script and print to terminal.
elapsed_time = time.time() - start_time
m, s = divmod(elapsed_time, 60)
h, m = divmod(m, 60)
print('Total script run time: ', '%d:%02d:%02d' % (h, m, s))
