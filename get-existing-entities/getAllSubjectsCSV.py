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

auth = requests.post(base_url + '/users/'+user+'/login?password='+password).json()
session = auth["session"]
headers = {'X-ArchivesSpace-Session': session, 'Content_Type': 'application/json'}
print('authenticated')

# Get list of all subject identifiers from ArchivesSpace.
endpoint = '/subjects?all_ids=true'
subject_ids = requests.get(base_url+endpoint, headers=headers).json()
total_subjects = len(subject_ids)
print('Total of {} subjects.'.format(total_subjects))

# Get properties from each subject and put in all_items log.
all_items = []
for count, subject_id in enumerate(subject_ids):
    subject_dict = {}
    endpoint = '/subjects/'+str(subject_id)
    print(count, endpoint)
    output = requests.get(base_url+endpoint, headers=headers).json()
    uri = output['uri']
    title = output['title']
    authority_id = output.get('authority_id')
    source = output.get('source')
    terms = output['terms'][0]
    term_type = terms.get('term_type')
    subject_dict['uri'] = uri
    subject_dict['title'] = title
    subject_dict['authority_id'] = authority_id
    subject_dict['source'] = source
    subject_dict['term_type'] = term_type
    all_items.append(subject_dict)

# Convert all_items to DataFrame, then to CSV and save.
df = pd.DataFrame.from_records(all_items)
print(df.head(15))
dt = datetime.now().strftime('%Y-%m-%d%H.%M.%S')
df.to_csv('allSubjects_'+dt+'.csv', index=False)

# Calculate total time of script and print to terminal.
elapsed_time = time.time() - start_time
m, s = divmod(elapsed_time, 60)
h, m = divmod(m, 60)
print('Total script run time: ', '%d:%02d:%02d' % (h, m, s))
