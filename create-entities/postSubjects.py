import json
import requests
import time
import secret
import pandas as pd
import argparse
from datetime import datetime

parser = argparse.ArgumentParser()
parser.add_argument('-f', '--file')
parser.add_argument('-p', '--post_record')
args = parser.parse_args()

if args.file:
    filename = args.file
else:
    filename = input('Enter filename (including \'.csv\'): ')
if args.post_record:
    post_record = args.post_record
else:
    post_record = input('Enter True to post records to AS.')

start_time = time.time()

if post_record == 'True':
    # Log into ArchivesSpace and start session on selected server.
    secretVersion = input('To edit production server, enter secret filename: ')
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
    repository = secret.repository

    auth = requests.post(base_url+'/users/'+user+'/login?password='+password).json()
    session = auth['session']
    headers = {'X-ArchivesSpace-Session': session, 'Content_Type': 'application/json'}
else:
    pass

# Convert CSV with subject information into DataFrame.
df = pd.read_csv(filename)

all_items = []
for index, row in df.iterrows():
    # Get subject information from CSV.
    term = row['term']
    print('Gathering subject #{}: {}.'.format(index,  term))
    subject_type = row['term_type']
    source = row['source']
    authority_id = row['authority_id']

    # Build JSON record for subject.
    subjectRecord = {'jsonmodel_type': 'subject'}
    terms = []
    term_dict = {'jsonmodel_type': 'term', 'term':  term, 'term_type': subject_type, 'vocabulary': '/vocabularies/1'}
    terms.append(term_dict)
    subjectRecord['terms'] = terms
    subjectRecord['title'] = term
    subjectRecord['publish'] = True
    subjectRecord['source'] = source
    subjectRecord['vocabulary'] = '/vocabularies/1'
    subjectRecord['authority_id'] = authority_id

    # Create dictionary for item log.
    item_log = {}

    if post_record == 'True':
        # Create JSON record for subject.
        subjectRecord = json.dumps(subjectRecord)
        print(subjectRecord)
        print('JSON created for {}.'.format(term))

        try:
            # Try to POST JSON to ArchivesSpace API subject endpoint.
            post = requests.post(base_url+'/subjects', headers=headers, data=subjectRecord).json()
            # Get URI and add to item log
            uri = post['uri']
            print('Subject successfully created with URI: {}'.format(uri))
            item_log = {'uri': uri, 'subject_name': term}
            # Add item log to list of logs
            all_items.append(item_log)

        except requests.exceptions.JSONDecodeError:
            # If POST to ArchivesSpace fails, break loop.
            item_log = {'uri': 'error', 'subject_name': term}
            # Add item log to list of logs
            all_items.append(item_log)
            print('POST to AS failed.')

        except KeyError:
            # If JSON error occurs, record here.
            error = post['error']
            item_log = {'error': error, 'subject_name': term}
            # Add item log to list of logs
            all_items.append(item_log)
            print('POST to AS failed.')

    else:
        # Create JSON records on your computer to review. Does not post to AS.
        dt = datetime.now().strftime('%Y-%m-%d')
        index += 1
        identifier = 'subject_'+str(index).zfill(3)
        s_filename = identifier+'_'+dt+'.json'
        directory = ''
        with open(directory+s_filename, 'w') as fp:
            json.dump(subjectRecord, fp)
        print('Subject record JSON successfully created with filename {}'.format(s_filename))
        item_log = {'filename': s_filename, 'term': term}
        all_items.append(item_log)
    print('')

# Convert all_items to DataFrame.
log = pd.DataFrame.from_records(all_items)

# Create CSV of all item logs.
dt = datetime.now().strftime('%Y-%m-%d%H.%M.%S')
subject_csv = 'postNewSubjects_'+dt+'.csv'
log.to_csv(subject_csv)
print('{} created.'.format(subject_csv))

elapsed_time = time.time() - start_time
m, s = divmod(elapsed_time, 60)
h, m = divmod(m, 60)
print('Total script run time: ', '%d:%02d:%02d' % (h, m, s))
