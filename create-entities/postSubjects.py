"""Create subject JSON files for ArchivesSpace from CSV template. Option to POST to ArchivesSpace via API or save JSON file to local machine."""

import json
import requests
import time
import secret
import pandas as pd
import argparse
from datetime import datetime
import extractvalues as ev

# Create argparse inputs for terminal.
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
    post_record = input('Enter True to post records to AS: ')

# Start script timer.
start_time = time.time()

# If posting to ArchivesSpace, authenticate to stage or production instance with secret files.
if post_record == 'True':
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
    headers = {'X-ArchivesSpace-Session': session}
else:
    pass

# Convert CSV with subject information into DataFrame.
df = pd.read_csv(filename)

all_items = []
for index, row in df.iterrows():
    # Get subject information from CSV.
    term = row['term']
    print('Gathering subject #{}: {}.'.format(index,  term))

    # Build JSON record for subject.
    subject_record = {'jsonmodel_type': 'subject', 'vocabulary': '/vocabularies/1',
                      'publish': True, 'is_slug_auto': True, 'title': term}

    ev.add_controlled_term(row, subject_record,'source', 'source', ev.subject_source_values)
    ev.add_single_string_value(row, subject_record, 'authority_id', 'authority_id')

    term_dict = {'jsonmodel_type': 'term', 'vocabulary': '/vocabularies/1', 'term': term}
    ev.add_controlled_term(row, term_dict, 'term_type', 'term_type', ev.subject_type_values)
    subject_record['terms'] = [term_dict]

    # Create dictionary for item log.
    item_log = {}

    if post_record == 'True':
        try:
            # Try to POST JSON to ArchivesSpace API subject endpoint.
            post_response = requests.post(base_url+'/subjects', headers=headers, json=subject_record).json()
            # Get URI and add to item log
            uri = post_response['uri']
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
            error = post_response['error']
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
            json.dump(subject_record, fp)
        print('Subject record JSON successfully created with filename {}'.format(s_filename))
        item_log = {'filename': s_filename, 'term': term}
        all_items.append(item_log)
    print('')

# Convert all_items log to DataFrame.
log = pd.DataFrame.from_records(all_items)

# Create CSV of item log from DataFrame.
dt = datetime.now().strftime('%Y-%m-%d%H.%M.%S')
subject_csv = 'postNewSubjectsLog_'+dt+'.csv'
log.to_csv(subject_csv, index=False)
print('{} created.'.format(subject_csv))

# Calculate total time of script and print to terminal.
elapsed_time = time.time() - start_time
m, s = divmod(elapsed_time, 60)
h, m = divmod(m, 60)
print('Total script run time: ', '%d:%02d:%02d' % (h, m, s))
