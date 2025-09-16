"""Create family JSON files for ArchivesSpace from CSV template. Option to POST to ArchivesSpace via API or save JSON file to local machine."""

import json
import requests
import secret
import time
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
    headers = {'X-ArchivesSpace-Session': session}
else:
    pass

# Convert CSV with family information into DataFrame.
df = pd.read_csv(filename)

all_items = []
for index, row in df.iterrows():
    sort_name = row.get('sort_name')
    print('Gathering family #{}: {}.'.format(index, sort_name))

    # Get agent information from CSV.
    publish_family = row.get('publish_family')
    family_record = {'agent_type': 'agent_family', 'jsonmodel_type': 'agent_family', 'publish': publish_family}

    # Add name components.
    names = []
    name = {'jsonmodel_type': 'name_family',
            'sort_name_auto_generate': True,
            'authorized': True,
            'is_display_name': True}

    ev.add_single_string_value(row, name, 'authority_id', 'authority_id')
    ev.add_controlled_term(row, name, 'source', 'source', ev.name_source_values)
    ev.add_controlled_term(row, name, 'rules', 'rules', ev.name_rule_values)
    ev.add_single_string_value(row, name, 'family_name', 'family_name')
    ev.add_single_string_value(row, name, 'dates', 'dates')
    ev.add_single_string_value(row, name, 'family_type', 'family_type')
    ev.add_single_string_value(row, name, 'qualifier', 'qualifier')
    names.append(name)
    family_record['names'] = names

    # Add dates of existence.
    dates = ev.add_dates_of_existence(row, 'dates_of_existence')
    if dates:
        family_record['dates_of_existence'] = dates

    # Add notes.
    notes = ev.add_agent_notes(row, 'notes')
    if notes:
        family_record['notes'] = notes

    # Create dictionary for item log.
    item_log = {}

    if post_record == 'True':
        try:
            # Try to POST JSON to ArchivesSpace API families endpoint.
            post_response = requests.post(base_url+'/agents/families', headers=headers, json=family_record).json()
            uri = post_response['uri']
            print('Family successfully created with URI: {}'.format(uri))
            item_log = {'uri': uri, 'agent_name': sort_name}
            # Add item log to list of logs
            all_items.append(item_log)

        except requests.exceptions.JSONDecodeError:
            # If POST to ArchivesSpace fails, break loop.
            item_log = {'uri': 'error', 'agent_name': sort_name}
            # Add item log to list of logs
            all_items.append(item_log)
            print('POST to AS failed, breaking loop.')

        except KeyError:
            # If JSON error occurs, record here.
            error = post_response['error']
            item_log = {'error': error, 'agent_name': sort_name}
            # Add item log to list of logs
            all_items.append(item_log)
            print('POST to AS failed.')
    else:
        # Create JSON records on your computer to review. Does not post to AS.
        dt = datetime.now().strftime('%Y-%m-%d')
        index = str(index+1)
        identifier = 'family_'+index.zfill(3)
        fa_filename = identifier+'_'+dt+'.json'
        directory = ''
        with open(directory+fa_filename, 'w') as fp:
            json.dump(family_record, fp)
        print('Agent record JSON successfully created with filename {}'.format(fa_filename))
        item_log = {'filename': fa_filename, 'sort_name': sort_name}
        all_items.append(item_log)
    print('')

# Convert all_items log to DataFrame.
log = pd.DataFrame.from_records(all_items)

# Create CSV of item log from DataFrame.
dt = datetime.now().strftime('%Y-%m-%d%H.%M.%S')
familyCSV = 'postNewFamilyAgents_'+dt+'.csv'
log.to_csv(familyCSV, index=False)
print('{} created.'.format(familyCSV))

# Calculate total time of script and print to terminal.
elapsed_time = time.time() - start_time
m, s = divmod(elapsed_time, 60)
h, m = divmod(m, 60)
print('Total script run time: ', '%d:%02d:%02d' % (h, m, s))
