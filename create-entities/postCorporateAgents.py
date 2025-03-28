import json
import requests
import secret
import time
import pandas as pd
import argparse
from datetime import datetime
import extractvalues as ev

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

# Convert CSV with corporate entity information into DataFrame.
df = pd.read_csv(filename)

all_items = []
for index, row in df.iterrows():
    # Get agent information from CSV.
    primary_name = row['primary_name']
    print('Gathering corporate entity #{}: {}.'.format(index, primary_name))

    agentRecord = {'jsonmodel_type': 'agent_corporate_entity'}
    ev.add_to_dict(row, agentRecord, 'publish', 'publish_corporate_body')
    names = []
    name = {'jsonmodel_type': 'name_corporate_entity',
            'sort_name_auto_generate': True,
            'authorized': True,
            'is_display_name': True}
    ev.add_to_dict(row, name, 'subordinate_name_1', 'subordinate_name_1')
    ev.add_to_dict(row, name, 'subordinate_name_2', 'subordinate_name_2')
    ev.add_to_dict(row, name, 'qualifier', 'qualifier')
    ev.add_to_dict(row, name, 'authority_id', 'authority_id')
    ev.add_to_dict(row, name, 'source', 'source')
    ev.add_to_dict(row, name, 'rules', 'rules')
    ev.add_to_dict(row, name, 'name_order', 'name_order')
    ev.add_to_dict(row, name, 'use_dates', 'use_dates')
    names.append(name)
    agentRecord['names'] = names

    dates = ev.add_dates(row, 'dates_of_existence')
    if dates:
        agentRecord['dates_of_existence'] = dates

    notes = []
    note = {}
    subnotes = []
    subnote = {}
    ev.add_to_dict(row, note, 'jsonmodel_type', 'note_jsonmodel_type')
    ev.add_to_dict(row, note, 'publish', 'publish_note')
    ev.add_to_dict(row, subnote, 'content', 'content')
    ev.add_to_dict(row, subnote, 'jsonmodel_type', 'subnote_jsonmodel_type')
    ev.add_to_dict(row, subnote, 'publish', 'publish_subnote')
    if subnote:
        subnotes.append(subnote)
        note['subnotes'] = subnotes
    if note:
        notes.append(note)
        agentRecord['notes'] = notes

    # Create dictionary for item log.
    item_log = {}

    if post_record == 'True':
        # Create JSON record for corporate entity.
        agentRecord = json.dumps(agentRecord)
        print('JSON created for {}.'.format(primary_name))

        try:
            # Try to POST JSON to ArchivesSpace API corporate entities' endpoint.
            post = requests.post(base_url+'/agents/corporate_entities', headers=headers, data=agentRecord).json()
            print(json.dumps(post))
            uri = post['uri']
            title = post['title']
            print('Corporate entity successfully created with URI: {}'.format(uri))
            item_log = {'uri': uri, 'agent_name': title}
            # Add item log to list of logs
            all_items.append(item_log)

        except requests.exceptions.JSONDecodeError:
            # If POST to ArchivesSpace fails, break loop.
            item_log = {'uri': 'error', 'agent_name': primary_name}
            # Add item log to list of logs
            all_items.append(item_log)
            print('POST to AS failed, breaking loop.')

        except KeyError:
            # If JSON error occurs, record here.
            error = post['error']
            item_log = {'error': error, 'agent_name': primary_name}
            # Add item log to list of logs
            all_items.append(item_log)
            print('POST to AS failed.')

    else:
        # Create JSON records on your computer to review. Does not post to AS.
        dt = datetime.now().strftime('%Y-%m-%d')
        index += 1
        identifier = 'corporateAgent_'+str(index).zfill(3)
        ca_filename = identifier+'_'+dt+'.json'
        directory = ''
        with open(directory+ca_filename, 'w') as fp:
            json.dump(agentRecord, fp)
        print('Agent record JSON successfully created with filename {}'.format(ca_filename))
        item_log = {'filename': ca_filename, 'primary_name': primary_name}
        all_items.append(item_log)
    print('')


# Convert all_items to DataFrame.
log = pd.DataFrame.from_reccords(all_items)

# Create CSV of all item logs.
dt = datetime.now().strftime('%Y-%m-%d%H.%M.%S')
corporateCSV = 'postNewCorporateAgents_'+dt+'.csv'
log.to_csv(corporateCSV)
print('{} created.'.format(corporateCSV))

elapsed_time = time.time() - start_time
m, s = divmod(elapsed_time, 60)
h, m = divmod(m, 60)
print('Total script run time: ', '%d:%02d:%02d' % (h, m, s))
