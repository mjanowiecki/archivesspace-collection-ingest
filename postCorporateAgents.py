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
parser.add_argument('-p', '--postRecord')
args = parser.parse_args()

if args.file:
    filename = args.file
else:
    filename = input('Enter filename (including \'.csv\'): ')
if args.postRecord:
    postRecord = args.postRecord
else:
    postRecord = input('Enter True to post records to AS.')

startTime = time.time()

if postRecord == 'True':
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

    baseURL = secret.baseURL
    user = secret.user
    password = secret.password
    repository = secret.repository

    auth = requests.post(baseURL+'/users/'+user+'/login?password='+password).json()
    session = auth['session']
    headers = {'X-ArchivesSpace-Session': session, 'Content_Type': 'application/json'}
else:
    pass

# Convert CSV with corporate entity information into DataFrame.
df = pd.read_csv(filename)

logForAllItems = []
for index, row in df.iterrows():
    # Get agent information from CSV.
    sort_name = row['sort_name']
    print('Gathering corporate entity #{}: {}.'.format(index, sort_name))

    agentRecord = {'jsonmodel_type': 'agent_corporate_entity'}
    ev.add_to_dict(row, agentRecord, 'publish', 'publish_corporate_body')
    names = []
    name = {'jsonmodel_type': 'name_corporate_entity',
            'sort_name_auto_generate': True,
            'authorized': True,
            'is_display_name': True}
    ev.add_to_dict(row, name, 'primary_name', 'primary_name')
    ev.add_to_dict(row, name, 'subordinate_name_1', 'subordinate_name_1')
    ev.add_to_dict(row, name, 'subordinate_name_2', 'subordinate_name_2')
    ev.add_to_dict(row, name, 'subordinate_name_3', 'subordinate_name_3')
    ev.add_to_dict(row, name, 'subordinate_name_4', 'subordinate_name_4')
    ev.add_to_dict(row, name, 'qualifier', 'qualifier')
    ev.add_to_dict(row, name, 'sort_name', 'sort_name')
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
    itemLog = {}

    if postRecord == 'True':
        # Create JSON record for corporate entity.
        agentRecord = json.dumps(agentRecord)
        print('JSON created for {}.'.format(sort_name))

        try:
            # Try to POST JSON to ArchivesSpace API corporate entities' endpoint.
            post = requests.post(baseURL+'/agents/corporate_entities', headers=headers, data=agentRecord).json()
            print(json.dumps(post))
            uri = post['uri']
            sort_name = post['sort_name']
            print('Corporate entity successfully created with URI: {}'.format(uri))
            itemLog = {'uri': uri, 'sort_name': sort_name}
            # Add item log to list of logs
            logForAllItems.append(itemLog)

        except requests.exceptions.JSONDecodeError:
            # If POST to ArchivesSpace fails, break loop.
            itemLog = {'uri': 'error', 'sort_name': sort_name}
            # Add item log to list of logs
            logForAllItems.append(itemLog)
            print('POST to AS failed, breaking loop.')
            break

    else:
        # Create JSON records on your computer to review. Does not post to AS.
        dt = datetime.now().strftime('%Y-%m-%d')
        index = str(index+1)
        identifier = 'corporateAgent_'+index.zfill(3)
        ca_filename = identifier+'_'+dt+'.json'
        directory = ''
        with open(directory+ca_filename, 'w') as fp:
            json.dump(agentRecord, fp)
        print('Agent record JSON successfully created with filename {}'.format(ca_filename))
        itemLog = {'filename': ca_filename, 'sort_name': sort_name}
        logForAllItems.append(itemLog)
    print('')


# Convert logForAllItems to DataFrame.
log = pd.DataFrame.from_dict(logForAllItems)

# Create CSV of all item logs.
dt = datetime.now().strftime('%Y-%m-%d %H.%M.%S')
corporateCSV = 'postNewCorporateAgents_'+dt+'.csv'
log.to_csv(corporateCSV)
print('{} created.'.format(corporateCSV))

elapsedTime = time.time() - startTime
m, s = divmod(elapsedTime, 60)
h, m = divmod(m, 60)
print('Total script run time: ', '%d:%02d:%02d' % (h, m, s))