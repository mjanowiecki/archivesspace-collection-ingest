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

    corporate_record = {'jsonmodel_type': 'agent_corporate_entity'}
    ev.add_single_string_value(row, corporate_record, 'publish', 'publish_corporate_body')
    names = []
    name = {'jsonmodel_type': 'name_corporate_entity',
            'sort_name_auto_generate': True,
            'authorized': True,
            'is_display_name': True}
    ev.add_single_string_value(row, name, 'authority_id', 'authority_id')
    ev.add_single_string_value(row, name, 'source', 'source')
    ev.add_single_string_value(row, name, 'rules', 'rules')
    ev.add_single_string_value(row, name, 'subordinate_name_1', 'subordinate_name_1')
    ev.add_single_string_value(row, name, 'subordinate_name_2', 'subordinate_name_2')
    ev.add_single_string_value(row, name, 'number', 'number')
    ev.add_single_string_value(row, name, 'dates', 'dates')
    ev.add_single_string_value(row, name, 'location', 'location')
    ev.add_single_string_value(row, name, 'conference_meeting', 'conference_meeting')
    ev.add_single_string_value(row, name, 'jurisdiction', 'jurisdiction')
    ev.add_single_string_value(row, name, 'qualifier', 'qualifier')
    names.append(name)
    corporate_record['names'] = names

    dates = ev.add_dates_of_existence(row, 'dates_of_existence')
    if dates:
        corporate_record['dates_of_existence'] = dates

    notes = ev.add_agent_notes(row, 'notes')
    if notes:
        corporate_record['notes'] = notes
        
    # Create dictionary for item log.
    item_log = {}

    if post_record == 'True':
        # Create JSON record for corporate entity.
        corporate_record = json.dumps(corporate_record)
        print('JSON created for {}.'.format(primary_name))

        try:
            # Try to POST JSON to ArchivesSpace API corporate entities' endpoint.
            post = requests.post(base_url+'/agents/corporate_entities', headers=headers, data=corporate_record).json()
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
            json.dump(corporate_record, fp)
        print('Agent record JSON successfully created with filename {}'.format(ca_filename))
        item_log = {'filename': ca_filename, 'primary_name': primary_name}
        all_items.append(item_log)
    print('')


# Convert all_items to DataFrame.
log = pd.DataFrame.from_records(all_items)

# Create CSV of all item logs.
dt = datetime.now().strftime('%Y-%m-%d%H.%M.%S')
corporateCSV = 'postNewCorporateAgents_'+dt+'.csv'
log.to_csv(corporateCSV)
print('{} created.'.format(corporateCSV))

elapsed_time = time.time() - start_time
m, s = divmod(elapsed_time, 60)
h, m = divmod(m, 60)
print('Total script run time: ', '%d:%02d:%02d' % (h, m, s))
