import json
import requests
import time
import secret
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

# Convert CSV with people information into DataFrame.
df = pd.read_csv(filename, dtype={'dates': 'string'})

all_items = []
for index, row in df.iterrows():
    # Get person information from CSV.
    sort_name = row['sort_name']
    print('Gathering person #{}: {}.'.format(index, sort_name))

    # Build JSON record for person.
    person_record = {'agent_type': 'agent_person'}
    ev.add_controlled_term(row, person_record, 'publish', 'publish_person', ev.boolean_values)
    names = []
    name = {'jsonmodel_type': 'name_person',
            'sort_name_auto_generate': True,
            'authorized': True,
            'is_display_name': True}
    ev.add_single_string_value(row, name, 'primary_name', 'primary_name')
    ev.add_single_string_value(row, name, 'rest_of_name', 'rest_of_name')
    ev.add_single_string_value(row, name, 'authority_id', 'authority_id')

    ev.add_controlled_term(row, name, 'name_order', 'name_order', ev.name_order_values)
    ev.add_controlled_term(row, name, 'rules', 'rules', ev.name_rule_values)
    ev.add_controlled_term(row, name, 'source', 'source', ev.name_source_values)

    ev.add_single_string_value(row, name, 'fuller_form', 'fuller_form')
    ev.add_single_string_value(row, name, 'title', 'title')
    ev.add_single_string_value(row, name, 'prefix', 'prefix')
    ev.add_single_string_value(row, name, 'suffix', 'suffix')
    ev.add_single_string_value(row, name, 'dates', 'dates')
    names.append(name)
    person_record['names'] = names

    dates = ev.add_dates_of_existence(row, 'dates_of_existence')
    if dates:
        person_record['dates_of_existence'] = dates

    notes = ev.add_agent_notes(row, 'notes')
    if notes:
        person_record['notes'] = notes
    # Create dictionary for item log.
    item_log = {}

    if post_record == 'True':
        # Create JSON record for person.
        person_record = json.dumps(person_record)
        print('JSON created for {}.'.format(sort_name))

        try:
            # Try to POST JSON to ArchivesSpace API people endpoint.
            post = requests.post(base_url+'/agents/people', headers=headers, data=person_record).json()
            print(json.dumps(post))
            uri = post['uri']
            print('Person successfully created with URI: {}'.format(uri))
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
            error = post['error']
            item_log = {'error': error, 'agent_name': sort_name}
            # Add item log to list of logs
            all_items.append(item_log)
            print('POST to AS failed.')
    else:
        # Create JSON records on your computer to review. Does not post to AS.
        dt = datetime.now().strftime('%Y-%m-%d')
        index += 1
        identifier = 'personalAgent_'+str(index).zfill(3)
        pa_filename = identifier+'_'+dt+'.json'
        directory = ''
        with open(directory+pa_filename, 'w') as fp:
            json.dump(person_record, fp)
        print('Agent record JSON successfully created with filename {}'.format(pa_filename))
        item_log = {'filename': pa_filename, 'sort_name': sort_name}
        all_items.append(item_log)
    print('')

# Convert all_items to DataFrame.
log = pd.DataFrame.from_records(all_items)

# Create CSV of all item logs.
dt = datetime.now().strftime('%Y-%m-%d%H.%M.%S')
person_csv = 'postNewPersonalAgentsLog_'+dt+'.csv'
log.to_csv(person_csv)
print('{} created.'.format(person_csv))

elapsed_time = time.time() - start_time
m, s = divmod(elapsed_time, 60)
h, m = divmod(m, 60)
print('Total script run time: ', '%d:%02d:%02d' % (h, m, s))
