import pandas as pd
import json
import argparse
import time
import extractvalues as ev
import requests
from datetime import datetime
import os
import secret

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

    auth = requests.post(base_url + '/users/' + user + '/login?password=' + password).json()
    session = auth['session']
    headers = {'X-ArchivesSpace-Session': session, 'Content_Type': 'application/json'}
else:
    pass

# Convert CSV with digital object information into DataFrame.
df = pd.read_csv(filename)

all_items = []
for index, row in df.iterrows():
    # Build JSON record to store data.
    digital_object_record = {'jsonmodel_type': 'digital_object',
                             'is_slug_auto': False}

    # For required fields, add directly to digital_object_record.
    title = row['title']
    digital_object_record['title'] = title
    digital_object_record['digital_object_id'] = row['digital_object_id']
    digital_object_record['publish'] = row['publish']
    digital_object_record['restrictions'] = row['restrictions']
    digital_object_record['suppressed'] = row['suppressed']
    digital_object_record['digital_object_type'] = row['digital_object_type']

    # For optional fields with 'ref' key, use function to add.
    ev.add_ref_value(row, digital_object_record, 'subjects', 'subjects', 'multi')
    ev.add_ref_value(row, digital_object_record, 'linked_events', 'linked_events', 'multi')

    # Add dates.
    dates = ev.add_dates(row, 'dates')
    if dates:
        digital_object_record['dates'] = dates

    # Add linked_agents.
    linked_agents = ev.add_linked_agents(row, 'linked_agents')
    if linked_agents:
        digital_object_record['linked_agents'] = linked_agents

    # Add notes.
    do_notes = ev.add_do_notes(row, 'notes')
    if do_notes:
        digital_object_record['notes'] = do_notes

    # Add language materials.
    lang_materials = ev.add_lang_materials(row, 'lang_materials')
    if lang_materials:
        digital_object_record['lang_materials'] = lang_materials

    # Add extents.
    extents = ev.add_extents(row, 'extents')
    if extents:
        digital_object_record['extents'] = extents

    # Add instances.
    instances = ev.add_instances(row, 'instances')
    if instances:
        digital_object_record['instances'] = instances

    # Add file versions.
    file_versions = ev.add_file_versions(row, 'file_versions')
    if file_versions:
        digital_object_record['file_versions'] = file_versions

    # Create dictionary for item log.
    item_log = {}

    if post_record == 'True':
        # Create JSON record for archival object.
        digital_object_record = json.dumps(digital_object_record)
        print('JSON created for {}.'.format(title))

        try:
            # Try to POST JSON to ArchivesSpace API digital object endpoint.
            post = requests.post(base_url+'/repositories/'+repository+'/digital_objects', headers=headers, data=digital_object_record).json()
            print(json.dumps(post))
            uri = post['uri']
            print('Digital object successfully created with URI: {}'.format(uri))
            item_log = {'uri': uri, 'title': title}
            # Add item log to list of logs
            all_items.append(item_log)

        except requests.exceptions.JSONDecodeError:
            # If POST to ArchivesSpace fails, break loop.
            item_log = {'uri': 'error', 'title': title}
            # Add item log to list of logs
            all_items.append(item_log)
            print('POST to AS failed, breaking loop.')

        except KeyError:
            # If JSON error occurs, record here.
            error = post['error']
            item_log = {'error': error, 'title': title}
            # Add item log to list of logs
            all_items.append(item_log)
            print('POST to AS failed.')

    else:
        # Create JSON records on your computer to review. Does not post to AS.
        dt = datetime.now().strftime('%Y-%m-%d')
        index = str(index + 1)
        identifier = 'digitalObject_' + index.zfill(3)
        do_filename = identifier + '_' + dt + '.json'
        directory = 'digital_objects/'
        os.makedirs(directory, exist_ok=True)
        with open(directory + do_filename, 'w') as fp:
            json.dump(digital_object_record, fp)
        print('Digital object record JSON successfully created with filename {}'.format(do_filename))
        item_log = {'filename': do_filename, 'title': title}
        all_items.append(item_log)
    print('')

# Convert all_items to DataFrame.
log = pd.DataFrame.from_records(all_items)

# Create CSV of all item logs.
dt = datetime.now().strftime('%Y-%m-%d%H.%M.%S')
digital_object_csv = 'postNewDigitalObjectsLog_'+dt+'.csv'
log.to_csv(digital_object_csv)
print('{} created.'.format(digital_object_csv))

elapsed_time = time.time() - start_time
m, s = divmod(elapsed_time, 60)
h, m = divmod(m, 60)
print('Total script run time: ', '%d:%02d:%02d' % (h, m, s))
