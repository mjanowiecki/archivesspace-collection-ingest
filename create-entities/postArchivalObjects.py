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
    headers = {'X-ArchivesSpace-Session': session, 'Content-Type': 'application/json'}
else:
    pass

# Convert CSV with archival object information into DataFrame.
df = pd.read_csv(filename)

all_items = []
for index, row in df.iterrows():
    # Build JSON record to store data.
    archival_object_record = {'jsonmodel_type': 'archival_object'}

    # For required fields, add directly to archival_object_record.
    title = row['title']
    archival_object_record['title'] = title
    archival_object_record['resource'] = {'ref': row['resource']}
    archival_object_record['level'] = row['level']
    archival_object_record['publish'] = row['publish']
    archival_object_record['suppressed'] = row['suppressed']
    archival_object_record['restrictions_apply'] = row['restrictions_apply']

    # For optional fields, try to find value and add to archival_object_record if found.
    ev.add_single_string_value(row, archival_object_record, 'repository_processing_note', 'repository_processing_note')
    ev.add_single_string_value(row, archival_object_record, 'position', 'position')
    ev.add_single_string_value(row, archival_object_record, 'component_id', 'component_id')

    # For optional fields with 'ref' key, use function to add.
    ev.add_ref_value(row, archival_object_record, 'parent', 'parent', 'single')
    ev.add_ref_value(row, archival_object_record, 'repository', 'repository', 'single')
    ev.add_ref_value(row, archival_object_record, 'series', 'series', 'single')
    ev.add_ref_value(row, archival_object_record, 'accession_links', 'accession_links', 'multi')
    ev.add_ref_value(row, archival_object_record, 'subjects', 'subjects', 'multi')
    ev.add_ref_value(row, archival_object_record, 'linked_events', 'linked_events', 'multi')

    # Add dates.
    dates = ev.add_dates(row, 'dates')
    if dates:
        archival_object_record['dates'] = dates

    # Add linked_agents.
    linked_agents = ev.add_linked_agents(row, 'linked_agents')
    if linked_agents:
        archival_object_record['linked_agents'] = linked_agents

    # Add notes.
    notes_multipart = ev.add_multipart_note(row, 'multipart_note')
    notes_singlepart = ev.add_singlepart_note(row, 'singlepart_note')
    notes = []
    if notes_singlepart:
        for single_note in notes_singlepart:
            notes.append(single_note)
    if notes_multipart:
        for multipart_note in notes_multipart:
            notes.append(multipart_note)
    if notes:
        archival_object_record['notes'] = notes

    # Add language materials.
    lang_materials = ev.add_lang_materials(row, 'lang_materials')
    if lang_materials:
        archival_object_record['lang_materials'] = lang_materials

    # Add extents.
    extents = ev.add_extents(row, 'extents')
    if extents:
        archival_object_record['extents'] = extents

    # Add instances.
    instances = ev.add_instances(row, 'instances')
    if instances:
        archival_object_record['instances'] = instances

    # Create dictionary for item log.
    item_log = {}

    if post_record == 'True':
        # Create JSON record for archival object.
        archival_object_record = json.dumps(archival_object_record)
        print('JSON created for {}.'.format(title))

        try:
            # Try to POST JSON to ArchivesSpace API archival object endpoint.
            post = requests.post(base_url+'/repositories/'+repository+'/archival_objects', headers=headers, data=archival_object_record).json()
            print(json.dumps(post))
            uri = post['uri']
            print('Archival object successfully created with URI: {}'.format(uri))
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
        identifier = 'archivalObject_' + index.zfill(3)
        ao_filename = identifier + '_' + dt + '.json'
        directory = 'archival_objects/'
        os.makedirs(directory, exist_ok=True)
        with open(directory + ao_filename, 'w') as fp:
            json.dump(archival_object_record, fp)
        print('Archival object record JSON successfully created with filename {}'.format(ao_filename))
        item_log = {'filename': ao_filename, 'title': title}
        all_items.append(item_log)
    print('')

# Convert all_items to DataFrame.
log = pd.DataFrame.from_records(all_items)

# Create CSV of all item logs.
dt = datetime.now().strftime('%Y-%m-%d%H.%M.%S')
archival_object_csv = 'postNewArchivalObjectsLog_'+dt+'.csv'
log.to_csv(archival_object_csv)
print('{} created.'.format(archival_object_csv))

elapsed_time = time.time() - start_time
m, s = divmod(elapsed_time, 60)
h, m = divmod(m, 60)
print('Total script run time: ', '%d:%02d:%02d' % (h, m, s))
