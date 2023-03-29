import pandas as pd
import json
import argparse
import time
import extractvalues as ev
import requests
from datetime import datetime

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

# Convert CSV with archival object information into DataFrame.
df = pd.read_csv(filename)

logForAllItems = []
for index, row in df.iterrows():
    # Create dictionary to store data.
    archivalObjectRecord = {'jsonmodel_type': 'archival_object', 'suppressed': False}

    # For required fields, add directly to archivalObjectRecord.
    title = row['title']
    archivalObjectRecord['title'] = title
    archivalObjectRecord['resource'] = {'ref': row['resource']}
    archivalObjectRecord['level'] = row['level']
    archivalObjectRecord['publish'] = row['publish']
    archivalObjectRecord['restrictions_apply'] = row['restrictions_apply']

    # For optional fields, try to find value and add to archivalObjectRecord if found.
    ev.add_to_dict(row, archivalObjectRecord, 'repository_processing_note', 'repository_processing_note')
    ev.add_to_dict(row, archivalObjectRecord, 'position', 'position')
    ev.add_to_dict(row, archivalObjectRecord, 'other_level', 'other_level')

    # For optional fields with 'ref' key, use function to add.
    ev.add_with_ref(row, archivalObjectRecord, 'parent', 'parent', 'single')
    ev.add_with_ref(row, archivalObjectRecord, 'repository', 'repository', 'single')
    ev.add_with_ref(row, archivalObjectRecord, 'linked_events', 'linked_events', 'multi')
    ev.add_with_ref(row, archivalObjectRecord, 'subjects', 'subjects', 'multi')

    # Add notes.
    notes_multipart = ev.add_multipart_note(row, 'multipart_note')
    notes_singlepart = ev.add_singlepart_note(row, 'singlepart_note')
    notes = notes_multipart + notes_singlepart
    if notes:
        archivalObjectRecord['notes'] = notes

    # Add extent.
    extents = ev.add_extents(row, 'extents')
    if extents:
        archivalObjectRecord['extents'] = extents

    # Create dictionary for item log.
    itemLog = {}

    if postRecord == 'True':
        # Create JSON record for archival object.
        archivalObjectRecord = json.dumps(archivalObjectRecord)
        print('JSON created for {}.'.format(title))

        try:
            # Try to POST JSON to ArchivesSpace API archival object endpoint.
            post = requests.post(baseURL+'/agents/people', headers=headers, data=archivalObjectRecord).json()
            print(json.dumps(post))
            uri = post['uri']
            print('Archival object successfully created with URI: {}'.format(uri))
            itemLog = {'uri': uri, 'title': title}
            # Add item log to list of logs
            logForAllItems.append(itemLog)

        except requests.exceptions.JSONDecodeError:
            # If POST to ArchivesSpace fails, break loop.
            itemLog = {'uri': 'error', 'title': title}
            # Add item log to list of logs
            logForAllItems.append(itemLog)
            print('POST to AS failed, breaking loop.')
            break
    else:
        # Create JSON records on your computer to review. Does not post to AS.
        dt = datetime.now().strftime('%Y-%m-%d')
        index = str(index+1)
        identifier = 'archivalObject_'+index.zfill(3)
        ao_filename = identifier+'_'+dt+'.json'
        directory = ''
        with open(directory + ao_filename,  'w') as fp:
            json.dump(archivalObjectRecord, fp)
        print('Archival object record JSON successfully created with filename {}'.format(ao_filename))
        itemLog = {'filename': ao_filename, 'title': title}
        logForAllItems.append(itemLog)
    print('')

# Convert logForAllItems to DataFrame.
log = pd.DataFrame.from_dict(logForAllItems)

# Create CSV of all item logs.
dt = datetime.now().strftime('%Y-%m-%d %H.%M.%S')
archivalObjectCSV = 'postNewArchivalObjects_'+dt+'.csv'
log.to_csv(archivalObjectCSV)
print('{} created.'.format(archivalObjectCSV))

elapsedTime = time.time() - startTime
m, s = divmod(elapsedTime, 60)
h, m = divmod(m, 60)
print('Total script run time: ', '%d:%02d:%02d' % (h, m, s))