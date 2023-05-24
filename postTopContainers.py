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

# Convert CSV with top container information into DataFrame.
df = pd.read_csv(filename)

logForAllItems = []
for index, row in df.iterrows():
    # Get top container information from CSV.
    barcode = row['barcode']
    indicator = row['indicator']
    type = row['type']
    print('Gathering top container #{}: {}.'.format(index, barcode))

    # Build JSON record for top container.
    containerRecord = {'jsonmodel_type': 'top_container', 'publish': True, 'restricted': False,
                       'repository': {'ref': '/repositories/3'}}
    containerRecord['barcode'] = str(barcode)
    containerRecord['indicator'] = str(indicator)
    containerRecord['type'] = str(type)

    locations = []
    location = {}
    ev.add_to_dict(row, location, 'ref', 'location_ref')
    ev.add_to_dict(row, location, 'status', 'status')
    ev.add_to_dict(row, location, 'start_date', 'start_date')
    ev.add_to_dict(row, location, 'end_date', 'end_date')
    ev.add_to_dict(row, location, 'note', 'note')
    if location:
        location['jsonmodel_type'] = 'container_location'
        locations.append(location)
        containerRecord['container_locations'] = locations

    ev.add_with_ref(row, containerRecord, 'collection', 'collection_ref', 'multiple')
    ev.add_with_ref(row, containerRecord, 'container_profile', 'container_profile', 'single')

    # Create dictionary for item log.
    itemLog = {}

    if postRecord == 'True':
        # Create JSON record for top container.
        containerRecord = json.dumps(containerRecord)
        print('JSON created for {}.'.format(barcode))

        try:
            # Try to POST JSON to ArchivesSpace API top container endpoint.
            post = requests.post(baseURL+'/repositories/'+repository+'/top_containers', headers=headers, data=containerRecord).json()
            print(json.dumps(post))
            uri = post['uri']
            print('Top container successfully created with URI: {}'.format(uri))
            itemLog = {'uri': uri, 'barcode': barcode, 'indicator': indicator}
            # Add item log to list of logs
            logForAllItems.append(itemLog)

        except requests.exceptions.JSONDecodeError:
            # If POST to ArchivesSpace fails, break loop.
            itemLog = {'uri': 'error', 'barcode': barcode}
            # Add item log to list of logs
            logForAllItems.append(itemLog)
            print('POST to AS failed, breaking loop.')
            break
    else:
        # Create JSON records on your computer to review. Does not post to AS.
        dt = datetime.now().strftime('%Y-%m-%d')
        index = str(index+1)
        identifier = 'topContainer_'+index.zfill(3)
        tc_filename = identifier+'_'+dt+'.json'
        directory = ''
        with open(directory+tc_filename, 'w') as fp:
            json.dump(containerRecord, fp)
        print('Top container JSON successfully created with filename {}'.format(tc_filename))
        itemLog = {'filename': tc_filename, 'barcode': barcode}
        logForAllItems.append(itemLog)
    print('')

# Convert logForAllItems to DataFrame.
log = pd.DataFrame.from_dict(logForAllItems)

# Create CSV of all item logs.
dt = datetime.now().strftime('%Y-%m-%d %H.%M.%S')
topContainerCSV = 'postNewTopContainers_'+dt+'.csv'
log.to_csv(topContainerCSV)
print('{} created.'.format(topContainerCSV))

elapsedTime = time.time() - startTime
m, s = divmod(elapsedTime, 60)
h, m = divmod(m, 60)
print('Total script run time: ', '%d:%02d:%02d' % (h, m, s))
