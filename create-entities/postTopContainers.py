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
    headers = {'X-ArchivesSpace-Session': session}
else:
    pass

# Convert CSV with top container information into DataFrame.
df = pd.read_csv(filename)

all_items = []
for index, row in df.iterrows():
    # Get top container information from CSV.
    barcode = row['barcode']
    indicator = row['indicator']
    container_type = row['type']
    print('Gathering top container #{}: {}.'.format(index, barcode))

    # Build JSON record for top container.
    container_record = {'jsonmodel_type': 'top_container', 'publish': True, 'restricted': False,
                       'repository': {'ref': '/repositories/3'}, 'barcode': str(barcode), 'indicator': str(indicator),
                       'type': str(container_type)}

    ev.add_ref_value(row, container_record, 'container_profile', 'container_profile', 'single')
    container_locations = ev.add_locations(row, 'container_location' )
    if container_locations:
        container_record['container_locations'] = container_locations
    # Create dictionary for item log.
    item_log = {}

    if post_record == 'True':
        try:
            # Try to POST JSON to ArchivesSpace API top container endpoint.
            post_response = requests.post(base_url+'/repositories/'+repository+'/top_containers', headers=headers, json=container_record).json()
            print(json.dumps(post_response))
            uri = post_response['uri']
            print('Top container successfully created with URI: {}'.format(uri))
            item_log = {'uri': uri, 'barcode': barcode, 'indicator': indicator}
            # Add item log to list of logs
            all_items.append(item_log)

        except requests.exceptions.JSONDecodeError:
            # If POST to ArchivesSpace fails, break loop.
            item_log = {'uri': 'error', 'barcode': barcode}
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
        identifier = 'topContainer_'+str(index).zfill(3)
        tc_filename = identifier+'_'+dt+'.json'
        directory = ''
        with open(directory+tc_filename, 'w') as fp:
            json.dump(container_record, fp)
        print('Top container JSON successfully created with filename {}'.format(tc_filename))
        item_log = {'filename': tc_filename, 'barcode': barcode}
        all_items.append(item_log)
    print('')

# Convert all_items to DataFrame.
log = pd.DataFrame.from_records(all_items)

# Create CSV of all item logs.
dt = datetime.now().strftime('%Y-%m-%d%H.%M.%S')
top_container_csv = 'postNewTopContainersLog_'+dt+'.csv'
log.to_csv(top_container_csv, index=False)
print('{} created.'.format(top_container_csv))

elapsed_time = time.time() - start_time
m, s = divmod(elapsed_time, 60)
h, m = divmod(m, 60)
print('Total script run time: ', '%d:%02d:%02d' % (h, m, s))
