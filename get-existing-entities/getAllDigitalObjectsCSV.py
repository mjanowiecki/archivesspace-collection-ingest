import requests
import secret
import pandas as pd
import argparse
from datetime import datetime

secretVersion = input('To edit production server, enter the name of the secret file: ')
if secretVersion != '':
    try:
        secret = __import__(secretVersion)
        print('Editing Production')
    except ImportError:
        print('Editing Development')
else:
    print('Editing Development')

parser = argparse.ArgumentParser()
parser.add_argument('-f', '--file', help='filename to retrieve')
args = parser.parse_args()

if args.file:
    filename = args.file
else:
    filename = input('Enter filename as filename.csv: ')

base_url = secret.base_url
user = secret.user
password = secret.password

df = pd.read_csv(filename)
itemList = df.digital.to_list()


def collect_property(dictionary, do_property, name=None):
    if dictionary is not None:
        value = dictionary.get(do_property)
        if value is not None:
            if name:
                digital_dict[name] = value
            else:
                digital_dict[do_property] = value


auth = requests.post(base_url + '/users/' + user + '/login?password=' + password).json()
session = auth['session']
print(auth)
print(session)
headers = {'X-ArchivesSpace-Session': session, 'Content_Type': 'application/json'}

all_items = []
for count, item in enumerate(itemList):
    digital_dict = {}
    print(count)
    print(base_url + item)
    output = requests.get(base_url + item, headers=headers).json()
    collect_property(output, 'uri')
    collect_property(output, 'digital_object_id')
    files = output.get('file_versions')
    for file in files:
        collect_property(file, 'file_uri')
    all_items.append(digital_dict)

# Convert all_items to CSV and save.
df = pd.DataFrame.from_records(all_items)
print(df.head)
dt = datetime.now().strftime('%Y-%m-%d%H.%M.%S')
df.to_csv('allDigitalObjects_'+dt+'.csv', index=False)
