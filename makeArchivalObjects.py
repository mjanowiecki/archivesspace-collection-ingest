import pandas as pd
import json
import argparse
from datetime import datetime
# import prefixLists

parser = argparse.ArgumentParser()
parser.add_argument('-f', '--file')
args = parser.parse_args()

if args.file:
    filename = args.file
else:
    filename = input('Enter filename (including \'.csv\'): ')
filename = 'exampleSheets_archivalObjects.csv'


def add_to_dict(dict_name, key, value):
    try:
        value = row.get(value)
        if pd.notna(value):
            if isinstance(value, float):
                value = int(value)
            else:
                value = value.strip()
            dict_name[key] = value
    except KeyError:
        pass


def add_with_ref(dict_name, key, value, repeat):
    try:
        value = row[value]
        if pd.notna(value):
            if repeat == 'single':
                value = value.strip()
                dict_name[key] = {'ref': value}
            else:
                new_list = []
                value = value.split('|')
                for item in value:
                    new_dict = {'ref': item}
                    new_list.append(new_dict)
                dict_name[key] = new_list
    except KeyError:
        pass


def add_notes(dict_name, value):
    try:
        value = row[value]
        if pd.notna(value):
            notes = []
            notes_values = value.split('||')
            for note_value in notes_values:
                note_parts = note_value.split(';;')
                first_model_type = note_parts[0]
                note_type = note_parts[1]
                second_model_type = note_parts[2]
                content = note_parts[3]
                note = {}
                note['jsonmodel_type'] = first_model_type
                note['publish'] = True
                note['subnotes'] = [{'content': content, 'jsonmodel_type': second_model_type, 'publish': True}]
                note['type'] = note_type
                notes.append(note)
            dict_name['notes'] = notes
    except KeyError:
        pass


def add_extents(dict_name, value):
    try:
        value = row[value]
        if pd.notna(value):
            extent = []
            extent_values = value.split('|')
            extent_dict = {}
            for extent_value in extent_values:
                extent_value = extent_value.split(';;')
                k = extent_value[0]
                v = extent_value[1]
                extent_dict[k] = v
            extent.append(extent_dict)
            dict_name['extent'] = extent
    except KeyError:
        pass


df = pd.read_csv(filename)

for index, row in df.iterrows():
    # Create empty dictionary to store data.
    json_file = {}
    json_file['jsonmodel_type'] = 'archival_object'
    json_file['suppressed'] = False

    # For required fields, add directly to json_file.
    identifier = row['local_id']
    json_file['title'] = row['title']
    json_file['resource'] = {'ref': row['resource']}
    json_file['level'] = row['level']
    json_file['publish'] = row['publish']
    json_file['restrictions_apply'] = row['restrictions_apply']

    # For optional fields, try to find value and add to json_file if found.
    add_to_dict(json_file, 'repository_processing_note', 'repository_processing_note')
    add_to_dict(json_file, 'position', 'position')
    add_to_dict(json_file, 'other_level', 'other_level')

    # For optional fields with 'ref' key, use function to add.
    add_with_ref(json_file, 'parent', 'parent', 'single')
    add_with_ref(json_file, 'repository', 'repository', 'single')
    add_with_ref(json_file, 'linked_events', 'linked_events', 'multi')
    add_with_ref(json_file, 'subjects', 'subjects', 'multi')

    # Add notes.
    add_notes(json_file, 'notes')

    # Add extent.
    add_extents(json_file, 'extents')
    print(json_file)

    dt = datetime.now().strftime('%Y-%m-%d')
    ao_filename = identifier + '_' + dt + '.json'
    directory = ''
    with open(directory + ao_filename, 'w') as fp:
        json.dump(json_file, fp)