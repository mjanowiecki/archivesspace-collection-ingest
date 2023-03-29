import pandas as pd

# note_multipart
# Repeatable, separate by ||
# Multipart note pattern: type==;;publish==;;content==
# Example: type==accessrestrict;;publish==True;;content==This digital content is available offline. Contact Special Collections for more information.

note_type_valid_values = ['abstract', 'accessrestrict', 'accruals', 'acqinfo', 'altformavail',
                          'appraisal', 'arrangement', 'bioghist', 'custodhist', 'materialsspec',
                          'odd', 'originalsloc', 'otherfindaid', 'physdesc', 'physfacet' 'phystech',
                          'prefercite', 'processinfo', 'relatedmaterial', 'scopecontent', 'separatedmaterial',
                          'userestrict']

publish_valid_values = ['True', 'False']

note_valid_fields = {'type': note_type_valid_values, 'publish': publish_valid_values, 'content': 'Not controlled'}

# note_singlepart
# Not repeatable
# Singlepart note pattern: type==;;publish==;;content==
# Example: type==physdesc;;publish==True;;content==The handwritten letter in this collection is fragile and should be handled with care.


subnote_type_valid_value = []

# dates
# Repeatable, separate by ||
# Date pattern: label==;;date_type==;;certainty==;;calendar==;;era==;;expression==;;begin==;;end==
# Example: label==creation;;date_type==single;;expression==2007 April 30;;begin==2007-04-30

label_valid_values = ['agent_relation', 'copyright', 'creation', 'deaccession', 'digitized', 'event', 'existence',
                      'modified', 'other', 'usage']

date_type_valid_values = ['inclusive', 'single', 'range', 'bulk']

certainty_valid_values = ['approximate', 'inferred']

date_valid_fields = {'label': label_valid_values, 'date_type': date_type_valid_values, 'certainty': certainty_valid_values, 'expression': 'Not controlled', 'begin': 'Not controlled', 'end': 'Not controlled'}

# extents
# Repeatable, separate by ||
# Extent pattern: portion==;;extent_type==;;number==;;container_summary==;;physical_details==;;dimensions==
# Example: portion==whole;;extent_type==cubic_feet;;number==.167;;container_summary==1 legal size folder

portion_valid_values = ['whole', 'part']

extent_type_valid_values = ['cubic_feet', 'gigabytes', 'photographic_prints', 'items', 'Folders', 'volumes',
                            'megabytes', 'terabytes', 'Boxes', 'Disks', 'Website(s)']

extent_valid_fields = {'portion': portion_valid_values, 'extent_type': extent_type_valid_values, 'number': 'Not controlled', 'container_summary': 'Not controlled'}

# linked_agents
# Repeatable, separate by ||
# Linked agents pattern: role==;;relator==;;ref==
# Example: role==creator;;relator==pht;;ref==/agents/corporate_entities/388
# Example: role==subject;;ref==/agents/corporate_entities/609

role_valid_values = ['creator', 'source', 'subject']

relator_valid_values = ['art', 'asn', 'auc', 'aut', 'bsl', 'cll', 'col;;', 'com', 'cre;;', 'crp', 'cur', 'dnr', 'dpt',
                        'edt', 'eng', 'fmo', 'ive', 'ivr', 'own', 'pbl', 'pht', 'prt', 'spn']

linked_agents_valid_fields = {'role': role_valid_values, 'relators': relator_valid_values, 'ref': 'Not controlled'}


# This function grabs a value from your spreadsheet and adds it to the JSON record you are building.
# row_name is the name of your row variable.
# dic_name is the name of the dictionary variable where your field (aka key) and value pair is being added.
# json_field is what you want to name the field (key) in the JSON file.
# value_from_csv is the name of the column in the CSV where the value comes from.
def add_to_dict(row_name, dict_name, json_field, value_from_csv):
    try:
        value_from_csv = row_name.get(value_from_csv)
        if pd.notna(value_from_csv):
            if isinstance(value_from_csv, float):
                value_from_csv = int(value_from_csv)
            elif isinstance(value_from_csv, str):
                value_from_csv = value_from_csv.strip()
            else:
                value_from_csv = value_from_csv
            dict_name[json_field] = value_from_csv
    except KeyError:
        pass


# This function grabs a value fom your spreadsheet and adds it as a {'ref': value_from_csv} pair to
# JSON file you are building.
# row_name is the name of your row variable.
# dic_name is the name of the dictionary variable where your {'ref': value_from_csv} pair is being added.
# json_field is the name of the field that contains the {'ref': value_from_csv} pair. For instance, 'subjects'
# will create 'subjects': {'ref': value_from_csv}.
# value_from_csv is the name of the column in the CSV where the value comes from.
# repeat determines whether the {'ref': value_from_csv} pair is contained in an array or not.
def add_with_ref(row_name, dict_name, json_field, value_from_csv, repeat):
    try:
        value_from_csv = row_name[value_from_csv]
        if pd.notna(value_from_csv):
            if repeat == 'single':
                value_from_csv = value_from_csv.strip()
                dict_name[json_field] = {'ref': value_from_csv}
            else:
                new_list = []
                value_from_csv = value_from_csv.split('|')
                for item in value_from_csv:
                    new_dict = {'ref': item}
                    new_list.append(new_dict)
                dict_name[json_field] = new_list
    except KeyError:
        pass


# This function validates certain values for specified JSON fields.
def validate_field_values(container, field_key, field_value, valid_field_dict):
    retrieved_field_value = valid_field_dict.get(field_key)
    if retrieved_field_value is None:
        print('{} field is not in {}.'.format(field_key, valid_field_dict))
    elif retrieved_field_value == 'Not controlled':
        container[field_key] = field_value
    else:
        if field_value in retrieved_field_value:
            container[field_key] = field_value
        else:
            print('{} field has bad {} field_value.'.format(field_key, field_value))


# This function splits up field components for dates, notes, and extent fields based on specified patterns.
def split_pattern(value_from_csv):
    list_of_values = []
    if pd.notna(value_from_csv):
        value_from_csv = value_from_csv.split('||')
        for single_value in value_from_csv:
            value_parts = single_value.split(';;')
            value_in_dict = {}
            for part in value_parts:
                field_and_value = part.split('==')
                field_name = field_and_value[0]
                field_value = field_and_value[1]
                value_in_dict[field_name] = field_value
            list_of_values.append(value_in_dict)              
    else:
        pass
    return list_of_values


def add_multipart_note(row_name, value_from_csv):
    try:
        value_from_csv = row_name[value_from_csv]
        list_of_values = split_pattern(value_from_csv)
        notes_multipart = []
        if list_of_values:
            note = {'jsonmodel_type': 'note_multipart', 'publish': True}
            subnotes = []
            for entry in list_of_values:
                subnote = {}
                for key, value in entry.items():
                    if key == 'type':
                        validate_field_values(note, key, value, note_valid_fields)
                    else:
                        validate_field_values(subnote, key, value, note_valid_fields)
                subnotes.append(subnote)
            note['subnotes'] = subnotes
            notes_multipart.append(note)
        if notes_multipart:
            return notes_multipart
    except KeyError:
        pass


def add_singlepart_note(row_name, value_from_csv):
    try:
        value_from_csv = row_name[value_from_csv]
        notes_singlepart = []
        list_of_values = split_pattern(value_from_csv)
        for entry in list_of_values:
            note = {'jsonmodel_type': 'note_singlepart', 'publish': True}
            for key, value in entry.items():
                validate_field_values(note, key, value, note_valid_fields)
            notes_singlepart.append(note)
        if notes_singlepart:
            return notes_singlepart
    except KeyError:
        pass


def add_extents(row_name, value_from_csv):
    try:
        value_from_csv = row_name[value_from_csv]
        list_of_values = split_pattern(value_from_csv)
        extents = []
        for entry in list_of_values:
            extent = {'jsonmodel_type': 'extent'}
            for key, value in entry.items():
                validate_field_values(extent, key, value, extent_valid_fields)
            extents.append(extent)
        if extents:
            return extents
    except KeyError:
        pass


def add_dates(row_name, value_from_csv):
    try:
        value_from_csv = row_name[value_from_csv]
        list_of_values = split_pattern(value_from_csv)
        dates = []
        for entry in list_of_values:
            date = {'jsonmodel_type': 'date'}
            for key, value in entry.items():
                validate_field_values(date, key, value, date_valid_fields)
            dates.append(date)
        if dates:
            return dates
    except KeyError:
        pass