import pandas as pd
import ast

df = pd.read_csv('enumerations.csv', index_col='Enumeration code')
df = df.drop(columns=['Enumeration', 'Value', 'Position', 'Read-only'])
ser = df['Value code']

# Lists of controlled terms to validate fields
boolean_values = [True, False]

# Date-related lists
begin_standard_values = list(ser.get('begin_date_standardized_type'))
certainty_values = list(ser.get('date_certainty'))
end_standard_values = list(ser.get('end_date_standardized_type'))
extent_type_values = list(ser.get('extent_extent_type'))
date_label_values = list(ser.get('date_label'))
date_type_values = list(ser.get('date_type'))
date_role_values = list(ser.get('date_role'))
date_type_struct_values = list(ser.get('date_type_structured'))
date_standard_values = list(ser.get('date_standardized_type'))

# Extent-related lists
portion_values = list(ser.get('extent_portion'))
instance_type_values = list(ser.get('instance_instance_type'))

# Language-related lists
language_values = list(ser.get('language_iso639_2'))
script_values = list(ser.get('script_iso15924'))

# Note-related lists
note_type_values = list(ser.get('note_multipart_type'))
subnote_type_values = list(ser.get('note_singlepart_type'))
local_access_restrict_values = list(ser.get('restriction_type'))

# Linked agent related lists
role_values = list(ser.get('linked_agent_role'))
relator_values = list(ser.get('linked_agent_archival_record_relators'))

# Subject-related lists
subject_source_values = list(ser.get('subject_source'))
subject_type_values = list(ser.get('subject_term_type'))

# Top container related lists
container_type_values = list(ser.get('container_type'))
location_status_values = list(ser.get('container_location_status'))

# Name related lists
name_source_values = list(ser.get('name_source'))
name_rule_values = list(ser.get('name_rule'))
name_order_values = list(ser.get('name_person_name_order'))

# container_location (repeatable, separate by ||)
#
container_location_fields = {'status': location_status_values,
                             'start_date': 'Not controlled',
                             'end_date': 'Not controlled',
                             'note': 'Not controlled',
                             'ref': 'Not controlled'}

# dates (repeatable, separate by ||)
# example: label==creation;;date_type==single;;expression==2007 April 30;;begin==2007-04-30
date_fields = {'begin': 'Not controlled',
               'date_type': date_type_values,
               'certainty': certainty_values,
               'end': 'Not controlled',
               'expression': 'Not controlled',
               'label': date_label_values}

# dates_of_existence (repeatable, separate by ||)
dates_of_existence_fields = {'date_type_structured': date_type_struct_values,
                             'date_certainty': certainty_values}

date_range_fields = {'begin_date_expression': 'Not controlled',
                     'begin_date_standardized': 'Not controlled',
                     'begin_date_standardized_type': begin_standard_values,
                     'end_date_expression': 'Not controlled',
                     'end_date_standardized': 'Not controlled',
                     'end_date_standardized_type': end_standard_values}

date_single_fields = {'date_expression': 'Not controlled',
                      'date_standardized': 'Not controlled',
                      'date_role': date_role_values,
                      'date_standardization_type': date_standard_values}

dates_of_existence_keys = list(dates_of_existence_fields.keys())
date_range_keys = list(date_range_fields.keys())
date_single_keys = list(date_single_fields.keys())


# extents (repeatable, separate by ||)
# example: portion==whole;;extent_type==cubic_feet;;number==.167;;container_summary==1 legal size folder
extent_fields = {'container_summary': 'Not controlled',
                 'dimensions': 'Not controlled',
                 'extent_type': extent_type_values,
                 'number': 'Not controlled',
                 'physical_details': 'Not controlled',
                 'portion': portion_values}


# instances (repeatable, separate by ||)
# example: instance_type==mixed_materials;;is_representative==False;;indicator_2==2;;type_2==folder;;ref==/repositories/3/top_containers/16978
instance_fields = {'instance_type': instance_type_values,
                   'is_representative': boolean_values}
sub_container_fields = {'indicator_2': 'Not controlled',
                        'type_2': instance_type_values}
instance_keys = list(instance_fields.keys())
sub_container_keys = list(sub_container_fields.keys())

# lang_materials (repeatable, separate by ||)
# example: language==fre;;publish==True;;script==Latn;;content==French
lang_material_fields = {'content': 'Not controlled',
                        'language': language_values,
                        'label': 'Not controlled',
                        'publish': boolean_values,
                        'script': script_values}


# linked_agents (repeatable, separate by ||)
# example: role==creator;;relator==pht;;ref==/agents/corporate_entities/388
linked_agents_fields = {'ref': 'Not controlled',
                        'relators': relator_values,
                        'role': role_values}


# note_multipart (repeatable, separate by ||)
# example: type==accessrestrict;;publish==True;;content==Digital content is available offline.
note_fields = {'content': 'Not controlled',
               'publish': boolean_values,
               'type': note_type_values}
rights_restriction_fields = {'local_access_restriction_type': local_access_restrict_values,
                             'end': 'Not controlled'}
rights_keys = list(rights_restriction_fields.keys())

# note_singlepart (not repeatable)
# example: type==physdesc;;publish==True;;content==The handwritten letter is fragile and should be handled with care.
subnote_fields = {'content': 'Not controlled',
                  'publish': boolean_values,
                  'type': note_type_values}


# agent_notes (repeatable, separated by ||)
# example:
agent_notes_jsonmodel_types = ['bioghist', 'mandate', 'legal_status',
                               'structure_or_genealogy', 'general_context']
agent_note_fields = {'content': 'Not controlled',
                     'publish': boolean_values,
                     'type': agent_notes_jsonmodel_types}



# This function validates values from enumerations.csv controlled lists for specified JSON fields.
def validate_field_values(container, field_key, field_value, valid_field_dict):
    string_boolean = ['True', 'False', 'TRUE', 'FALSE']
    if field_value in string_boolean:
        field_value = field_value.title()
        field_value = ast.literal_eval(field_value)
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

# This function grabs a value from your spreadsheet and adds it to the JSON record you are building.
# row_name is the name of your row variable.
# dic_name is the name of the dictionary variable where your field (aka key) and value pair is being added.
# json_field is what you want to name the field (key) in the JSON file.
# value_from_csv is the name of the column in the CSV.
def add_single_string_value(row_name, dict_name, json_field, value_from_csv):
    try:
        value_from_csv = row_name[value_from_csv]
        # If value_from_csv is not blank, add to JSON.
        if pd.notna(value_from_csv):
            if isinstance(value_from_csv, str):
                value_from_csv = value_from_csv.strip()
            else:
                value_from_csv = str(value_from_csv)
                value_from_csv = value_from_csv
            dict_name[json_field] = value_from_csv
    except KeyError:
        print('{} field not found in CSV.'.format(value_from_csv))

# This function grabs a value from your spreadsheet and adds it to the JSON record you are building.
# row_name is the name of your row variable.
# dic_name is the name of the dictionary variable where your field (aka key) and value pair is being added.
# json_field is what you want to name the field (key) in the JSON file.
# value_from_csv is the name of the column in the CSV.
def add_integer_value(row_name, dict_name, json_field, value_from_csv):
    try:
        value_from_csv = row_name[value_from_csv]
        # If value_from_csv is not blank, add to JSON.
        if pd.notna(value_from_csv):
            if isinstance(value_from_csv, float):
                value_from_csv = int(value_from_csv)
            elif isinstance(value_from_csv, int):
                value_from_csv = value_from_csv
            else:
                value_from_csv = int(value_from_csv)
            dict_name[json_field] = value_from_csv
    except KeyError:
        print('{} field not found in CSV.'.format(value_from_csv))

# This function grabs a value from your spreadsheet and adds it to the JSON record you are building.
# row_name is the name of your row variable.
# dic_name is the name of the dictionary variable where your field (aka key) and value pair is being added.
# json_field is what you want to name the field (key) in the JSON file.
# value_from_csv is the name of the column in the CSV.
# controlled_list is the name of the controlled list dictionary you want to validate from.
def add_controlled_term(row_name, dict_name, json_field, value_from_csv, controlled_list):
    controlled_dictionary = {json_field: controlled_list}
    try:
        value_from_csv = row_name[value_from_csv]
        # If value_from_csv is not blank, add to JSON.
        if pd.notna(value_from_csv):
            if isinstance(value_from_csv, str):
                value_from_csv = value_from_csv.strip()
                validate_field_values(dict_name, json_field, value_from_csv, controlled_dictionary)
            else:
                pass
    except KeyError:
        print('{} field not found in CSV.'.format(value_from_csv))



# This function grabs a value fom your CSV and adds it as a {'ref': value_from_csv} pair to JSON file you are building.
# row_name is the name of your row variable.
# dic_name is the name of the dictionary variable where your {'ref': value_from_csv} pair is being added.
# json_field is the name of the field that contains the {'ref': value_from_csv} pair. For instance, 'subjects' will create 'subjects': {'ref': value_from_csv}.
# value_from_csv is the name of the column in the CSV where the value comes from.
# repeat determines whether the {'ref': value_from_csv} pair is contained in an array or not.
def add_ref_value(row_name, dict_name, json_field, value_from_csv, repeat):
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
        print('{} field not found in CSV.'.format(value_from_csv))


# This function splits up a list into subfields and values based on specified patterns.
# Example: role==creator;;ref==/agents/corporate_entities/388||role==source;;ref==/agents/corporate_entities/102 -->
# [{'role': 'creator', 'ref': '/agents/corporate_entities/388'}, {'role': 'source', 'ref': '/agents/corporate_entities/102'}]
def split_pattern(value_from_csv):
    list_of_values = []
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
    return list_of_values

# This function checks to see if a values exist in cell and if so, uses split_pattern function to divide into subfields and values.
def check_for_values(row_name, value_from_csv):
        value_from_csv = row_name.get(value_from_csv)
        if pd.notna(value_from_csv):
            value_from_csv = value_from_csv.strip()
            list_of_values = split_pattern(value_from_csv)
            return list_of_values
        else:
            pass


# This function uses check_for_values, split_pattern, and validate_field_values to build JSON for subfield type fields.
def build_subfields(row_name, value_from_csv, jsonmodel_type, field_dictionary):
    list_of_values = check_for_values(row_name, value_from_csv)
    container_for_objects = []
    if list_of_values:
        for entry in list_of_values:
            json_object = {}
            for key, value in entry.items():
                validate_field_values(json_object, key, value, field_dictionary)
            if jsonmodel_type is not None:
                json_object['jsonmodel_type'] = jsonmodel_type
            container_for_objects.append(json_object)
    if container_for_objects:
        return container_for_objects
    else:
        pass


def add_multipart_note(row_name, value_from_csv):
    try:
        value_from_csv = row_name[value_from_csv]
        list_of_values = split_pattern(value_from_csv)
        notes_multipart = []
        if list_of_values:
            note = {'jsonmodel_type': 'note_multipart', 'publish': True}
            subnotes = []
            rights_restriction = {}
            for entry in list_of_values:
                subnote = {'jsonmodel_type': 'note_text'}
                for key, value in entry.items():
                    if key == 'type':
                        validate_field_values(note, key, value, note_fields)
                    elif key in rights_keys:
                        validate_field_values(rights_restriction, key, value, rights_restriction_fields)
                    else:
                        validate_field_values(subnote, key, value, note_fields)
                subnotes.append(subnote)
            note['subnotes'] = subnotes
            if rights_restriction:
                value_1 = rights_restriction['local_access_restriction_type']
                rights_restriction['local_access_restriction_type'] = [value_1]
                note['rights_restriction'] = rights_restriction
            notes_multipart.append(note)
        if notes_multipart:
            return notes_multipart
    except KeyError:
        pass


def add_singlepart_note(row_name, value_from_csv):
    note_singlepart = build_subfields(row_name, value_from_csv, 'note_singlepart', note_fields)
    return note_singlepart


def add_extents(row_name, value_from_csv):
    extents = build_subfields(row_name, value_from_csv, 'extent', extent_fields)
    return extents


def add_dates(row_name, value_from_csv):
    dates = build_subfields(row_name, value_from_csv, 'date', date_fields)
    return dates


def add_dates_of_existence(row_name, value_from_csv):
    list_of_values = check_for_values(row_name, value_from_csv)
    dates_of_existence = []
    if list_of_values:
        date_of_existence = {'date_label': 'existence',
                             'jsonmodel_type': 'structured_date_label'}
        for entry in list_of_values:
                structured_date_range = {}
                structured_date_single = {}
                for key, value in entry.items():
                    if key in dates_of_existence_keys:
                        validate_field_values(date_of_existence, key, value, dates_of_existence_fields)
                    elif key in date_range_keys:
                        validate_field_values(structured_date_range, key, value, date_range_fields)
                    elif key in date_single_keys:
                        validate_field_values(structured_date_single, key, value, date_single_fields)
                    else:
                        print('Error!')
                if structured_date_range:
                    structured_date_range['jsonmodel_type'] = 'structured_date_range'
                    date_of_existence['structured_date_range'] = structured_date_range
                if structured_date_single:
                    structured_date_single['jsonmodel_type'] = 'structured_date_single'
                    date_of_existence['structured_date_single'] = structured_date_single
                dates_of_existence.append(date_of_existence)
    if dates_of_existence:
        return dates_of_existence
    else:
        pass



def add_linked_agents(row_name, value_from_csv):
    linked_agents = build_subfields(row_name, value_from_csv, None, date_fields)
    return linked_agents



def add_instances(row_name, value_from_csv):
    list_of_values = check_for_values(row_name, value_from_csv)
    if list_of_values:
        instances = []
        for entry in list_of_values:
            instance = {'jsonmodel_type': 'instance'}
            sub_container = {'jsonmodel_type': 'sub_container'}
            for key, value in entry.items():
                if key in instance_keys:
                    validate_field_values(instance, key, value, instance_fields)
                elif key == 'ref':
                    sub_container['top_container'] = {'ref': value}
                elif key in sub_container_keys:
                    validate_field_values(sub_container, key, value, sub_container_fields)
                else:
                    print('Error!')
            instance['sub_container'] = sub_container
            instances.append(instance)
        if instances:
            return instances
    else:
        pass


def add_lang_materials(row_name, value_from_csv):
    list_of_values = check_for_values(row_name, value_from_csv)
    if list_of_values:
        lang_materials = []
        for entry in list_of_values:
            lang_material = {'jsonmodel_type': 'lang_material'}
            for key, value in entry.items():
                if key == 'language':
                    validate_field_values(lang_material, key, value, lang_material_fields)
                else:
                    notes = []
                    note = {'jsonmodel_type': 'note_langmaterial'}
                    validate_field_values(note, key, value, lang_material_fields)
            if note:
                notes.append(note)
                lang_material['notes'] = notes
            lang_materials.append(lang_material)
        if lang_materials:
            return lang_materials
    else:
        pass

def add_agent_notes(row_name, value_from_csv):
    list_of_values = check_for_values(row_name, value_from_csv)
    if list_of_values:
        agent_notes = []
        for entry in list_of_values:
            agent_note = {}
            subnote = {'jsonmodel_type': 'note_text',
                       'publish': True}
            for key, value in entry.items():
                if key != 'content':
                    validate_field_values(agent_note, key, value, agent_note_fields)
                else:
                    validate_field_values(subnote, key, value, agent_note_fields)
            note_type = agent_note['type']
            agent_note['jsonmodel_type'] = 'note_'+note_type
            del agent_note['type']
            agent_note['subnotes'] = [subnote]
            agent_notes.append(agent_note)
        if agent_notes:
            return agent_notes
        else:
            pass


def add_locations(row_name, value_from_csv):
    container_locations = build_subfields(row_name, value_from_csv, 'container_location', container_location_fields)
    return container_locations
