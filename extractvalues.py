import pandas as pd


df = pd.read_csv('enumerations.csv', index_col='Enumeration code')
df = df.drop(columns=['Enumeration', 'Value', 'Position', 'Read-only'])
ser = df['Value code']
boolean_values = ['True', 'False']

# dates (repeatable, separate by ||)
# example: label==creation;;date_type==single;;expression==2007 April 30;;begin==2007-04-30
label_values = list(ser.get('date_label'))
date_type_values = list(ser.get('date_type'))
certainty_values = list(ser.get('date_certainty'))
date_fields = {'begin': 'Not controlled',
               'date_type': date_type_values,
               'certainty': certainty_values,
               'end': 'Not controlled',
               'expression': 'Not controlled',
               'label': label_values}

# extents (repeatable, separate by ||)
# example: portion==whole;;extent_type==cubic_feet;;number==.167;;container_summary==1 legal size folder
portion_values = list(ser.get('extent_portion'))
extent_type_values = list(ser.get('extent_extent_type'))
extent_fields = {'container_summary': 'Not controlled',
                 'dimensions': 'Not controlled',
                 'extent_type': extent_type_values,
                 'number': 'Not controlled',
                 'physical_details': 'Not controlled',
                 'portion': portion_values}


# instances (repeatable, separate by ||)
# example: instance_type==mixed_materials;;is_representative==False;;indicator_2==2;;type_2==folder;;ref==/repositories/3/top_containers/16978
instance_type_values = list(ser.get('instance_instance_type'))
instance_fields = {'instance_type': instance_type_values,
                   'is_representative': boolean_values}
sub_container_fields = {'indicator_2': 'Not controlled',
                        'ref': 'Not controlled',
                        'type_2': instance_type_values}

# lang_materials (repeatable, separate by ||)
# example: language==fre;;publish==True;;script==Latn;;content==French
language_values = list(ser.get('language_iso639_2'))
script_values = list(ser.get('script_iso15924'))
lang_material_fields = {'content': 'Not controlled',
                        'language': language_values,
                        'label': 'Not controlled',
                        'publish': boolean_values,
                        'script': script_values}


# linked_agents (repeatable, separate by ||)
# example: role==creator;;relator==pht;;ref==/agents/corporate_entities/388
role_values = list(ser.get('linked_agent_role'))
relator_values = list(ser.get('linked_agent_archival_record_relators'))
linked_agents_fields = {'ref': 'Not controlled',
                        'relators': relator_values,
                        'role': role_values}

# note_multipart (repeatable, separate by ||)
# example: type==accessrestrict;;publish==True;;content==This digital content is available offline. Contact Special Collections for more information.
note_type_values = list(ser.get('note_multipart_type'))
note_fields = {'content': 'Not controlled',
               'publish': boolean_values,
               'type': note_type_values}

# note_singlepart (not repeatable)
# example: type==physdesc;;publish==True;;content==The handwritten letter in this collection is fragile and should be handled with care.
subnote_type_values = list(ser.get('note_singlepart_type'))
subnote_fields = {'content': 'Not controlled',
                  'publish': boolean_values,
                  'type': note_type_values}

# note_index

# note_bibliography

# rights_statements
rights_type_values = list(ser.get('rights_statement_rights_type'))
status_values = list(ser.get('rights_statement_ip_status'))
jurisdiction_values = list(ser.get('country_iso_3166'))
other_rights_basis_values = list(ser.get('rights_statement_other_rights_basis'))
right_statement_fields = {'rights_type': rights_type_values,
                          'identifier': 'Not controlled',
                          'status': status_values,
                          'determination_date': 'Not controlled',
                          'start_date': 'Not controlled',
                          'end_date': 'Not controlled',
                          'license_terms': 'Not controlled',
                          'statue_citation': 'Not controlled',
                          'jurisdiction': jurisdiction_values,
                          'other_rights_basis': other_rights_basis_values,
                          'external_documents': 'Not controlled',
                          'acts': 'Not controlled',
                          'linked_agents': 'Not controlled',
                          'notes': 'Not controlled'}

act_values = list(ser.get('rights_statement_act_type'))
restriction_values = list(ser.get('rights_statement_act_restriction'))
acts_fields = {'act_type': act_values,
               'restriction': restriction_values,
               'start_date': 'Not controlled',
               'end_date': 'Not controlled',
               'notes': 'Not controlled'}

note_rights_statement_values = list(ser.get('note_rights_statement_type'))
notes_rights_statement_fields = {'content': 'Not controlled',
                                 'type': note_rights_statement_values}


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


# This function splits up field components based on specified patterns.
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


def check_for_values(row_name, value_from_csv):
    try:
        value_from_csv = row_name[value_from_csv]
        list_of_values = split_pattern(value_from_csv)
        return list_of_values
    except KeyError:
        pass


def build_json(row_name, value_from_csv, jsonmodel_type, field_dictionary):
    list_of_values = check_for_values(row_name, value_from_csv)
    container_for_objects = []
    if list_of_values:
        for entry in list_of_values:
            json_object = {'jsonmodel_type': jsonmodel_type}
            for key, value in entry.items():
                validate_field_values(json_object, key, value, field_dictionary)
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
            for entry in list_of_values:
                subnote = {}
                for key, value in entry.items():
                    if key == 'type':
                        validate_field_values(note, key, value, note_fields)
                    else:
                        validate_field_values(subnote, key, value, note_fields)
                subnotes.append(subnote)
            note['subnotes'] = subnotes
            notes_multipart.append(note)
        if notes_multipart:
            return notes_multipart
    except KeyError:
        pass


def add_singlepart_note(row_name, value_from_csv):
    build_json(row_name, value_from_csv, 'note_singlepart', note_fields)


def add_extents(row_name, value_from_csv):
    build_json(row_name, value_from_csv, 'extent', extent_fields)


def add_dates(row_name, value_from_csv):
    build_json(row_name, value_from_csv, 'date', date_fields)


def add_linked_agents(row_name, value_from_csv):
    list_of_values = check_for_values(row_name, value_from_csv)
    if list_of_values:
        agents = []
        for entry in list_of_values:
            linked_agents = {}
            for key, value in entry.items():
                validate_field_values(linked_agents, key, value, linked_agents_fields)
            agents.append(linked_agents)
        if agents:
            return agents
    else:
        pass


def add_instances(row_name, value_from_csv):
    list_of_values = check_for_values(row_name, value_from_csv)
    if list_of_values:
        instances = []
        for entry in list_of_values:
            instance = {'jsonmodel_type': 'instance'}
            sub_container = {'jsonmodel_type': 'sub_container'}
            for key, value in entry.items():
                list_valid_fields = list(instance_fields.keys())
                if key in list_valid_fields:
                    validate_field_values(instance, key, value, instance_fields)
                elif key == 'ref':
                    sub_container['top_container'] = {'ref': value}
                else:
                    validate_field_values(sub_container, key, value, sub_container_fields)
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