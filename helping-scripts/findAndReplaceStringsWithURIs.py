import csv

import pandas as pd
import argparse
from datetime import datetime
import os
import csv

parser = argparse.ArgumentParser()
parser.add_argument('-f', '--file')
parser.add_argument('-d', '--directory')
args = parser.parse_args()

if args.file:
    filename = args.file
else:
    filename = input('Enter metadata filename (including \'.csv\'): ')
if args.directory:
    directory = args.directory
else:
    directory = input('Enter directory containing log csvs: ')


metadata = pd.read_csv(filename, dtype=str)

dataframes = []
for count, file in enumerate(os.listdir(directory)):
    file = directory + "/" + file
    print(file)
    if file.endswith('.csv'):
        new_df = pd.read_csv(file, dtype=str)
        dataframes.append(new_df)

find_replace = pd.concat(dataframes)

for index, row in find_replace.iterrows():
    old_term = row['agent_name']
    new_term = row['uri']
    if pd.notnull(old_term):
        metadata['linked_agents'] = metadata['linked_agents'].str.replace(old_term, new_term, regex=False)

for index, row in find_replace.iterrows():
    old_term = row['subject_name']
    new_term = row['uri']
    if pd.notnull(old_term):
        metadata['subjects'] = metadata['subjects'].str.replace(old_term, new_term, regex=False)

for index, row in find_replace.iterrows():
    old_term = row.get('barcode')
    new_term = row.get('uri')
    if pd.notnull(old_term):
        metadata['instances'] = metadata['instances'].str.replace(old_term, new_term, regex=False)


# Create CSV for new DataFrame.
filename = filename[:-4]
dt = datetime.now().strftime('%Y-%m-%d%H.%M.%S')
metadata.to_csv('wReplacements_'+filename+'_'+dt+'.csv', index=False, quoting=csv.QUOTE_ALL)
