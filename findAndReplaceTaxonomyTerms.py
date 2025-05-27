import pandas as pd
import argparse
from datetime import datetime
import os

parser = argparse.ArgumentParser()
parser.add_argument('-f', '--file')
parser.add_argument('-d', '--directory')
args = parser.parse_args()

if args.file:
    filename = args.file
else:
    filename = input('Enter metadata filename (including \'.csv\'): ')
if args.file2:
    directory = args.directory
else:
    directory = input('Enter directory containing log csvs: ')


metadata = pd.read_csv(filename)

dataframes = []
for count, filename in enumerate(os.listdir(directory)):
    filename = directory + "/" + filename
    print(filename)
    if filename.endswith('.csv'):
        newDF = pd.read_csv(filename)
        dataframes.append(newDF)

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


# Create CSV for new DataFrame.
filename = filename[:-4]
dt = datetime.now().strftime('%Y-%m-%d%H.%M.%S')
metadata.to_csv('wReplacements_'+filename+'_'+dt+'.csv')