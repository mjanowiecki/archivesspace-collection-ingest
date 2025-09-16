"""Joins two CSVs using pandas left merge on an identifier."""

import pandas as pd
import argparse
from datetime import datetime
import csv

# Create argparse inputs for terminal.
parser = argparse.ArgumentParser()
parser.add_argument('-f', '--file')
parser.add_argument('-f2', '--file2')
parser.add_argument('-c', '--column_name')
args = parser.parse_args()

if args.file:
    filename = args.file
else:
    filename = input('Enter template filename (including \'.csv\'): ')
if args.file2:
    filename2 = args.file2
else:
    filename2 = input('Enter filename of entities from ArchivesSpace (including \'.csv\'): ')
if args.column_name:
    column_name = args.column_name
else:
    column_name = input('Enter column to merge on: ')


template_entities = pd.read_csv(filename, header=0, dtype=str)
already_existing = pd.read_csv(filename2, header=0, dtype=str)


frame = pd.merge(template_entities, already_existing, how='left', on=[column_name], suffixes=('_1', '_2'), indicator=True)

frame = frame.reindex(sorted(frame.columns), axis=1)
print(frame.columns)
print(frame.head)
dt = datetime.now().strftime('%Y-%m-%d')
frame.to_csv(filename[:-4]+'-matches_'+dt+'.csv', index=False, quoting=csv.QUOTE_ALL)
