# archivesspace-collection-ingest
 Workflow to ingest full collection via [ArchivesSpace REST API](https://archivesspace.github.io/archivesspace/api/#archivesspace-rest-api).


## Authentication

To run these scripts against your ArchivesSpace API, you must have two files in the directory with your scripts. These files are `secret.py` and `secretProd.py`. Each will have the following five variables, one for your test/development site and one for your production site.

```python
baseURL = “yourarchivesspace.com”
user = “username”
password = “password”
repository = “4”
verify = False
```

## Running scripts

These scripts, one for each type of ArchivesSpace entity, take data from a completed CSV and create valid JSON records. Then, the script will either `POST` these JSON records to the relevant ArchivesSpace endpoint or save the JSON records to your local filesystem. We use the `argparse` module to enter our decision in the terminal. We also use the `argparse` module to tell the script what our CSV file is named. The CSV file in this example is located in the same folder as our script. So, if we would like to `POST` our JSON records to ArchivesSpace and our CSV is called `collection-10-people.csv`, the terminal command would look something like this:

```
python3 postPeopleAgents.py -file collection-10-people.csv -postRecord True
```

Alternatively, if we don't want to post the JSON records to ArchivesSpace, our input in the terminal might look like this:

```
python3 postPeopleAgents.py -f collection-10-people.csv -p False
```

If you enter `True` to `-postRecord`, the terminal will then prompt you to enter the secret filename for the Production instance of ArchivesSpace. If you want to POST to the development or test instance of your ArchivesSpace site instead, just hit enter without typing anything. Otherwise, type `secretProd` and hit enter.

## CSV templates
For your collection, create spreadsheets for the following types of entities according to our template guidelines. These templates assume that you have already created the main resource for the collection manually.
    1. Person
    2. Corporate Entity
    3. Family
    4. Archival Object
    5. Top Container

## Steps to add new collection

1. Determine what agents and subjects in your collection already exist in ArchivesSpace. Run a script to gather their URIs.
2. For agents and subjects that do not yet exist, create new agents (people, corporate entities, families) and subjects.
3. Create (manually) the collection resource in ArchivesSpace.
4. Replace strings of agent and subject names with URIs in archival object spreadsheet.
5. 

## Testing scripts