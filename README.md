# archivesspace-collection-ingest
 Workflow to ingest full collection via [ArchivesSpace REST API](https://archivesspace.github.io/archivesspace/api/#archivesspace-rest-api).


## Authentication

To run these scripts against your ArchivesSpace API, you must have two files in the directory with your scripts. These files are secret.py and secretProd.py. Each will have the following five variables, one for your test site and one for your production site.

baseURL = “someurl.com”
user = “username”
password = “password”
repository = “4”
verify = False

## Running scripts

## CSV templates
For your collection, create spreadsheets for the following types of entities according to our template guidelines.
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