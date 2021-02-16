# 'Plotter' - make a graph

EOL related utilities.

Scripts are invoked using `rake`.

## Installation

Clone the repository.  Copy config/config.sample.yml to config/config.yml.
Modify paths in config.yml as needed:

* `locations: workspace: path:` should be a directory in which the
  scripts can do their work and (in some cases) leave their results.

* `locations: staging:` is used by scripts that modify the graphdb.
  For `LOAD CSV`, a directory is required that neo4j can read via HTTP,
  and there must be a way for the scripts to write to the same directory.  The
  `scp_location` is used in an `rsync` command to transfer files from
  the local workspace to the HTTP area.  In order for `rsync` to work,
  appropriate `ssh` credentials have to be in place.

* `locations: staging: url:` is the URL for the HTTP
  area, which is accessed by LOAD CSV commands.

Obtain a v3 API token ([documentation here](https://github.com/EOL/eol_website/blob/master/doc/api.md)) and put it in a file.  The file location is
configured in `token_file:` (for a 'power user' account) or
`update_token_file:` (for an admin account that can make changes to the
graphdb).  Obviously it shouldn't say `/home/jar`.


## All-traits dump

See [doc/trait-bank-dumps.md](doc/trait-bank-dumps.md)

## Trait inference ("branch painting")

See [doc/branch-painting.md](doc/branch-painting.md)

## Dynamic hierarchy

There are scripts for adding ranks and vernacular names to the graphdb.

## Workspace structure

Workspace root comes from config.yml (via system.rb).  Currently set
to /home/jar/.plotter_workspace.

  (workspace root):
    dwca:
      NNN/  ... one directory per DwCA ... id = lower 8 chars of uuid ?
        properties.json
        dwca.zip or dwca.tgz
        unpacked:
          meta.xml
          (all the other tsv or csv files)
    prod:
      publishing-resources.json  (cached)
      repository-resources.json  (cached)
      resources:    ... one resource-in-repository
        TAG.PID.RID/  ... one per instance + publishing id + repository id
          paint:     - temporary directory for intermediate files
            assert.csv
            retract.csv
          (any other intermediate files: cached things, page id 
            map, reports, etc; none at present)
      staging:     (exactly parallels a directory tree on the staging server)
        TAG.PID.RID/  ... one per instance + publishing id + repository id
         - vernaculars/vernaculars.csv
         - inferences/inferences.csv
    beta:
      ... same structure ...
    (other instance):


An 'instance' is a (publishing, repository) server pair.
TAG is either 'prod' or 'beta' (never 'test').

## Testing

Lots of things to test.  For end to end tests we need to look at:

* hierarchy - dynamic hierarchy diff and patch
* painter - branch painting
* resource - copy vernaculars into graphdb
* traits_dumper - copy traits from graphdb to a set of files
* traits_loader - inverse of traits_dumper
* cypher - run a single cypher query
* instance - flush caches

The 'concordance' feature is not currently working.
