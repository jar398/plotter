# 'Plotter' - make a graph

2021-02-17  work in progress, this doc might be obsolete

EOL related utilities.

Scripts are invoked using `rake`.

## Installation

Clone the repository.  Copy config/config.sample.yml to config/config.yml.
Modify paths in config.yml as needed:

* `locations: workspace: path:` should be a directory in which the
  scripts can do their work and (in some cases) leave their results.

* `locations: staging:` is used by scripts that modify the graphdb.
  For operations that do `LOAD CSV`, a directory is required that neo4j can read via HTTP,
  and there must be a way for the scripts to write to the same directory.  The
  `rsync_specifier` is used in an `rsync` command to transfer files from
  the local workspace to the HTTP area.  In order for `rsync` to work,
  appropriate `ssh` credentials hoave to be in place.  They can be specified
  in `~/.ssh/config` or with an -I argument in the `rsync_command`.

* `locations: staging: url:` is  the URL for the HTTP
  area, which is accessed by LOAD CSV commands.

Obtain a v3 API token 
([documentation here](https://github.com/EOL/eol_website/blob/master/doc/api.md))
and put it in a file.  The file location is
configured in `token_file:` (for a 'power user' account) or
`update_token_file:` (for an admin account that can make changes to the
graphdb).

## Choosing a configuration

Most `rake` commands require a `CONF=` parameter to specify which
graphdb configuration is to be used.  The configurations are listed in
`config.yml` but are typically `test` (for a private testing
instance), `beta` (EOL beta instance), or `prod` (EOL production
instance).

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
    dwca/
      NNN/  ... one directory per DwCA ... id = final 8 chars of uuid
        properties.json     ... metadata for this DwCA
        dwca.zip or dwca.tgz
        unpacked:
          meta.xml
          (all the other .tsv or .csv files)
    prod_pub/
      resources/
        ID/  ... one per publishing resource id
          paint/     - temporary directory for intermediate files
            assert.csv
            retract.csv
          (any other intermediate files: cached things, page id 
            map, reports, etc; none at present)
    prod_repo/
      resources/
        ID/  ... one per repository resource id
          page_id_map.csv
      publishing-resources.json  (optional, cached)
      repository-resources.json  (optional, cached)
    beta_pub/
    beta_repo/
      ... same structure ...
    export/     ... files are copied from here to the staging site ...
      prod_pub/
        resources/
          ID/
            inferences/
              inferences.csv
        resources.csv
      prod_repo/
        resources/
          ID/
      beta_pub/
      beta_repo/
      ptest/
        resources.csv

An 'instance' is a (publishing, repository) server pair.
TAG is either 'prod' or 'beta' (never 'test').

## Testing

Lots of things to test.  For end to end tests we need to look at:

* painter - branch painting
* traits_dumper - copy traits from graphdb to a set of files
* traits_loader - inverse of traits_dumper
* resource - copy vernaculars into graphdb
* cypher - run a single cypher query
* instance - flush caches (fallen into disrepair)
* hierarchy - dynamic hierarchy diff and patch

The 'concordance' feature is not currently working.
