# 'Plotter' - make a graph

2021-02-17  work in progress, this doc might be obsolete

EOL related utilities.

Scripts are invoked using `rake`.

## Installation

Clone the repository.  Copy config/config.sample.yml to config/config.yml
and modify as follows.

After configuration you can test using `rake`, e.g.

    rake resource:info CONF=prod ID=40

## Configuration

Copy `config/config.sample.yml` to `config/config.yml` and edit the
configuration directives as appropriate for your local `plotter`
installation.

 1. Set `locations: workspace: path:` to a local directory where the plotter scripts 
    can put results and intermediate files.  Personally I set this to
    directory `.plotter_workspace` in my home directory.
 1. Set `prod_pub: token_file:` to be the local file that contains (or will contain)
    a token for an EOL v3 API endpoint; similarly for `beta_pub`.  (see note, below)
     1. Obtain a read token for the graphdb, and if you'll be writing as well, also obtain 
        a write token.  The write token must be associated with an admin account; a read 
        token can be associated with any account but using a non-admin account is a 
        bit more secure.  If using an admin account the read and write tokens 
        will be the same.
     1. To obtain a token, go to
        `https://eol.org/services/authenticate` or
        `https://beta.eol.org/services/authenticate`
        as the case may be
        (see [API documentation](https://github.com/EOL/eol_website/blob/master/doc/api.md)).
     1. Put the token into a file.
     1. Set `prod_pub: token_file:` (or `beta_pub: token_file:` or `prod_pub: update_token_file:` 
        etc.) to be the path to that file.
 1. Configure the staging server `locations: staging:`.  The directory on the 
        staging server has to be writable via the `rsync`
        command, which is used by plotter scripts,
        and it has to be readable by the Neo4j server(s) via HTTP.
        The examples in `config.sample.yml` should provide guidance on how to set these variables.
     1. `rsync_command` specifies the `rsync`-like command to use to transfer local files
        to the staging server (this string does not include the source or target).  
        If not specified, defaults to `rsync -av`.
     1. `rsync_specifier` should be either a local path or `host:path` as would 
        be understood by an `ssh` command.  `host` is either a DNS name or 
        a name configured in `~/.ssh/config`.  If a local path, then neo4j should 
        be running locally and the path should be the 
        path to neo4j's `import` directory.  If remote, the directory should be 
        one that is exposed by that host's HTTP server.
     1. (It may be possible to use the neo4j `import` directory with `file:///` URLs, but 
        I haven't tried it.)
     1. `url` gives the prefix designating the `rsync_specifier`
        directory when accessed using 
        HTTP.  It will be used in the URLs that will occur in neo4j `LOAD CSV` 
        commands. 
     1. In order for `rsync` to work,
        appropriate `ssh` credentials have to be in place.  They can be specified
        in `~/.ssh/config` or with an -I argument in the `rsync_command`.



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

Workspace root comes from config.yml (via system.rb).  Default
is /home/jar/.plotter_workspace, which should be changed.

  (workspace root)/
    dwca/
      NNN/  ... one directory per DwCA ... id = final 8 chars of uuid
        properties.json     ... metadata for this DwCA
        dwca.zip or dwca.tgz
        unpacked:
          meta.xml
          (all the other .tsv or .csv files)
    prod/
      resources.csv
    prod_pub/
      resources/
        ID/  ... one per publishing resource id
          inferences/     - temporary directory for intermediate files
            inferences.csv
    prod_repo/
      resources/
        REPO_ID/  ... one per repository resource id
          page_id_map.csv
          pages/
            accepted.csv.chunks/
    beta/
    beta_repo/
      ... same structure as or production ...

An 'instance' is a triple (graphdb, publishing instance, repository instance).

## Testing

Lots of things to test.  For end to end tests we need to look at:

* painter - branch painting
* traits_dumper - copy traits from graphdb to a set of files
* traits_loader - inverse of traits_dumper
* resource - copy vernaculars into graphdb
* cypher - run a single cypher query
* hierarchy - dynamic hierarchy load, diff, and patch
* instance - flush caches (this has fallen into disrepair)

The 'concordance' feature is not currently working.
