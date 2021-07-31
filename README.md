# 'Plotter' - make a graph

A collection of utilities for reading and modifying EOL graph databases.

## Installation

Clone the repository.

## Configuration

Copy `config/config2.sample.yml` to `config/config2.yml`
and modify as follows to make it
appropriate for your local `plotter` installation.

 1. Configure neo4j URL, user name, password, and graphdb name
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
        The examples in `config2.sample.yml` should provide guidance on how 
        to set these variables.
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


## Choosing EOL servers

Most of the `rake` commands require a `CONF=` parameter to specify
which publishing and/or graphdb server is to be contacted (and, in
turn, which EOL repository (content) server is to be consulted).  The
graphdb choices are listed in `config2.yml` but are typically `test`
(for a private testing instance), `beta` (EOL beta instance), or
`prod` (EOL production instance).

## 'Smoke test'

As a simple first test of installation, try

    rake resource:info CONF=beta ID=40

which will display a bunch of information about resource 40 on the
beta publishing instance.

## All-traits dump

Plotter can generate a zip file with a dump of all trait records.
See [doc/trait-bank-dumps.md](doc/trait-bank-dumps.md).

## Trait inference ("branch painting")

Plotter can do "branch painting" or inference of traits through the taxonomic hierarchy.
See [doc/branch-painting.md](doc/branch-painting.md).

## Resource metadata

There is a script for copying resource metadata into the graphdb.  For example:

    rake traits:sync_resource_metadata CONF=prod

ensures that there is a neo4j `Resource` node for every resource known
to the production publishing server, and sets the `resource_id`,
`name`, `description`, and `repository_id` property of each.

Normally it won't be necessary to invoke this script since this
information is also transferred on a 'publish'.

## Dynamic hierarchy

There are scripts for adding ranks (see `rake --tasks hierarchy:sync_metadata`)
and vernacular names (see `rake --tasks vernaculars`)
to the graphdb.  It is also possible load a dynamic hierarchy into a test instance
(see `rake --tasks hierarchy`).

## Deltas

There are some general scripts for manipulating CSV files, and in
particular for comparing them ('delta' or 'diff').  See [doc/csv.md](doc/csv.md).

## Retrieve DwCA for a resource

See `rake --tasks resource`.

## Workspace structure

Workspace root comes from config2.yml (via system.rb).  Default
is /home/jar/.plotter_workspace, which should be changed.

Sometimes the choice of location is a bit arbitrary or even wrong.
In general artifacts that depend only on the repository goes in
`xxx_repo` (for xxx = prod, beta, test); if it depends on the
additionally on the publishing relational database it goes in
`xxx_pub`; and if it depends additionally on the graphdb it goes in
`xxx`.

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

## Upgrading from plotter 0.1.0 to plotter 0.2.0

* Make sure you have a version of the `bundle` command with version >= 2.2.10.
  I did `gem install bundler` followed by `bundle` to accomplish this.
* Make a new configuration file with `cp config/config2.sample.yml config/config2.yml`.
* Add details to `config/config2.yml` based on what you did for your previous config file
  `config/config.yml`.  The structure and syntax are slightly different but 
  overall the details are mostly the same.  I hope that this step is self-evident.
* Traits dumps now go in workspace subdirectory `prod/trait_dumps` (for production).
  Modify scripts for this new location as necessary.
* Traits dump temp files will now go in workspace subdirectory `prod/trait_dumps/tmp/`
* Be sure to include `R` among the rsync flags (see `rsync_command:`)
