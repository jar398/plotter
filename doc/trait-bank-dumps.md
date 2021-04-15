# Dumping Traitbank

At present (June 2020) the script is driven entirely from the
neo4j graphdb.  This is based on the hypothesis that when people say
they want "all the traits", then all the information they need will be
present in the graphdb, not the MySQL database.  If they need other
tables (e.g. synonyms or vernaculars from the MySQL database), they
will need to get them in some other way.  There may be more work to do
here.

The traits dump script is invoked via `rake`.  Parameters are passed
via environment the `rake` `VAR=value` syntax.


* For a small dump (carnivores only) to test that the process works, try
  `rake traits:dump CONF=beta ID=7662`

* The dump of all trait records would be
  `rake traits:dump CONF=prod`

* Parameters:

 - `ZIP`: pathname of the .zip file to be written (should include
         the terminal '.zip').  Defaults to a location in the workspace.
 - `ID`: the page id of the taxon that is the root of the subtree to
        be dumped.  Default is to dump the entire traitbank.
 - `CHUNK`: number of records in each 'chunk' to be fetched.
            Default is 10000.
            Larger values can be more efficient over all, but
            can result in neo4j timeouts.
 - `TEMP`: where to put intermediate files (defaults to a directory under `/tmp`)

The script may fail due to neo4j and/or web server timeouts (about one
minute as of this writing).  In considering the risk of a timeout,
note that the run time of each chunked query depends on both the
number of result rows (`CHUNK`) and the time required by `SKIP` (which is
related to the number of trait nodes per predicate, and can be quite
large, e.g. for the `Present` predicate).  The time required for a
`SKIP` could be several minutes.

See the associated support module
[traits_dumper.rb](../lib/traits_dumper.rb) for
further documentation and to see how it's implemented.

## From the shell using HTTP and the API

The default zip file destination (`ZIP`) has a form similar to
`traitbank_TAG_YYYYMM.zip` where `TAG` in the pathname is
the page id (`ID`) or `all`.

    rake traits:dump CONF=beta ID=7674 CHUNK=50000

For the record, the following command completed successfully on
varela.csail.mit.edu on 29 May 2019:

    TEMP=/wd/tmp/all_201905 CHUNK=10000 TOKEN=`cat ~/a/eol/api.token` time \
      ruby -r ./lib/traits_dumper.rb -e TraitsDumper.main

which should be equivalent to the more modern form

    time rake traits:dump CONF=prod CHUNK=10000 TEMP=/wd/tmp/all_201905

`TEMP` is redirected in case the default `/tmp/...` is on a file
system lacking adequate space for this task.  `CHUNK` is set to 10000
because an earlier run with `CHUNK=20000` failed with a timeout.

## Via `rake` using neography

The default zip path (`ZIP`) is formed from the directory returned by
the `path` method of the `DataDownload` class, which I believe
corresponds to the web site URL with path `/data/downloads/`, and the
filename as described above, giving the clade (when specified) and
current month.

### `rake traits:dump`

Generates a ZIP file dump of the entire traitbank graphdb.

### `rake traits:smoke`

This is for testing only.  Same as `dump` but defaults `ID` to 7674
(Felidae) and `CHUNK` to 1000.

## Testing this module

Tests to do in sequence (easier to harder):

  1. Smoke test (Carnivora): \
         `rake traits:dump CONF=prod ID=7662`
     - check that the files in the .zip file are nonempty and seem 
       plausible, then delete the .zip file in the workspace
  2. Vertebrates:\
         `time rake traits:dump CONF=prod ID=2774383`
     - you can delete the .zip
  3. All life: - this takes a long time, maybe 8 hours -\
         `time rake traits:dump CONF=prod`
