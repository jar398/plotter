# Branch painting

TL;DR: Use `rake paint:paint CONFIG=x ID=y` where x is `beta` or
`prod` and y is the id of the resource according to the particular publishing
database you want to access.  E.g.

    rake paint:paint CONFIG=beta ID=640

to paint resource 640 in the the beta neo4j instance.

'Branch painting' is the process of adding trait assertions to the
graph database that are inferred by propagating selected traits through the
taxonomic hierarchy.  An inferred trait assertion is represented as
an `:inferred_trait` relationship between a Page node and a Trait
node, very similar in form to a `:trait` relationship.

## Preparing a resource file

Branch painting is driven by 'start' and 'stop' directives associated
with traits in the relevant resource.  Directives are represented as
MetaData nodes attached to Trait nodes delimiting the subtree that is to be 'painted'.  A
'start' directive MetaData node on a Trait says that branch painting 
of that Trati should be initiated at the
Page given in the MetaData node (the `measurement`), propagating the Trait to
descendant taxa, while a 'stop' directive says that branch painting
for the Trait should not apply to the given node or its
descendants.

Typically a Trait node will have one start directive and any number of
stop directives that are descendants of the start directive.  The page
given in the start node will typically be the same as the page associated with
the Trait.  However, the start node could be any node at all, and
there could even be multiple start nodes for the same Trait.

The stop directive for one Trait node might be accompanied by a
start directive for another trait.  That is, a second trait
can "override" the first.

Start directive MetaData nodes have a predicate of
`https://eol.org/schema/terms/starts_at`.  Stop directives have a
predicate of 
`https://eol.org/schema/terms/stops_at`.

## Using the branch painting script

Branch painting is invoked using the `paint` family of rake commands.
Applicable parameters are:

* `CONFIG`  - tag identifying configuration block within `config/config.yml`
* `ID`   - resource id (relative to publishing site)

The following commands are supported

* `show_directives` - lists all of a resource's branch painting directives
  (a directive is a 'start' or 'stop' metadata node)
* `count` - print a count of the resource's already-published inferred trait 
  assertions, from a previous painting run (should be 0 before any new painting)
* `qc` - run a series of quality control queries to identify problems
  with the resource's painting directives
* `infer` - determine a resource's inferred trait assertions (based on
  directives), and write them to a set of files in the workspace
* `stage` - copy inferences files from local workspace to staging area
* `prepare` = `infer` + `stage`
* `publish` - ask neo4j to read inferred trait assertions from file and
   add them to the graphdb
* `paint` = `prepare` + `publish`
* `clean` - remove all of a resource's inferred trait assertions from the graphdb

The choice of command, and any parameters, are communicated via
the `rake` syntax `variable=value paint:command`.

The complete sequence of operations, if one is being very careful, would be:

 1. Set up the plotter config file; see below
 2. Publish a new version of the resource
 3. Clear the cache from any previous painting run,
    since otherwise the `infer` command will be lazy and assume that
    cached results (from the previous version of the resource) are still
    correct.  Do `rm -rf infer-NNN` where NNN is the resource id.
    (found somewhere under `~/.plotter-workspace/`)
 4. `rake paint:count` - if the count is 0, that probably means
    that the resource has been recently republished and it is time to
    proceed with branch painting.  It could also mean that the
    resource id is incorrect.  If the count is nonzero, then the resource has
    been previously painted, but has not been updated since, so go
    back and make sure you've published the new version.
 4. `rake paint:show_directives` - lists all of the resource's painting directives.
    If no directives are shown then you may have the wrong resource id.
    (Ids come from the publishing site.  Production and beta have different 
    publishing sites and therefore different resource ids.)
 5. `rake paint:qc` - run quality control checks on the directives, looking for ill-formed
    ones (those referring to missing pages and so on).
 6. `rake paint:prepare` - write the inferred relationships to an `infer-NNN`
    directory on the staging server, where NNN is the resource id.
 7. `rake paint:publish` - store the inferred relationships into the graphdb.

The command `rake paint:paint` simply does `rake paint:prepare`
followed by `rake paint:publish` and is adequate by itself when
configuration is complete, the resource ID is known, and the
resource's painting directives are well formed.

## Configuration

Copy `config/config.sample.yml` to `config/config.yml` and edit the
configuration directives as appropriate for your local `plotter`
installation.

 1. Set `locations:workspace:path` to a local directory where plotter can put files
 1. Set `prod_publishing:token_file` to be the local file that contains (or will contain)
    a v3 API token; similarly for beta.  (see note, below)
     1. Obtain a production admin token using 
        `https://beta.eol.org/services/authenticate` or
        `https://eol.org/services/authenticate`
        (see API [documentation](https://github.com/EOL/eol_website/blob/master/doc/api.md))
     1. Put the token in a file
     1. Set `prod_publishing:token_file` to the path to that file
 1. Configure the staging server `locations:staging`.  The staging server must handle `scp`
        commands (similar to `ssh`) that allow the local `rake` commands to place files in 
        a directory that is visible via HTTP from the Neo4J server(s).
        The config file needs both the `scp` destination location and
        the HTTP location.  The examples in `config.sample.yml` should provide guidance.
     1. `scp_location` specifies the directory on the staging server, prefixed by the
        server name as understood by `ssh` (either a DNS name or a name configured in
        `~/.ssh/config`)
     1. `url` gives the prefix for URLs that will occur in neo4j `LOAD CSV` commands.
     1. If the local machine has a suitable directory, with an HTTP server visible to neo4j,
        the staging area can be configured to be that directory.  In this case the 
        `scp_location` does not contain a colon and `url` points to the local machine.


## Notes

If you have both admin and non-admin accounts, it would be prudent to
provide tokens for both to `plotter`.  (The admin token goes in the
file named under `update_token_file`, non-admin goes in file named
under `token_file`.)  Non-admin accounts are prevented from writing to
the graphdb, so using a non-admin account when possible reduces the
chance of mistakes, which can be difficult to remedy.  Plotter runs
commands that only read neo4j using the non-admin token.

Branch painting generates a lot of logging output.  If you are running a
local web application instance, you might want to add `config.log_level = :warn` to
`config/environments/development.rb` to reduce noise emitted to
console.
