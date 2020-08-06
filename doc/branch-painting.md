# Branch painting

TL;DR: use `rake paint:paint CONFIG=x ID=y` where x is `beta` or
`prod` and y is the id of the resource according to neo4j (and the publishing
database).  E.g.

    rake paint:paint CONFIG=beta ID=640

'Branch painting' is the process of adding of trait assertions to the
graph database that are inferred by propagating selected traits through the
taxonomic hierarchy.  An inferred trait assertion is represented as
an `:inferred_trait` relationship between a Page node and a Trait
node, very similar in form to a `:trait` relationship.

## Preparing a resource file

Branch painting is driven by 'start' and 'stop' directives associated
with traits in the relevant resource.  Directives are represented as
MetaData nodes attached to the Trait node that is to be 'painted'.  A
'start' directive says that branch painting should be initiated at the
Page given in the directive (its `measurement`), propagating to
descendant taxa, while a 'stop' directive says that branch painting
for this trait should not continue apply to the given node or its
descendants.

Typically a Trait node will have one start directive and any number of
stop directives that are descendants of the start directive.  The page
given in the start node will be the same as the page associated with
the trait.  However, the start node could be any node at all, and
there could even be multiple start nodes.

Often the stop directive for one trait will be a start node for
another trait.  That is, the second trait "overrides" the first.

Start directive MetaData nodes have a predicate of
https://eol.org/schema/terms/starts_at.  Stop directives have a
predicate of 
https://eol.org/schema/terms/stops_at.

## Using the branch painting script

The branch painting script (in [lib/painter.rb](../lib/painter.rb))
implements a suite of operations related to branch painting.

* `count` - count a resource's inferred trait assertions
* `qc` - run a series of quality control queries to identify problems
  with the resource's directives
* `infer` - determine a resource's inferred trait assertions (based on
  directives), and write them to a file
* `merge` - read inferred trait assertions from file (see `infer`) and
  add them to the graphdb
* `clean` - remove all of a resource's inferred trait assertions
* `directives` - lists all of a resource's branch painting directives
  (a directive is a 'start' or 'stop' metadata node)

The choice of command, and any parameters, are communicated via
the `rake` syntax `variable=value command`.

Branch painting is invoked using the `paint` family of rake commands.
Applicable parameters are:

* `CONFIG`  - tag identifying configuration block within `config/config.yml`
* `ID`   - resource id (relative to publishing site)

The complete sequence of operations, if one is being very careful, would be:

 1. Set up the plotter config file
     1. Obtain a production admin token using 
        `https://beta.eol.org/services/authenticate` or
        `https://eol.org/services/authenticate`
        (see API documentation)
     2. Put it in the appropriate place in the config file
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
 5. `rake paint:qc` - run quality control checks on the directives, looking for ill-formed
    ones (those referring to missing pages and so on).
 6. `rake paint:infer` - write the inferred relationships to an `infer-NNN`
    directory, where NNN is the resource id.
 7. `rake paint:merge` - store the inferred relationships into the graphdb.

The command `rake paint:paint` simply combines `rake paint:infer` followed by `rake paint:merge`.

## Notes

If you have both admin and non-admin tokens, it would be prudent to
run all but the last command using the non-admin token, out of an
abundance of caution.  Non-admin accounts are prevented from writing
to the graphdb.

Branch painting generates a lot of logging output.  If you are running a
local web application instance, you might want to add `config.log_level = :warn` to
`config/environments/development.rb` to reduce noise emitted to
console.
