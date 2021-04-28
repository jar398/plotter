# Vernacular names in the graphdb

## Changes to the Neo4j schema

 * `Vernacular` - node type
    * `supplier` - link to the Resource that provides the vernacular
    * `string` - property, the vernacular name
    * `language_code` - property
    * `is_preferred_name` - property (boolean)

 * vernacular - one-to-many link from Page node to Vernacular node

## What the EOL publishing logic will need to do

 * When removing a resource, remove every `Vernacular` supplied by that resource
 * When populating the graphdb from a resource, create a `Vernacular` node for each vernacular, with properties as above, linked to the `supplier` Resource node

## Using plotter to populate the graphdb with vernaculars

 1. `rake vernaculars:prepare CONF=xxx ID=nnn` - get vernaculars out of opendata resource and store csv files on staging site.  nnn is resource id, xxx is `beta` or `prod`.  See [README.md](../README.md).
 1. `rake vernaculars:publish CONF=xxx ID=nnn` - load graphdb from csv files left on staging site
 1. `rake vernaculars:erase CONF=xxx ID=nnn` - erase Vernacular nodes for the given resource
