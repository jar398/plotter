# Vernacular names in the graphdb

## Changes to the Neo4j schema

 * `Vernacular` - node type
    * `supplier` - link to the `Resource` node that provides the vernacular
    * `string` - property, the vernacular name
    * `language_code` - property
    * `is_preferred_name` - property (integer) (what is default?)

 * `vernacular` - one-to-many link from `Page` node to `Vernacular` node

## What the EOL publishing logic will need to do

 * When removing a resource, remove every `Vernacular` supplied by that resource
 * When populating the graphdb from a resource, create a `Vernacular` node for each vernacular, with properties as above, linked via `supplier` to its `Resource` node

## Using plotter to populate the graphdb with vernaculars

There is now code to create Vernacular nodes whenever a resource is
published.  To create Vernaculars for resources that were published
before this code came into being, there are raks tasks:

 1. `rake vernaculars:prepare CONF=xxx ID=nnn` - get vernaculars out of opendata resource and store csv files on staging site.  nnn is resource id, xxx is `beta` or `prod`.  See [README.md](../README.md).
 1. `rake vernaculars:publish CONF=xxx ID=nnn` - load graphdb from csv files left on staging site
 1. `rake vernaculars:erase CONF=xxx ID=nnn` - erase Vernacular nodes for the given resource

E.g. one might try `ID=40` (English vernaculars) or `CONF=beta ID=559`
or `CONF=prod ID=557` (Wikidata).
