Things to try

(eventually automate these tests)

## Cypher tests

```
rake cypher CONF=beta QUERY="match (p:Page) return count(p) limit 1"
rake cypher CONF=prod QUERY="match (p:Page) return count(p) limit 1"
```

## Clear out the test graphdb, and populate it with Pages

```
# This ought to be translated to ruby and accessed via traits.rake
(cd diffpatch; make wipe)

(cd diffpatch; make prepare-prod)

time rake hierarchy:load CONF=test PAGES="file:///prod-dh-prepared.csv"

rake cypher CONF=test QUERY="match (p:Page) return count(p)"
```

## Populate graphdb with Resources


## Resource tests

```
rake resource:info CONF=test ID=40
rake resource:info CONF=beta ID=40

rake resource:fetch CONF=test ID=40

rake resource:map CONF=test ID=40
```

## Vernaculars tests

```
rake resource:erase_vernaculars CONF=test ID=40
rake resource:count_vernaculars CONF=test ID=40

rake resource:prepare_vernaculars CONF=test ID=40
rake resource:publish_vernaculars CONF=test ID=40

rake resource:count_vernaculars CONF=test ID=40
```

## Hierarchy tests including diff/patch

```



```
