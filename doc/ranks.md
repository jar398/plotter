There ought to be a batch job that does the whole operation.

**Download the DH:**
  In a browser, visit https://opendata.eol.org/dataset/tram-807-808-809-810-dh-v1-1
  We want the 'EOL DH active version' - accessing this could be scripted using
  the opendata API, but is it worth it?
  Click on 'Explore' then 'download' to get the .tar.gz file.

**Extract taxon.tab:** (we only need that one file out of the .tar.gz)

    tar xzf ....tar.gz taxon.tab

**Verify columns:** It's probably a good idea to verify that the columns in taxon.tab are
  what we expect, since columns could conceivably change from one DH 
  version to the next.  The columns we want are:

  * taxonID, which we expect in the column 1 (first column)
  * taxonRank, which we expect in column 8
  * EOLid, which we expect in column 13 (not currently used)

  If these change, the following `cut` command will need to be modified.

**Process taxon.tab:** Use the `cut` command to get relevant columns of `taxon.tab`
  and convert tabs to commas; results to `ranks.csv`

    cut --output-delimiter="," -f "1,8,13" <taxon.tab >ranks.csv

**Download the `provider_ids.csv` file:** (I don't think this is in a resource)

    wget https://eol.org/data/provider_ids.csv.gz
    gunzip provider_ids.csv.gz

**Confirm `provider_ids.csv` version:** If possible, confirm somehow that provider_ids.csv file corresponds
  to the active DH version.  I don't know how to do this.

**Process `provider_ids.csv`:** Use `egrep` to obtain relevant rows of `provider_ids.csv`, yielding `page_ids.csv`
  (maybe just an optimization, but I'm not sure)

    egrep "EOL-|resource_pk," provider_ids.csv >page_ids.csv

**Install `textql`:** Install `textql` if necessary, using apt-get, brew, or whatever

*Join:** Use `textql` to join `ranks.csv` with `page_ids.csv`, yielding `join.csv`

    (time textql -header -output-header -sql '
      select ranks.taxonID as taxon_id, page_ids.page_id as page_id,
        ranks.taxonRank as rank
      from ranks inner join page_ids
      on ranks.taxonID = page_ids.resource_pk' ranks.csv page_ids.csv >join.csv)

**Split:** Split join.csv so we can update in chunks, to forestall timeouts

    ./split-csv.sh join.csv

**Erase:** (Optional / TBD: erase existing rank properties in the graphdb... I
  didn't have to do this, since there weren't any there. The consequences of not doing this do
  not seem to be severe so it's possible to skip this step.)

**Use cypher service to set ranks:**

    ./set-ranks.sh join.csv.chunks https://beta.eol.org/ beta-admin.token

# Scripts

When this is a bit more mature, put these (or their successors) in the repository somewhere.

split-csv.sh:

    #!/bin/bash
    set -e
    FILE="$1"
    DIR="$FILE".chunks

    mkdir -p "$DIR"
    tail --lines=+2 "$FILE" | split --lines=100000 - "$DIR"/
    for f in "$DIR"/*; do
        echo "adding header line -> $f.csv"
        (head -1 "$FILE"; cat "$f") >"$f".csv
        rm "$f"
    done

set-ranks.sh:

    #!/bin/bash

    # Set rank property of Page nodes in graphdb
    #
    # set-ranks.sh join.csv.chunks https://beta.eol.org/ beta-admin.token
    # set-ranks.sh join.csv.chunks https://eol.org/ production-admin.token

    set -e
    CHUNKS="$1"
    RSYNCDEST=varela:public_html/eol
    RSYNCSOURCE=http://varela.csail.mit.edu/~jar/eol
    SERVER="$2"
    TOKEN="$3"

    rsync -pr $CHUNKS $RSYNCDEST/

    for f in $CHUNKS/*; do
      echo $f
      b=`basename $f`
      python3 doc/cypher.py --format csv --unsafe true \
        --server "$SERVER" --tokenfile "$TOKEN" \
        --query \
      'LOAD CSV WITH HEADERS FROM "$RSYNCSOURCE/'$b'"
      AS row
      MATCH (page:Page {page_id: toInteger(row.page_id)})
      WHERE row.rank <> ""
      SET page.rank = row.rank
      RETURN COUNT(page) LIMIT 1'
    done
