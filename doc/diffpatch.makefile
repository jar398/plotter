# This is an example of the use of the pylib/ utilities.

# Run with: 
#   make -f doc/diffpatch.makefile A=oldtable B=newtable
# The tables here would be oldtable.csv and newtable.csv
#
# For example:

#A=work/dh11-mapped
#B=work/dh12

A=work/dh11-mapped-mammals
B=work/dh12-mammals

all: work/delta.csv

SHELL = /usr/bin/bash
P = pylib

# Columns managed by diff/patch
MANAGED="taxonID,canonicalName,scientificName,taxonRank,source,taxonomicStatus,datasetID,EOLid"
SORTKEY="taxonID"

# Formerly: $P/project.py --keep $(MANAGED) <$< | ...

%-input.csv: %.csv
	set -o pipefail; \
	$P/sortcsv.py --key $(SORTKEY) <$< >$@.new
	mv -f $@.new $@

work/delta.csv: $A-input.csv $B-input.csv $P/match.py
	set -o pipefail; \
	$P/match.py --target $B-input.csv --pk taxonID < $A-input.csv \
	| $P/sortcsv.py --key $(SORTKEY) \
	> $@.new
	mv -f $@.new $@

# something:
# 	$P/scatter.py --dest work/delta < work/delta.csv

work/round.csv: work/delta.csv
	set -o pipefail; \
	$P/apply.py --delta $< --pk taxonID \
	    < $A-input.csv \
	| $P/sortcsv.py --key $(SORTKEY) \
	> $@.new
	mv -f $@.new $@
	echo Now, compare $@ to work/$B-input.csv
	wc work/$B-input.csv $@

# Particular taxa files to use with the above

inputs: dh work/dh09-mapped.csv work/dh11-mapped.csv
dh: work/dh09.csv work/dh11.csv work/dh12.csv
ASSEMBLY=prod

work/dh09.id:
	echo 1 >$@
work/dh11.id:
	echo 724 > $@

# about half a minute for DH 1.1

work/%.csv: work/%.id $P/start.py
	mkdir -p work
	$P/start.py --input `rake resource:taxa_path \
	       	          CONF=$(ASSEMBLY) \
		          ID=$$(cat $<)` \
		    --pk taxonID \
	        > $@.new
	mv -f $@.new $@

DH12_LP="https://opendata.eol.org/dataset/tram-807-808-809-810-dh-v1-1/resource/02037fde-cc69-4f03-94b5-65591c6e7b3b"

work/dh12.csv: $P/start.py
	mkdir -p work
	$P/start.py --input `rake dwca:taxa_path OPENDATA=$(DH12_LP)` \
		    --pk taxonID \
	       > $@.new
	mv -f $@.new $@

work/%-map.csv: work/%.id
	ln -sf `rake resource:map CONF=$(ASSEMBLY) ID=$$(cat $<)` \
	       $@

# $< = first prerequisite

work/%-mapped.csv: work/%.csv work/%-map.csv $P/map.py
	$P/map.py --mapping $(<:.csv=-map.csv) \
		  < $< \
		  > $@.new
	mv -f $@.new $@

# in1=./deprecated/work/1-mam.csv
# in2=./deprecated/work/724-mam.csv

# Mammals = page id 1642, usage id EOL-000000627548
MAMMALIA=EOL-000000627548

work/%-mammals.csv: work/%.csv $P/subset.py
	$P/subset.py --hierarchy $< --root $(MAMMALIA) < $< > $@.new
	mv -f $@.new $@
