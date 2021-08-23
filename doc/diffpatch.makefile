# This is an example of the use of the pylib/ utilities.

# Run with: 
#   make -f doc/diffpatch.makefile A=oldtable B=newtable
# The tables here would be oldtable.csv and newtable.csv
#
# For example:

# 1.1 / 1.2 mammals only 
A=work/dh11-mapped-mammals
B=work/dh12-mammals

# 0.9 / 1.1
# time make -f doc/diffpatch.makefile A=work/dh09-mapped B=work/dh11

# 0.9 / 1.1 mammals only
# time make -f doc/diffpatch.makefile A=work/dh09-mapped-mammals B=work/dh11-mammals

# 1.1 / 1.2
# time make -f doc/diffpatch.makefile A=work/dh11-mapped B=work/dh12

DELTA=work/delta-$(shell basename $A)-$(shell basename $B).csv
ROUND=work/round-$(shell basename $A)-$(shell basename $B).csv

all: $(ROUND)

SHELL = /usr/bin/bash
P = pylib

# Columns that are managed by diff/patch
SORTKEY="taxonID"

# Formerly: $P/project.py --keep $(MANAGE) <$< | ...

%-input.csv: %.csv
	$P/sortcsv.py --key $(SORTKEY) <$< >$@.new
	mv -f $@.new $@

INDEX=taxonID,EOLid,scientificName,canonicalName
MANAGE=$(INDEX),taxonRank,taxonomicStatus,datasetID

$(DELTA): $A-input.csv $B-input.csv $P/diff.py
	@echo
	@echo "--- COMPUTING DELTA ---"
	set -o pipefail; \
	$P/diff.py --target $B-input.csv --pk taxonID \
		   --index $(INDEX) --manage $(MANAGE) \
		   < $A-input.csv \
	| $P/sortcsv.py --key $(SORTKEY) \
	> $@.new
	mv -f $@.new $@
	wc $@

# something:
# 	$P/scatter.py --dest $(basename $(DELTA)) < $(DELTA)

$(ROUND): $(DELTA)
	@echo
	@echo "--- APPLYING DELTA ---"
	set -o pipefail; \
	$P/apply.py --delta $< --pk taxonID \
	    < $A-input.csv \
	| $P/sortcsv.py --key $(SORTKEY) \
	> $@.new
	mv -f $@.new $@
	@echo "--- Comparing $@ to $B-input.csv ---"
	wc $B-input.csv $@

# ----------------------------------------------------------------------
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
MAMMALIA11=EOL-000000627548

work/%-mammals.csv: work/%.csv $P/subset.py
	$P/subset.py --hierarchy $< --root $(MAMMALIA11) < $< > $@.new
	mv -f $@.new $@

MAMMALIA09=-168130
work/dh09-mapped-mammals.csv: work/dh09-mapped.csv $P/subset.py
	$P/subset.py --hierarchy $< --root $(MAMMALIA09) < $< > $@.new
	mv -f $@.new $@
