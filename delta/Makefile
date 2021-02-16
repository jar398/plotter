
IMPORT=/usr/local/src/neo4j-community-3.5.11/import

# Be sure to do 'make wipe' first to wipe the test graphdb clean.
# A good result for 'confirm' is all zeroes in the report except for
# 'field added' and 'unchanged'.

confirm: work/confirm/new.csv

work/confirm/new.csv: work/test-dh-prepared.csv work/724-prepared.csv
	time python3 diff.py work/test-dh-prepared.csv work/724-prepared.csv work/confirm

work/test-dh-prepared.csv: work/dump-of-test-dh.csv prepare.py
	time python3 prepare.py work/dump-of-test-dh.csv >$@.new
	mv $@.new $@

# load: 2:43, patch: 1:41, dump: 1:19

work/dump-of-test-dh.csv: work/1-prepared.csv work/patch/new.csv ../lib/hierarchy.rb
	cp -p work/1-prepared.csv $(IMPORT)/
	time rake hierarchy:load CONF=test PAGES="file:///1-prepared.csv"
	cp -pr work/patch $(IMPORT)/
	time rake hierarchy:patch CONF=test PATCH="file:///patch"
	rm work/dump-of-test-dh.csv
	time rake hierarchy:dump CONF=test DEST=$(PWD)/work/dump-of-test-dh.csv

# Create diff/patch directives to morph 0.9 into 1.1

work/patch/new.csv: work/1-prepared.csv work/724-prepared.csv diff.py
	time python3 diff.py work/1-prepared.csv work/724-prepared.csv work/patch

# This is very dangerous -- it loses everything in the test graphdb

wipe:
	neo4j stop
	rm -r $(IMPORT)/../data/databases/graph.db
	neo4j start
	sleep 5
	rake hierarchy:create_indexes CONF=test

# Compare production graphdb against 1.1  (or, could compare against patched 0.9)

compare-to-prod: work/prod-diff/new.csv

GROUND=724
#GROUND=test-dh

work/prod-diff/new.csv: work/prod-dh-prepared.csv work/$(GROUND)-prepared.csv diff.py
	time python3 diff.py work/prod-dh-prepared.csv work/$(GROUND)-prepared.csv work/prod-diff

work/prod-dh-prepared.csv: work/dump-of-prod-dh.csv prepare.py
	time python3 prepare.py work/dump-of-prod-dh.csv >$@.new
	mv $@.new $@

work/dump-of-prod-dh.csv: ../lib/hierarchy.rb
	rake hierarchy:dump CONF=prod DEST=$(PWD)/work/dump-of-prod-dh.csv

# Marshall the DH files

DH_1   = ~/.plotter_workspace/dwca/dd50ec34/unpacked/taxa.txt
DH_724 = ~/.plotter_workspace/dwca/db5120e8/unpacked/taxon.tab

work/1-prepared.csv: $(DH_1) work/1-map.csv prepare.py
	time python3 prepare.py $(DH_1) work/1-map.csv >$@.new
	mv $@.new $@

work/724-prepared.csv: $(DH_724) work/724-map.csv prepare.py
	time python3 prepare.py $(DH_724) work/724-map.csv >$@.new
	mv $@.new $@

work/1-map.csv:
	python3 get_page_id_map.py 1 $@.new
	mv $@.new $@

work/724-map.csv:
	python3 get_page_id_map.py 817 $@.new
	mv $@.new $@

$(DH_1):
	(cd .. && rake resource:fetch CONF=prod ID=1)
$(DH_724):
	(cd .. && rake resource:fetch CONF=prod ID=724)

# Stow these commands here. not currently used because manipulated the
# entire DH is not so awfully slow.

CLDIFF=../../cldiff/src

work/1-mam.csv: $(CLDIFF)/subset_dwc.py work/1-prepared.csv
	python3 $(CLDIFF)/subset_dwc.py work/1-prepared.csv -168130 --out $@

work/724-mam.csv: $(CLDIFF)/subset_dwc.py work/724-prepared.csv
	python3 $(CLDIFF)/subset_dwc.py work/724-prepared.csv EOL-000000627548 --out $@

