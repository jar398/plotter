
CLDIFF=../../cldiff/src

work/patch/new.csv: work/1-prepared.csv work/724-prepared.csv diff.py
	mkdir -p work/patch
	time python3 diff.py work/1-prepared.csv work/724-prepared.csv work/patch

DH_1   = ~/.plotter_workspace/dwca/dd50ec34/unpacked/taxa.txt
DH_724 = ~/.plotter_workspace/dwca/db5120e8/unpacked/taxon.tab

work/1-mam.csv: $(CLDIFF)/subset_dwc.py work/1-prepared.csv
	python3 $(CLDIFF)/subset_dwc.py work/1-prepared.csv -168130 --out $@

work/724-mam.csv: $(CLDIFF)/subset_dwc.py work/724-prepared.csv
	python3 $(CLDIFF)/subset_dwc.py work/724-prepared.csv EOL-000000627548 --out $@

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

work/prod-hierarchy.csv: ../lib/hierarchy.rb
	rake hierarchy:dump CONF=prod DEST=$(PWD)/work/prod-hierarchy.csv
work/prod-prepared.csv: work/prod-hierarchy.csv prepare.py
	time python3 prepare.py work/prod-hierarchy.csv >$@.new
	mv $@.new $@

work/prod-diff/new.csv: work/prod-prepared.csv work/724-prepared.csv diff.py
	mkdir -p work/prod-diff
	time python3 diff.py work/prod-prepared.csv work/724-prepared.csv work/prod-diff

