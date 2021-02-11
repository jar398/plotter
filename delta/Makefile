
CLDIFF=../../cldiff/src

out/new.csv: work/1-prepared.csv work/724-prepared.csv diff.py
	time python3 diff.py work/1-prepared.csv work/724-prepared.csv out

TAXON_1 = ~/.plotter_workspace/dwca/dd50ec34/unpacked/taxa.txt
TAXON_724 = ~/.plotter_workspace/dwca/db5120e8/unpacked/taxon.tab

work/1-mam.csv: $(CLDIFF)/subset_dwc.py work/1-prepared.csv
	python3 $(CLDIFF)/subset_dwc.py work/1-prepared.csv -168130 --out $@

work/724-mam.csv: $(CLDIFF)/subset_dwc.py work/724-prepared.csv
	python3 $(CLDIFF)/subset_dwc.py work/724-prepared.csv EOL-000000627548 --out $@

work/1-prepared.csv: $(TAXON_1) work/1-map.csv prepare.py
	time python3 prepare.py $(TAXON_1) work/1-map.csv >$@.new
	mv $@.new $@

work/724-prepared.csv: $(TAXON_724) work/724-map.csv prepare.py
	time python3 prepare.py $(TAXON_724) work/724-map.csv >$@.new
	mv $@.new $@

work/1-map.csv:
	python3 get_page_id_map.py 1 $@.new
	mv $@.new $@

work/724-map.csv:
	python3 get_page_id_map.py 817 $@.new
	mv $@.new $@

$(TAXON_1):
	(cd .. && rake resource:fetch CONF=prod ID=1)
$(TAXON_724):
	(cd .. && rake resource:fetch CONF=prod ID=724)

