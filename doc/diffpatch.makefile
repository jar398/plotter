# This is an example of the use of the pylib/ utilities.
# It compares dynamic hierarchy (DH) version 0.9 with DH 1.1.

# Run with: make -f doc/diffpatch.makefile

P = pylib

# Compare DH 0.9 (resource 1 -> 1) to DH 1.1 (resource 724 -> 817)

ID_A = 1
ID_B = 817

DHA := $(shell rake resource:dwca_directory CONF=prod REPO_ID=$(ID_A))
DHB := $(shell rake resource:dwca_directory CONF=prod REPO_ID=$(ID_B))

DHA_TABLE = $(DHA)/taxa.txt
DHB_TABLE = $(DHB)/taxon.tab
DHA_MAP = ~/.plotter_workspace/prod_repo/resources/$(ID_A)/page_id_map.csv
DHB_MAP = ~/.plotter_workspace/prod_repo/resources/$(ID_B)/page_id_map.csv


all: work/diff-dha-dhb.csv

# Smoke test
smoke:
	@echo $(DHA) 
	@echo $(DHB)

fetch: $(DHA_TABLE) $(DHB_TABLE)

work/diff-dha-dhb.csv: work/dha_accepted.csv work/dhb_accepted.csv $P/diff.py
	$P/diff.py --key=EOLid < work/dha_accepted.csv work/dhb_accepted.csv > $@.new
	mv $@.new $@

COLUMNS_TO_KEEP = \
  EOLid,taxonID,parentNameUsageID,acceptedNameUsageID,taxonRank,canonicalName,scientificName,source,landmark_status
ACC_COL_DROP = \
  taxonID,parentNameUsageID,acceptedEOLid
SYN_COL_KEEP = \
  taxonID,taxonRank,canonicalName,scientificName,acceptedEOLid

CODE = $P/start.py $P/shunt.py $P/project.py $P/map.py $P/prepare.py

work/dha_accepted.csv: $(DHA_TABLE) $(DHA_MAP) $(CODE)
	mkdir -p work
	$P/start.py --input $(DHA_TABLE) \
	| $P/project.py --keep="$(COLUMNS_TO_KEEP)" \
	| $P/map.py --mapping $(DHA_MAP) \
	| $P/shunt.py --synonyms work/dha_shunt.csv \
	| $P/project.py --drop="$(ACC_COL_DROP)" \
	| $P/prepare.py --key=EOLid \
	> $@.new
	$P/project.py --keep="$(SYN_COL_KEEP)" < work/dha_shunt.csv \
	> work/dha_synonyms.csv.new
	mv $@.new $@
	mv work/dha_synonyms.csv.new work/dha_synonyms.csv

work/dhb_accepted.csv: $(DHB_TABLE) $(DHB_MAP) $(CODE)
	mkdir -p work
	$P/start.py --input $(DHB_TABLE) --clean \
	| $P/project.py --keep="$(COLUMNS_TO_KEEP)" \
	| $P/map.py --mapping $(DHB_MAP) \
	| $P/shunt.py --synonyms work/dhb_shunt.csv \
	| $P/project.py --drop="$(ACC_COL_DROP)" \
	| $P/prepare.py --key=EOLid \
	> $@.new
	$P/project.py --keep="$(SYN_COL_KEEP)" < work/dhb_shunt.csv \
	> work/dhb_synonyms.csv.new
	mv $@.new $@
	mv work/dhb_synonyms.csv.new work/dhb_synonyms.csv

$(DHA_MAP): 
	rake resource:map CONF=prod REPO_ID=$(ID_A)
$(DHB_MAP):
	rake resource:map CONF=prod REPO_ID=$(ID_B)
$(DHA_TABLE):
	rake resource:fetch CONF=prod REPO_ID=$(ID_A)
$(DHB_TABLE):
	rake resource:fetch CONF=prod REPO_ID=$(ID_B)
