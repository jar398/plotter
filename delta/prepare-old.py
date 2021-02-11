# Prepare EOL taxonomy for use with diff tool.

import sys, csv

# The main thing to do is to add EOL ids for rows that lack them.
# Also maybe some data cleaning.

dh11_columns = \
  ['taxonID', 'source', 'furtherInformationURL',
   'acceptedNameUsageID', 'parentNameUsageID', 'scientificName',
   'higherClassification', 'taxonRank', 'taxonomicStatus',
   'taxonRemarks', 'datasetID', 'canonicalName', 'EOLid',
   'EOLidAnnotations', 'Landmark']

# Ignored: higherClassification


def prepare(in_path, map_path, out_path):
  page_id_map = read_page_id_map(map_path)
  (delim, qc, qu) = csv_parameters(in_path)
  with open(in_path, 'r') as infile:
    reader = csv.reader(infile, delimiter=delim, quotechar=qc, quoting=qu)
    header = next(reader)
    taxid_pos = header.index("taxonID")
    eolid_pos = header.index("EOLid")
    canon_pos = header.index("canonicalName")
    sci_pos = header.index("scientificName")
    taxstat_pos = header.index("taxonomicStatus")
    add_canon = 0
    move_canon = 0
    fix_sci = 0
    with open(out_path, 'w') as outfile:
      writer = csv.writer(outfile)
      writer.writerow(header)
      for row in reader:
        taxid = row[taxid_pos]
        if taxid in page_id_map:
          eolid = page_id_map[taxid]
          if row[eolid_pos] == '':
            row[eolid_pos] = page_id_map[taxid]
          elif row[eolid_pos] != eolid:
            print ("disagreement: %s %s" % (eolid, row[eolid_pos]))
        if row[canon_pos] == '':
          if not is_scientific(row[sci_pos]):
            row[canon_pos] = row[sci_pos]
            add_canon += 1
        else:
          if is_scientific(row[canon_pos]):
            if row[sci_pos] == '':
              row[sci_pos] = row[canon_pos]
              row[canon_pos] = ''
              move_canon += 1
            elif row[sci_pos] != row[canon_pos]:
              print ("%s Canonical looks scientific: %s | %s" %
                     (taxid, row[canon_pos], row[sci_pos]))
            else:
              row[canon_pos] = ''
              fix_sci += 1
        writer.writerow(row)
    print ("Copied scientific to canonical %s times" % add_canon)
    print ("Moved canonical to scientific %s times" % move_canon)
    print ("Removed scientific from canonical %s times" % fix_sci)

def read_page_id_map(map_path):
  pmap = {}
  (delim, qc, qu) = csv_parameters(map_path)
  with open(map_path, 'r') as infile:
    print ("Reading %s" % map_path)
    reader = csv.reader(infile, delimiter=delim, quotechar=qc, quoting=qu)
    header = next(reader)
    if len(header) != 2:
      print ("** Unexpected stuff in page id map %s\n   %s" % (map_path, header,))

    for row in reader:
      (resource_pk, page_id) = row
      pmap[resource_pk] = page_id
  print ("page id map has %s entries" % len(pmap))
  return pmap

# copied from cldiff/src/table.py
def csv_parameters(path):
  if path.endswith(".csv"):
    return (",", '"', csv.QUOTE_MINIMAL)
  else:
    return ("\t", "\a", csv.QUOTE_NONE)

if __name__ == '__main__':
  prepare(sys.argv[1], sys.argv[2], sys.argv[3])
