# Read a dynamic hierarchy, map node ids to page ids, drop unneeded
# columns, write out the result, sorted by page id

import sys, csv

def prepare(mapfile, taxa):
  mappings = read_mappings(mapfile)
  (delim, qc, qu) = csv_parameters(taxa)
  with open(taxa, "r") as infile:
    reader = csv.reader(infile, delimiter=delim, quotechar=qc, quoting=qu)
    header = next(reader)
    id_pos = header.index("taxonID")
    parent_pos = header.index("parentNameUsageID")
    canon_pos = header.index("canonicalName")
    sci_pos = header.index("scientificName")
    rank_pos = header.index("taxonRank")
    taxstat_pos = header.index("taxonomicStatus")

    rows = []
    for row in reader:
      id = row[id_pos]
      page = mappings.get(id)
      # Synonyms have no mappings
      if page:
        parent = row[parent_pos]
        parent_page = mappings.get(parent)
        if not parent_page:
          print("Missing mapping for %s (parent of %s)" %
                (parent, page), file=sys.stderr)
        canon = row[canon_pos]
        sci = row[sci_pos]
        rank = row[rank_pos]
        taxstat = row[taxstat_pos]
        rows.append([page,
                     parent_page,
                     canon,
                     sci,
                     rank,
                     taxstat])
    rows.sort()

    # Write out the prepared taxon table
    writer = csv.writer(sys.stdout)
    writer.writerow(["taxonID",
                     "parentNameUsageID",
                     "canonicalName",
                     "scientificName",
                     "taxonRank",
                     "taxonomicStatus"])
    for row in rows:
      writer.writerow(row)

def read_mappings(mapfile):
  mappings = {}
  with open(mapfile, "r") as infile:
    reader = csv.reader(infile)
    next(reader)
    for [node_id, page_id] in reader:
      mappings[node_id] = int(page_id)
  print("Mappings: %s" % len(mappings),
        file=sys.stderr)
  return mappings

def csv_parameters(path):
  if ".csv" in path:
    return (",", '"', csv.QUOTE_MINIMAL)
  else:
    return ("\t", "\a", csv.QUOTE_NONE)

#  for row in reader:
    
if __name__ == '__main__':
  mapfile = "/home/jar/g/plotter/delta/work/724-map.csv"
  taxa = "/home/jar/.plotter_workspace/dwca/db5120e8/unpacked/taxon.tab"
  if len(sys.argv) > 1:
    taxa = sys.argv[1]
    if len(sys.argv) > 2:
      mapfile = sys.argv[2]
  prepare(mapfile, taxa)
