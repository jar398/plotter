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
    rank_pos = header.index("taxonRank")
    canon_pos = header.index("canonicalName")
    if "scientificName" in header:
      sci_pos = header.index("scientificName")
    else: sci_pos = -1
    if "taxonomicStatus" in header:
      taxstat_pos = header.index("taxonomicStatus")
    else: taxstat_pos = -1

    def id_to_page(id):
      if mappings:
        return mappings.get(id)
      elif len(id) > 0:
        return int(id)
      else:
        return None

    rows = []
    losers = 0
    all_pages = {}
    redundant = 0
    orphans = 0
    for row in reader:
      id = row[id_pos]
      page = id_to_page(id)
      # Synonyms have no mappings
      if page:
        parent = row[parent_pos]
        parent_page = id_to_page(parent)
        if not parent_page:
          orphans += 1
        if parent_page == page:
          # e.g. 467511,467511,Dicoria canescens hispidula,...
          losers += 1
          continue
        rank = row[rank_pos]
        canon = row[canon_pos]
        sci = row[sci_pos] if sci_pos > 0 else None
        taxstat = row[taxstat_pos] if taxstat_pos > 0 else None
        if page in all_pages:
          if isinstance(page,int) and page % 211 == 0:
            (id2, canon2) = all_pages[page]
            print("Redundant: %s -> %s %s, %s %s" % (page, id, canon, id2, canon2), file=sys.stderr)
          redundant += 1
          continue
        all_pages[page] = (id, canon)
        rows.append([page,
                     parent_page,
                     rank,
                     canon,
                     sci,
                     taxstat])
    print("%s orphans" % orphans, file=sys.stderr)
    print("%s self-parent nodes" % losers, file=sys.stderr)
    print("%s redundant nodes" % redundant, file=sys.stderr)
    print("%s pages" % len(all_pages), file=sys.stderr)
    rows.sort(key=lambda row:(row[0], len(row[2])))

    # Write out the prepared taxon table
    writer = csv.writer(sys.stdout)
    writer.writerow(["taxonID",
                     "parentNameUsageID",
                     "taxonRank",
                     "canonicalName",
                     "scientificName",
                     "taxonomicStatus"])
    for row in rows:
      writer.writerow(row)

def read_mappings(mapfile):
  if not mapfile: return None
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
  if len(sys.argv) > 1:
    taxa = sys.argv[1]
  else:
    taxa = "/home/jar/.plotter_workspace/dwca/db5120e8/unpacked/taxon.tab"
  if len(sys.argv) > 2:
    mapfile = sys.argv[2]
  else:
    mapfile = None
  prepare(mapfile, taxa)
