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

    def node_id_to_page_id(id):
      if mappings:
        return mappings.get(id)
      elif len(id) > 0:
        return int(id)
      else:
        return None

    pages = {}
    losers = 0
    redundant = 0
    conflicts = {}    # some page ids
    orphans = 0
    for row in reader:
      id = row[id_pos]
      page_id = node_id_to_page_id(id)
      # Synonyms have no mappings
      if page_id:
        parent = row[parent_pos]
        parent_page_id = node_id_to_page_id(parent)
        if not parent_page_id:
          orphans += 1
        if parent_page_id == page_id:
          # e.g. 467511,467511,Dicoria canescens hispidula,...
          losers += 1
          continue
        rank = row[rank_pos]
        canon = row[canon_pos]
        sci = row[sci_pos] if sci_pos > 0 else None
        taxstat = row[taxstat_pos] if taxstat_pos > 0 else None
        new_page = [page_id,
                    parent_page_id,
                    rank,
                    canon,
                    sci,
                    taxstat,
                    id]
        if page_id in pages:
          (have_page, have_nid) = pages[page_id]
          if isinstance(page_id, int) and page_id % 211 == 0:
            print("Redundant: %s -> %s %s, %s %s" %
                  (page_id, id, canon, have_page[0], have_page[3]),
                  file=sys.stderr)
          redundant += 1
          pages[page_id] = merge(have_page, have_nid, new_page, id)
          conflicts[page_id] = True
          continue
        else:
          pages[page_id] = (new_page, id)
    print("%s orphans" % orphans, file=sys.stderr)
    print("%s self-parent nodes" % losers, file=sys.stderr)
    print("%s redundant nodes" % redundant, file=sys.stderr)
    print("%s pages with conflict" % len(conflicts), file=sys.stderr)
    print("%s pages" % len(pages), file=sys.stderr)
    ordered = sorted(pages.values(), key=lambda page:page[0][0])

    # Write out the prepared taxon table
    writer = csv.writer(sys.stdout)
    writer.writerow(["taxonID",
                     "parentNameUsageID",
                     "taxonRank",
                     "canonicalName",
                     "scientificName",
                     "taxonomicStatus",
                     "nodeID"])
    for (page, nid) in ordered:
      if not (page[1] in pages) and page[1] and page[1] != "?":
        print("Parent page id does not resolve: %s -> %s" % (page[0], page[1]),
              file=sys.stderr)
      writer.writerow(page)

def merge(row1, nid1, row2, nid2):
  if nid1 < nid2:
    return (row1, nid1)
  else:
    return (row2, nid2)
  #was: return [merge_values(x, y) for (x, y) in zip(row1, row2)]

def merge_values(x, y):
  if not x: return y
  if not y: return x
  if x == y: return x
  else: return "?"

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
