# Read a dynamic hierarchy, map node ids to page ids, drop unneeded
# columns, write out the result, sorted by page id

import sys, csv

def prepare(mapfile, taxa):
  mappings = read_mappings(mapfile)
  (merged, ordered_keys, header) = read_and_merge(taxa, mappings)
  key_is_page_id = is_key_page_id(header, mappings)
  write_prepared(merged, ordered_keys, header, key_is_page_id)

def read_and_merge(taxa, mappings):
  merged = {}
  conflicts = {}    # some page ids ... ?
  rows_in = 0
  redundant = 0
  keyless = 0
  losers = 0
  orphans = 0
  (delim, qc, qu) = csv_parameters(taxa)
  no_key = 0
  with open(taxa, "r") as infile:
    reader = csv.reader(infile, delimiter=delim, quotechar=qc, quoting=qu)
    header = next(reader)
    node_id_pos = windex(header, "taxonID")
    page_id_pos = windex(header, "EOLid")
    key_is_page_id = is_key_page_id(header, mappings)
    parent_page_pos = windex(header, "parentEOLid")       # another page id
    parent_node_pos = windex(header, "parentNameUsageID") # another node id
    # Read the input and merge redundant rows, producing 'merged'
    for row in reader:
      rows_in += 1
      node_id = get(row, node_id_pos)
      page_id = get_int(row, page_id_pos)
      key = page_id if key_is_page_id else node_id
      # Synonyms have no mappings... ?
      if key:
        parent_key = (get_int(row, parent_page_pos)
                      if key_is_page_id 
                      else get(row, parent_node_pos))
        # Report on orphans (parentless rows)
        if not parent_key:
          orphans += 1
        # Discard rows whose parent has same key
        elif parent_key == key:
          # e.g. 467511,467511,Dicoria canescens hispidula,...
          losers += 1
          continue
        if key in merged:
          have_row = merged[key]
          if isinstance(key, int) and key % 211 == 0:
            have_node_id = get(have_row, node_id_pos)
            have_page_id = get_int(have_row, page_id_pos)
            print("Redundant: %s <- %s|%s, %s|%s" %
                  (key,
                   node_id, page_id,
                   have_node_id, have_page_id),
                  file=sys.stderr)
          redundant += 1
          merged[key] = merge(have_row, row)
          conflicts[key] = True
          continue
        else:
          merged[key] = row
      else:
        no_key += 1
  print("%s rows in" % rows_in, file=sys.stderr)
  print(  "%s keyless" % no_key, file=sys.stderr)
  print(  "%s keys with >1 rows" % len(conflicts), file=sys.stderr)
  print(  "%s redundant rows" % redundant, file=sys.stderr)
  print(  "%s orphans" % orphans, file=sys.stderr)
  print(  "%s self-parent nodes" % losers, file=sys.stderr)
  ordered_keys = sorted(merged.keys())
  print("%s rows out" % len(merged), file=sys.stderr)
  return (merged, ordered_keys, header)

def write_prepared(merged, ordered_keys, header, key_is_page_id):
  node_id_pos = windex(header, "taxonID")
  page_id_pos = windex(header, "EOLid")
  parent_page_pos = windex(header, "parentEOLid")       # another page id
  parent_node_pos = windex(header, "parentNameUsageID") # another node id
  rank_pos    = windex(header, "taxonRank")
  canon_pos   = windex(header, "canonicalName")
  sci_pos     = windex(header, "scientificName")
  taxstat_pos = windex(header, "taxonomicStatus")
  landmark_pos = windex(header, "landmark")     # Numeric not symbolic

  def row_to_page(row, key):
    return [get(row, page_id_pos),
            get(row, parent_page_pos),
            get(row, rank_pos),
            get(row, canon_pos),
            get(row, sci_pos),
            get(row, taxstat_pos),
            convert_landmark(get(row, landmark_pos)),
            get(row, node_id_pos)]

  # Write out the prepared taxon table, suitable for ingestion by
  # `load` in hierarchy.rb
  writer = csv.writer(sys.stdout)
  writer.writerow(outfile_header)
  for key in ordered_keys:
    row = merged[key]
    parent_key = (get_int(row, parent_page_pos)
                  if key_is_page_id 
                  else get(row, parent_node_pos))
    if parent_key and not (parent_key in merged) and parent_key != "?":
      node_id = get(row, node_id_pos)
      page_id = get_int(row, page_id_pos)
      print("Parent key %s does not resolve for page %s node %s" %
            (parent_key, page_id, node_id),
            file=sys.stderr)
    writer.writerow(row_to_page(row, key))

outfile_header = ["EOLid",
                  "parentEOLid",
                  "taxonRank",
                  "canonicalName",
                  "scientificName",
                  "taxonomicStatus",
                  "landmark",
                  "taxonID"]

def is_key_page_id(header, mappings):
  node_id_pos = windex(header, "taxonID")
  page_id_pos = windex(header, "EOLid")
  if node_id_pos >= 0 and mappings:
    key_is_page_id = True
    print("Key is page id as mapped from node id", file=sys.stderr)
  elif node_id_pos >= 0:
    key_is_page_id = False
    print("Key is node id", file=sys.stderr)
  else:
    key_is_page_id = True
    print("Key is page id", file=sys.stderr)
  return key_is_page_id

def windex(header, fieldname):
  if fieldname in header:
    return header.index(fieldname)
  else:
    return -1

def get(row, pos):
  return row[pos] if pos >= 0 else None

def get_int(row, pos):
  val = get(row, pos)    # could be "?"
  return int(val) if val and val.isdigit() else None

def merge(row1, row2):
  return [merge_values(x, y) for (x, y) in zip(row1, row2)]

def merge_values(x, y):
  # if not x: return y
  # if not y: return x
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

def convert_landmark(landmark):
  # app/decorators/page_decorator/brief_summary.rb
  if not landmark or landmark == "0":
    return None
  elif landmark == "1":
    return "minimal"
  elif landmark == "2":
    return "abbreviated"
  elif landmark == "3":
    return "extended"
  elif landmark == "4":
    return "full"
  else:
    return str(landmark)

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
