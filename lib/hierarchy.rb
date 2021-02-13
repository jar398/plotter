# Prepare for update:
#   prepare  - prepare hierarchy for use with 'diff'
#   diff  - generate patch bundle.
# Update sequence should be:
#   load   - add new nodes.
#   patch  - change fields of persisting nodes (including parents).
#     Fields to patch: 
#       parentNameUsageID, canonicalName, taxonRank
#     we could do these in three passes? maybe using grep?
#   delete - remove deprecated nodes.

# Assume we are not subject to web server timeouts... we're running in
# a privileged envirnment that has direct access to the neo4j server.
# This means we can do long-running queries, perhaps with PERIODIC COMMIT.

class Hierarchy

  def initialize(assembly)
    @assembly = assembly
  end

  def get_graph
    @assembly.get_graph
  end

  def run_query(cql)
    get_graph.run_query(cql)
  end

  def load(filename)
    puts "Loading page records from #{filename}"
    # First, create a Page node for each page id
    # taxonID,parentNameUsageID,canonicalName,scientificName,taxonRank,taxonomicStatus
    # We need a URL for the file !... hmm
    query = "USING PERIODIC COMMIT
             LOAD CSV WITH HEADERS FROM '#{filename}'
             AS row
             WITH row, toInteger(row.taxonID) AS page_id
             MERGE (page:Page {page_id: page_id})
             SET page.rank = row.taxonRank
             SET page.canonical = row.canonicalName
             RETURN COUNT(page)
             LIMIT 100000000"
    r = run_query(query)
    raise "Page table load failed" unless r 
    n = r["data"][0][0]
    puts "#{n} page records loaded"

    # Second, set the parent pointers.
    patch_parents(filename, "parentNameUsageID")
  end

  # Patch parent links

  def patch_parents(filename, parent_field = "to")
    puts "Patching parent links according to #{filename}"
    if parent_field == "to"
      w = "WHERE row.field = 'parentNameUsageID'"
    else
      w = ""
    end
    # The stupid 'WITH row' gets around a pointless Cypher clause syntax restriction
    query = "USING PERIODIC COMMIT
             LOAD CSV WITH HEADERS FROM '#{filename}'
             AS row
             WITH row
             #{w}
             WITH row,
                  toInteger(row.taxonID) AS page_id,
                  toInteger(row.#{parent_field}) AS parent_id
             MATCH (page:Page {page_id: page_id})
             MATCH (parent:Page {page_id: parent_id})
             MERGE (page)-[:parent]->(parent)
             RETURN COUNT(page)
             LIMIT 100000000"
    r = run_query(query)
    raise "No parent links set" unless r
    n = r["data"][0][0]
    puts "#{n} parent links set"
  end

  # Patch other fields

  def patch_field(filename, column, prop)
    puts "Patching property #{prop} from column #{column}"
    query = "USING PERIODIC COMMIT
             LOAD CSV WITH HEADERS FROM '#{filename}'
             AS row
             WITH row
             WHERE row.field = '#{column}'
             WITH row, toInteger(row.taxonID) AS page_id
             MATCH (page:Page {page_id: page_id})
             SET page.#{prop} = row.to
             RETURN COUNT(page)
             LIMIT 100000000"
    r = run_query(query)
    raise "No #{prop} properties set" unless r
    n = r["data"][0][0]
    puts "#{n} #{prop} properties set"
  end

  def patch(dirname)
    load(File.join(dirname, "new.csv"))
    changefile = File.join(dirname, "change.csv")
    patch_parents(changefile)
    delete(File.join(dirname, "delete.csv"))
    patch_field(changefile, "taxonRank", "rank")
    patch_field(changefile, "canonicalName", "canonical")
  end

  # Delete nodes.  Maybe check first to see which ones have traits?

  def delete(filename)
    puts "Deleting pages listed in #{filename}"
    query = "USING PERIODIC COMMIT
             LOAD CSV WITH HEADERS FROM '#{filename}'
             AS row
             WITH row,
                  toInteger(row.taxonID) AS page_id
             MATCH (page:Page {page_id: page_id})
             DETACH DELETE page
             RETURN COUNT(page)
             LIMIT 100000000"
    r = run_query(query)
    raise "No deletions found" unless r
    n = r["data"][0][0]
    puts "#{n} deletions performed"
  end

  # Change fields of nodes that are kept.

end
