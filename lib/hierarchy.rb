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

  def initialize(repo_resource, trait_bank)
    @resource = repo_resource
    @trait_bank = trait_bank
    @chunksize = 500000
  end

  def get_graph
    @trait_bank.get_graph
  end

  def run_query(cql)
    get_graph.run_query(cql)
  end

  def create_indexes()
    raise "Failed" unless
      run_query("CREATE INDEX ON :Page(page_id)")
  end

  def prepare_pages_table
    pages_csv = System.system.workspace_path(@resource.relative_path("pages/pages.csv"))
    names_csv = System.system.workspace_path(@resource.relative_path("pages/names.csv"))
    map_path = @resource.page_id_map_path
    in_table = @resource.get_dwca.get_table(Claes.taxon)
    input = in_table.path  #was File.join(unpacked, "taxon.tab")
    keep_columns = "EOLid,acceptedEOLid,parentEOLid,scientificName,taxonRank,taxonomicStatus,canonicalName,Landmark"
    value = 
      %x( pylib/start.py --input #{input} |\
          pylib/map.py --mapping #{map_path} |\
          pylib/project.py --keep #{keep_columns} |\
          pylib/names.py --names #{names_csv} \
            > #{pages_csv}
          bin/splitcsv #{pages_csv}
          bin/splitcsv #{names_csv} )
    STDERR.puts("Value = #{value}")
    STDERR.puts("Wrote #{pages_csv} and #{names_csv}")
  end

  def stage
    System.system.stage(@resource.relative_path("pages"))
  end

  # This is like load, but never creates new Page nodes, only sets
  # properties on existing ones

  def sync_metadata
    pages_url = System.system.staging_url(@resource.relative_path("pages/pages.csv"))
    urls = System.system.read_manifest(pages_url)
    if urls
      urls.each do |url|
        load_pages_chunk(url, op = "MATCH")
      end
    else
      load_pages_chunk(pages_url)
    end
  end

  def load
    pages_url = System.system.staging_url(@resource.relative_path("pages/pages.csv"))
    urls = System.system.read_manifest(pages_url)
    if urls
      # First, create the Page nodes.
      urls.each do |url|
        load_pages_chunk(url)
      end
      # Second, set the parent pointers.
      urls.each do |url|
        patch_parents(url, "parentEOLid")
      end
    else
      load_pages_chunk(pages_url)
      patch_parents(pages_url, "parentEOLid")
    end
  end

  def load_pages_chunk(pages_url, op = "MERGE")
    puts "Loading page records from #{pages_url}"
    # First, create a Page node for each page id
    # EOLid,parentEOLid,taxonRank,canonicalName,scientificName,
    #   taxonomicStatus,Landmark
    # This isn't right.  Landmark is numeric while landmark should be symbolic ??
    query = "USING PERIODIC COMMIT
             LOAD CSV WITH HEADERS FROM '#{pages_url}'
             AS row
             WITH row, toInteger(row.EOLid) AS page_id
             #{op} (page:Page {page_id: page_id})
             SET page.rank = row.taxonRank
             SET page.canonical = row.canonicalName
             SET page.landmark = row.Landmark
             RETURN COUNT(page)
             LIMIT 100000000"
    r = run_query(query)
    raise "Page table load failed" unless r 
    n = r["data"][0][0]
    puts "#{n} page records loaded"
  end

  # Patch parent links

  def patch_parents(pages_url, parent_field = "to")
    puts "Patching parent links according to #{pages_url}"
    if parent_field == "to"
      # The stupid 'WITH row' is a workaround for a pointless Cypher clause 
      # syntax restriction 
      w =   "WITH row WHERE row.field = 'parentEOLid'"
      # We are patching, not initializing, so we'll need to get rid of any 
      # existing parent relationship
      w2 =  "OPTIONAL MATCH (page)-[rel:parent]->(:Page)
             DELETE rel"
    else
      w = ""
      w2 = ""
    end
    query = "USING PERIODIC COMMIT
             LOAD CSV WITH HEADERS FROM '#{pages_url}'
             AS row
             #{w}
             WITH row,
                  toInteger(row.EOLid) AS page_id,
                  toInteger(row.#{parent_field}) AS parent_id
             MATCH (page:Page {page_id: page_id})
             #{w2}
             WITH page, parent_id
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

  def patch_field(pages_url, column, prop)
    puts "Patching property #{prop} from column #{column}"
    query = "USING PERIODIC COMMIT
             LOAD CSV WITH HEADERS FROM '#{pages_url}'
             AS row
             WITH row
             WHERE row.field = '#{column}'
             WITH row, toInteger(row.EOLid) AS page_id
             MATCH (page:Page {page_id: page_id})
             SET page.#{prop} = row.to
             RETURN COUNT(page)
             LIMIT 100000000"
    r = run_query(query)
    raise "No #{prop} properties set" unless r
    n = r["data"][0][0]
    puts "#{n} #{prop} properties set"
  end

  # Applies patch set to graphdb

  def patch(dirname)
    load(File.join(dirname, "new.csv"))
    changefile = File.join(dirname, "change.csv")
    patch_parents(changefile)
    delete(File.join(dirname, "delete.csv"))
    patch_field(changefile, "taxonRank", "rank")
    patch_field(changefile, "canonicalName", "canonical")
  end

  # Delete nodes.  Maybe check first to see which ones have traits?

  def delete(pages_url)
    puts "Deleting pages listed in #{pages_url}"
    query = "USING PERIODIC COMMIT
             LOAD CSV WITH HEADERS FROM '#{pages_url}'
             AS row
             WITH row,
                  toInteger(row.EOLid) AS page_id
             MATCH (page:Page {page_id: page_id})
             DETACH DELETE page
             RETURN COUNT(page)
             LIMIT 100000000"
    r = run_query(query)
    raise "No deletions found" unless r
    n = r["data"][0][0]
    puts "#{n} deletions performed"
  end

  # Extract hierarchy from graphdb
  # 7/28/2021: should be EOLid,taxonRank,canonicalName,scientificName,Landmark,parentEOLid

  def dump(csv_path)
    pag = Paginator.new(get_graph)
    cql = "MATCH (p:Page)
           OPTIONAL MATCH (p:Page)-[:parent]->(q:Page)
           WITH p.page_id AS EOLid,
                q.page_id as parentEOLid,
                p.rank as taxonRank,
                p.canonical as canonicalName,
                p.landmark as landmark
           RETURN id(p), EOLid, parentEOLid, taxonRank, canonicalName, landmark"
    pag.supervise_query(cql, nil, @chunksize, csv_path)
  end

end
