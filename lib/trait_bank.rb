
class TraitBank < Location

  # If this is a graphdb, return the associated publishing instance

  def get_publishing_location
    probe = @config["publishing"]    # Hack for graphdb
    raise "No publishing instance associated with location #{name}" unless probe
    @system.get_location(probe)
  end

  def get_graph
    return @graph if @graph

    url = @config["neo4j"]
    if url
      @graph = Graph.via_neo4j_directly(@config["graphdb_name"] || "eol",
                                        url,
                                        @config["neo4j_user"] || "neo4j",
                                        @config["neo4j_password"] || "neo4j")
    else
      v3api = @system.get_location(@config["via_api"])
      raise "no api" unless v3api
      @graph = v3api.proxy_graphdb
    end
    @graph
  end

  # Parse and cache a traitbank's collection of resource records
  # Array -> nil (for side effects)
  def get_resource_records
    unless @resource_records
      records = get_own_resource_records.clone
      get_publishing_location.get_own_resource_records.each do |key, rec|
        records[key] = merge_records(records[key], rec, key)
      end
      @resource_records = records
    end
    @resource_records
  end

  def get_resource(id)
    rec = get_resource_records[id]
    raise "no resource record for #{id}" unless rec
    resource_from_record(rec)
  end

  # ----------------------------------------------------------------------

  # Write resource information found in resources.json to a csv file,
  # then load that csv file into the graphdb

  def sync_resource_nodes
    # 1. Generate resources.csv (compare harvest_table in resource.rb)
    t = prepare_resource_table
    # 2. LOAD CSV into the graphdb
    load_resource_table(t)
  end

  def prepare_resource_table
    fname = "resources.csv"
    rel = relative_path(fname)
    path = workspace_path(rel)
    puts "# Preparing resource table; path = #{path}"
    table = Table.new(header: ["resource_id", "repository_id", "name", "description"],
                      basename: fname,
                      path: path)
    csv_out = table.open_csv_out
    get_resource_records.each do |key, r|
      csv_out << [r["id"].to_s, r["repository_id"].to_s, r["name"], r["description"]]
    end
    csv_out.close
    table
  end

  def load_resource_table(table)
    puts("# Copying resource table from #{table.path} to stage")
    # 1. scp to staging host.
    url = system.stage(relative_path(table.basename))
    puts("# Staging URL is #{url}")

    # 2. LOAD CSV
    # For column headings see property.rb - these can be changed
    query = "LOAD CSV WITH HEADERS FROM '#{url}'
             AS row
             WITH row, toInteger(row.resource_id) as resource_id,
                       toInteger(row.repository_id) as repository_id
             MERGE (r:Resource {resource_id: resource_id})
             SET r.repository_id = repository_id
             SET r.name = row.name
             SET r.description = row.description
             RETURN COUNT(r)
             LIMIT 1"
    r = get_graph.run_query(query)
    count = r ? r["data"][0][0] : 0
    STDERR.puts("Merged #{count} resources from #{url}")
  end

end
