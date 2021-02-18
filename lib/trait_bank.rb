
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
      @graph = Graph.via_neography(url)
    else
      v3api = @system.get_location(@config["via_api"])
      raise "no api" unless v3api
      @graph = v3api.proxy_graphdb
    end
    @graph
  end

  # Parse and cache a resource's collection of resource records
  # Array -> nil (for side effects)
  def get_resource_records
    return @records_by_id if @records_by_id
    records = get_publishing_location.get_own_resource_records
    # records is a hash
    finish_records(records.values)
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
    path = export_path(rel)
    puts "# Preparing resource table; export path = #{path}"

    table = Table.new(property_vector: [Property.resource_id,
                                        Property.resource_version_id,
                                        Property.label,
                                        Property.comment],
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
    url = system.export(relative_path(table.basename))
    puts("# Staging URL is #{url}")

    # 2. LOAD CSV
    # For column headings see property.rb - these can be changed
    query = "LOAD CSV WITH HEADERS FROM '#{url}'
             AS row
             MERGE (r:Resource {resource_id: row.resource_id})
             SET r.repository_id = row.resource_version_id
             SET r.name = row.label
             SET r.description = row.comment
             RETURN COUNT(r)
             LIMIT 1"
    r = get_graph.run_query(query)
    count = r ? r["data"][0][0] : 0
    STDERR.puts("Merged #{count} resources from #{url}")
  end

end
