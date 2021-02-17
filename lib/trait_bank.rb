
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

  def proxy_graphdb
    token_path = @config["update_token_file"] ||
                 @config["token_file"]
    token = File.read(token_path).strip
    puts "# Graphdb proxy URL is #{get_url}"
    Graph.via_http(get_url, token)
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
    dir = get_export_dir
    FileUtils.mkdir_p(dir)
    fname = "resources.csv"
    table = Table.new(property_vector: [Property.resource_id,
                                        Property.label,
                                        Property.resource_version_id],
                      location: fname,
                      path: File.join(dir, fname))
    csv_out = table.open_csv_out
    get_resource_records.each do |key, r|
      csv_out << [r["id"].to_s, r["name"]]
    end
    csv_out.close
    table
  end

  def load_resource_table(table)
    puts("# Table is supposed to be at #{table.path}")
    # 1. scp to staging host.
    copy_to_stage("resources.csv")

    # 2. LOAD CSV
    url = staging_url("resources.csv")
    puts("# URL is #{url}")
  end

end
