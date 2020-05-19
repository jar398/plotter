
class Location

  def initialize(system, config, name)
    @system = system
    @config = config
    @name = name
  end

  def name; @name; end

  def get_graph
    return @graph if @graph

    url = @config["neo4j"]
    if url
      puts "# Neo4j URL is #{url}"
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

  # for workspace and concordance...
  def get_path
    return @config["path"]
  end

  # for workspace, page ids, staging, etc
  def get_url
    return @config["url"]
  end

  def get_scp_specifier; @config["scp_location"]; end

  # Stored file looks like {"resources":[{"id":830, ...}, ...], ...}
  def load_resource_records(cachep = false)   # Returns an array
    if @config.key?("resource_records")
      when_cached = @config["resource_records"]    # maybe nil
      unless File.exists?(when_cached)
        url = "#{get_url}resources.json?per_page=10000"
        System.copy_from_internet(url, when_cached)
      end
      obj = System.load_json(when_cached)
    else
      url = "#{get_url}resources.json?per_page=10000"
      obj = System.load_json(url)
    end
    resources = obj["resources"]
    puts "Read #{resources.length} resource records"
    resources
  end

  def flush_resource_records_cache
    if @config.key?("resource_records")
      path = @config["resource_records"]
      puts "# Attempting deletion of #{path}"
      FileUtils.rm_rf(path)
    end
  end

  # Array -> nil (for side effects)
  def get_resource_records
    return if @records_by_name
    records = load_resource_records
    @records_by_name = {}
    @records_by_id = {}
    records.each do |r|
      id = r["id"]
      # puts "# saw a resource with id 40" if id == 40
      name = r["name"]
      if @records_by_name.include?(name)
        # Collision.  Keep the one with higher id.
        # OK for repository, not OK for publishing.
        other_id = @records_by_name[name]["id"]
        r = nil if other_id > id
      end
      if r
        @records_by_id[id] = r
        @records_by_name[name] = r
      end
    end
    puts "#{@records_by_id.length} resource ids, #{@records_by_name.length} resource names"
  end

  def get_resource_record(name)
    get_resource_records
    @records_by_name[name]
  end

  def get_resource_record_by_id(id)
    id = id.to_i
    get_resource_records
    rec = @records_by_id[id]
    puts "No resource record at #{@name} with id #{id}" unless rec
    # Don't raise exception
    rec
  end

  # Get an id that this particular location will understand

  def id_for_resource(name)
    probe = @config["ids_from"]    # Hack for graphdb
    if probe
      loc = @system.get_location(probe)
      raise "There is no ids_from location #{loc}" unless loc
      id = loc.id_for_resource(name)
      puts "There is no id for #{name} at #{loc}" unless id
      id
    else
      rec = get_resource_record(name)
      if rec
        rec["id"]
      else
        puts "There is no resource record for #{id} at #{loc}"
      end
    end
  end

end
