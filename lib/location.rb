
class Location

  def initialize(system, config, name)
    @system = system
    @config = config
    @name = name
    @resources_by_id = {}
    @resources_by_name = {}
  end

  def name; @name; end

  def get_workspace        # For all purposes
    File.join(@system.get_workspace, @name)
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

  # for workspace and concordance...
  def get_path
    return @config["path"]
  end

  def get_repository_location
    @system.get_location(@config["repository"])
  end

  # for workspace, page ids, staging, etc
  def get_url
    return @config["url"]
  end

  def get_staging_location
    @system.get_location(@config["staging"])
  end
  def get_rsync_location
    @config["rsync_location"]
  end
  def get_rsync_command
    @config["rsync_command"] || "rsync -va"
  end

  # Stored file looks like {"resources":[{"id":830, ...}, ...], ...}
  def load_resource_records(cachep = false)   # Returns an array
    if @config.key?("resource_records")
      # Ideally this would be cached in the instance workspace
      when_cached = @config["resource_records"]    # maybe nil
      puts "# Resource records when cached would be at #{when_cached}"
      unless File.exists?(when_cached)
        url = "#{get_url}resources.json?per_page=10000"
        puts "# Reading #{url}"
        System.copy_from_internet(url, when_cached)
      end
      obj = System.load_json(when_cached)
    else
      url = "#{get_url}resources.json?per_page=10000"
      obj = System.load_json(url)
    end
    if obj.key?("resources")
      resources = obj["resources"]
      puts "# Read #{resources.length} resource records"
      resources
    else
      puts "** No resource records"
      []
    end
  end

  def flush_resource_records_cache
    if @config.key?("resource_records")
      path = @config["resource_records"]
      puts "# Attempting deletion of #{path}"
      FileUtils.rm_rf(path)
    end
  end

  # Parse and cache the collection of resource records
  # Array -> nil (for side effects)
  def get_resource_records
    return if @records_by_name
    records = load_resource_records
    @records_by_name = {}
    @records_by_id = {}
    records.each do |r|
      id = r["id"]
      @records_by_id[id] = r

      # Store the highest-id record under the name
      name = r["name"]
      if @records_by_name.include?(name)
        # Collision (repository only).  Keep the one with higher id.
        other_id = @records_by_name[name]["id"]
        r = nil if other_id > id
      end
      if r
        @records_by_name[name] = r
      end

    end
    puts "#{@records_by_id.length} resource ids, #{@records_by_name.length} resource names"
  end

  def get_resource_record_by_id(id)
    get_resource_records
    id = id.to_i
    # Don't raise exception...
    merge_records(@system.get_resource_record_by_id(id) || {},
                  @records_by_id[id] || {})
  end

  # apparently this isn't used anywhere?
  def get_resource_record(name)
    merge_records(@system.get_resource_record(name) || {},
                  @records_by_name[name] || {})
  end

  def merge_records(record, record2)
    record = record.merge(record2) do |key, oldval, newval|
      if oldval != newval
        puts "** Conflict over value of resource property #{key}."
        puts "** Keeping configured = #{oldval}, ignoring publishing = #{newval}."
        oldval
      else
        newval
      end
    end
    record || nil
  end


  # Get an id that this particular location will understand
  # ???

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

  # Moved out of instance.rb

  def get_resource(name)
    record = get_resource_record(name)
    if record
      resource_from_record(record)
    end
  end

  # For ID= on rake command line...
  def get_resource_by_id(id)
    record = get_resource_record_by_id(id)
    if record
      resource_from_record(record)
    end
  end

  def id_to_name(id)
    rec = get_resource_record_by_id(id)
    raise "** No resource with id #{id} in #{@name}." unless rec
    rec["name"]
  end

  def resource_from_record(record)
    id = record["id"]
    res = @resources_by_id[id]
    unless res
      res = Resource.new(record, self)
      @resources_by_id[id] = res
      @resources_by_name[res.name] = res
    end
    res
  end

end
