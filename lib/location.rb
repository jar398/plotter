# Location could be a graphdb instance, publishing instance, repository instance, etc.

class Location

  def initialize(system, config, name)
    @system = system
    @config = config
    @name = name
    @records_by_id = nil
    @resources_by_id = {}
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

  # If this is a graphdb, return the associated publishing instance

  def get_publishing_location
    probe = @config["publishing"]    # Hack for graphdb
    raise "No publishing instance associated with location #{name}" unless probe
    @system.get_location(probe)
  end

  # If this is a publishing instance, return the associated repository instance

  def get_repository_location
    probe = @config["repository"]
    raise "No repository instance associated with location #{name}" unless probe
    @system.get_location(probe)
  end

  # Pub/repo HTTP endpoint only.  For resource lists, page id maps
  # (repo), neo4j proxy (pub).  Also used for staging file name LOAD CSV?

  def get_url
    url = @config["url"]
    raise "No URL set for #{name}" unless url
    url
  end

  # Staging locations - for publishing instances (? think about this).
  # i.e. same staged content can be pushed to multiple graphdbs.
  # A local directory and a remote directory that are synchronized.
  def get_staging_location
    @system.get_location(@config["staging"])
  end
  def get_rsync_location
    assert_repository
    r = @config["rsync_location"]
    raise "No remote rsync location set for #{name}" unless r
    r
  end
  def get_rsync_command
    assert_repository
    c = @config["rsync_command"] || "rsync -va"
    raise "No remote rsync command set for #{name}" unless c
    c
  end

  def assert_repository
    if @config["publishing"] || @config["repository"]
      raise "Attempt to get staging directory for non-repository #{name}"
    end
  end

  # Stored file looks like {"resources":[{"id":830, ...}, ...], ...}
  # Must be a publishing or repository instance
  # Incomplete caching implementation here... should be timeout based
  # Returns a vector of hashes {"id":NNN, ...}

  def load_rails_resource_records(cachep = false)   # Returns an array
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
      records = obj["resources"]
      puts "# Read #{records.length} resource records"
      records
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

  def get_own_resource_records
    return @records_by_id if @records_by_id
    records = load_rails_resource_records
    # records is a vector
    raise "bad records" unless records[0]["id"]
    finish_records(records)    # returns a ... hash? .values ?
  end

  # Parse and cache a graphdb's collection of resource records
  # Array -> nil (for side effects)
  def get_resource_records
    return @records_by_id if @records_by_id
    records = get_publishing_location.get_own_resource_records
    # records is a hash
    finish_records(records.values)
  end

  def finish_records(records)  # records must be a vector
    @records_by_id = {}
    process_records(records)
    configured = @config["resources"]
    process_records(configured) if configured
    puts "#{name}: #{@records_by_id.length} resources"
    @records_by_id
  end

  # Side-affects @records_by_id
  def process_records(records)
    records.each do |r|
      id = r["id"]
      raise "Record #{r["name"]} has no id" unless id
      probe = @records_by_id[id]
      if probe
        r = merge_records(r, probe, id)
      end
      @records_by_id[id] = r
    end
  end

  def merge_records(record, record2, id)
    record = record.merge(record2) do |key, oldval, newval|
      if oldval != newval
        puts "** Conflict over value of property #{key} of resource #{id} in #{name}"
        puts "** Keeping configured = #{oldval}, ignoring instance = #{newval}"
        oldval
      else
        newval
      end
    end
    record if record.size > 0
  end

  # For graphdb
  def get_resource_record_by_id(id)
    get_resource_records
    @records_by_id[id.to_i]
  end

  # Get an id that this particular location will understand
  # ???

  def id_for_resource(name)
    probe = @config["publishing"]    # Hack for graphdb
    if probe
      loc = @system.get_location(probe)
      raise "There is no publishing location #{loc}" unless loc
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

  # For ID= on rake command line... get graphdb resource
  def get_resource_by_id(id)
    record = get_resource_record_by_id(id)
    resource_from_record(record) if record
  end

  # For pub/repo resources
  def get_own_resource_by_id(id)
    get_own_resource_records
    record = @records_by_id[id.to_i]
    resource_from_record(record) if record
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
    end
    res
  end

end
