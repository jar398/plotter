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
  def system; @system; end

  # ----------

  # Path to root of tree containing various artifacts, relative to
  # workspace root, export root, or staging area root
  def relative_path(basename)
    File.join(name, basename)
  end

  # Trampolines
  def workspace_path(relative)
    raise "#{relative} not in #{name}" unless relative.start_with?(name)
    system.workspace_path(relative)
  end
  def export_path(relative)
    raise "#{relative} not in #{name}" unless relative.start_with?(name)
    system.export_path(relative)
  end

  # ----------

  # If this is a publishing instance, return the associated repository instance

  def get_repository_location
    probe = @config["repository"]
    raise "No repository instance associated with location #{name}" unless probe
    system.get_location(probe)
  end

  # For workspace and concordance... ?  non-traitbank locations
  def get_path
    path = @config["path"]
    raise "No path defined for location #{name}" unless path
    path
  end

  # Pub/repo HTTP endpoint.  For resource lists, page id maps
  # (repo), neo4j proxy (pub).  Also used for staging file name LOAD CSV?

  def get_url
    url = @config["url"]
    raise "No URL set for #{name}" unless url
    url
  end

  # see system.rb

  def get_rsync_specifier
    r = @config["rsync_specifier"]
    raise "No remote rsync specifier set for #{name}" unless r
    r
  end
  def get_rsync_command
    c = @config["rsync_command"] || "rsync -va"
    raise "No remote rsync command set for #{name}" unless c
    c
  end

  # ----------------------------------------------------------------------

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
      # when_cached = workspace_path("resource_records.csv")
      when_cached = @config["resource_records"]    # maybe nil
      puts "# Resource records when cached would be at #{when_cached}"
      unless File.exists?(when_cached)
        url = "#{get_url}resources.json?per_page=100000"
        puts "# Reading #{url}"
        System.copy_from_internet(url, when_cached)
      end
      obj = System.load_json(when_cached)
    else
      url = "#{get_url}resources.json?per_page=100000"
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

  def finish_records(records)  # records must be a vector
    @records_by_id = {}
    process_records(records)
    configured = @config["resources"]
    process_records(configured) if configured
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

  # This is overridden in trait_bank
  def get_resource_records
    get_own_resource_records
  end

  # For graphdb... and ... other kinds of locations too?
  # Overridden in trait_bank.rb
  def get_resource_record_by_id(id)
    get_resource_records
    @records_by_id[id.to_i]
  end

  # For ID= on rake command line... get graphdb resource
  def get_resource_by_id(id)
    record = get_resource_record_by_id(id)
    resource_from_record(record) if record
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

  # Invoke this on a publishing instance
  def proxy_graphdb
    token_path = @config["update_token_file"] ||
                 @config["token_file"]
    token = File.read(token_path).strip
    puts "# Graphdb proxy URL is #{get_url}"
    Graph.via_http(get_url, token)
  end

end
