# A system is an assembly of a workspace, a content repository, a
# publishing server, and a staging server.

require 'graph'
require 'open-uri'
require 'net/http'

class System
  class << self
    def system(tag)
      @systems = {} unless @systems
      unless @systems.key?(tag)
        @systems[tag] = System.new(tag)
      end
      @systems[tag]
    end

    def get_from_internet(url, path)
      workspace = File.basename(path)
      # Download the archive file from Internet if it's not
      `rm -rf #{workspace}`
      STDERR.puts "Copying #{url} to #{path}"
      # This is really clumsy... ought to stream it
      open(url, 'rb') do |input|
        File.open(path, 'wb') do |file|
          file.write(input.read)
        end
      end
      raise('Did not download') unless File.exist?(path) && File.size(path).positive?
      File.write(File.join(ws, "url"), url)
      STDERR.puts "... #{File.size(path)} octets"
      path
    end

  end

  def initialize(tag)
    raise("No configuration tag specified (try CONF=test)") unless tag
    @assembly_name = tag
  end

  def get_uber_config
    return @uber_config if @uber_config
    @uber_config = YAML.load(File.read("config/config.yml")) ||
                   raise("No configuration found")
    @uber_config
  end

  def get_assembly_config
    get_uber_config["assemblies"][@assembly_name] ||
      raise("No configuration found: #{@assembly_name}")
  end

  def get_database_config(db_name)
    get_uber_config["databases"][db_name] ||
      raise("No configuration for database #{db_name}")
  end

  def get_uber_workspace        # For all databases
    return get_uber_config["workspace"]
  end

  def get_graph
    return @graph if @graph
    db_name = get_assembly_config["graphdb"] ||
              raise("No graphdb specified in assembly {@assembly}")
    graphdb_config = get_database_config[db_name]
    url = graphdb_config["url"]
    if url
      @graph = Graph.via_neography(url)
    else
      pub_db_name = graphdb_config["via"] ||
                       raise("No API (publishing) site specified")
      pub_server_config = get_database_config[pub_db_name]
      token_path = pub_server_config["update_token_file"] ||
                   pub_server_config["token_file"]
      token = File.read(token_path).strip
      @graph = Graph.via_http(pub_server_config["url"], token)
    end
    @graph
  end

  # Stage root is shared by all servers

  def get_stage_scp_location(resource_id)
    prefix = get_uber_config["staging"]["scp_location"]
    graphdb_name = get_assembly_config["graphdb"]
    # Not easy to create directories on remote machine, so just use
    # hyphenated file names
    loc = "#{prefix}#{graphdb_name}-#{resource_id.to_s}-stage"
    puts "Staging location for scp is #{loc}"
    loc
  end

  def get_stage_url(resource_id)
    prefix = get_uber_config["staging"]["url"]
    graphdb_name = get_assembly_config["graphdb"]
    loc = "#{prefix}#{graphdb_name}-#{resource_id.to_s}-stage"
    puts "Staging URL is #{loc}"
    loc
  end

  # ---------- Link publishing info to repository (harvesting) info

  def get_resources
    return @resources if @resources

    # Get the resource record, if any, from the publishing site's resource list
    publishing_name = get_assembly_config["publishing"]
    publishing_url = get_database_config[publishing_name]["url"]
    @resources = resource_index(publishing_url, "id")
    @resources
  end

  def resource_index(url, key)
    records = JSON.parse(Net::HTTP.get(URI.parse("#{url}/resources.json")))
    records_index = {}
    records["resources"].each do |record|
      value = record["key"].to_i
      if records_index.key?(value)
        STDERR.puts "** Warning: More than one record has value #{value} for {key}"
      end
      records_index[value] = record
    end
    records
  end

  # ---------- Repository methods

  def get_repository_config
    return get_uber_config[get_assembly_config["repository"]]
  end

  def get_url_for_repository
    repository_url = get_repository_config["server"]["url"]
    repository_url += "/" unless repository_url.end_with?("/")
    repository_url
  end

  def get_workspace_for_repository
    get_repository_config["workspace"]["path"]
  end

end
