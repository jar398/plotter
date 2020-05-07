
class Location

  def initialize(system, config, tag)
    @system = system
    @config = config
    @name = tag
  end

  def name; @name; end

  def get_graph
    return @graph if @graph

    url = get_url
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
    Graph.via_http(get_url, token)
  end

  # for workspace and concordance...
  def get_path
    return @config["path"]
  end

  # for workspace, page ids, etc
  def get_url
    return @config["url"]
  end

  def get_scp_specifier; @config["scp_location"]; end

  def load_config(path)
    if path.end_with?(".yml")
      hash = YAML.load(File.read(path))
    elsif path.end_with?(".json")
      hash = JSON.parse(File.read(path))
    else
      puts "** No resource records at #{path}"
      return {}
    end
    hash
  end

  def get_resource_records   # Returns an array
    return @records if @records
    if @config.include?("resource_records")
      path = @config["resource_records"]
      hash = load_config(path)
    elsif @config.include?("url")
      url = "#{get_url}resources.json?per_page=10000"
      puts "GET #{url}"
      blob = Net::HTTP.get(URI.parse(url))
      hash = JSON.parse(blob)
    else
      hash = load_config(location.get_path)
    end
    @records_by_name = {}
    @records_by_id = {}
    hash["resources"].each do |r|
      name = r["name"]
      id = r["id"]
      if @records_by_name.include?(name)
        other_id = @records_by_name[name]["id"]
        r = nil if other_id > id
      end
      if r
        puts "got 40" if id == 40
        @records_by_id[id] = r
        @records_by_name[name] = r
      end
    end
    @records = @records_by_name.values
    @records
  end

  def get_resource_record(name)
    get_resource_records
    @records_by_name[name]
  end

  def get_resource_record_by_id(id)
    puts "looking for #{id}"
    get_resource_records
    puts "here are some #{@records_by_id.keys[0..5]}"
    rec = @records_by_id[id]
    puts "got #{rec['name']}"
    rec
  end

  # Get an id that this particular location will understand

  def id_for_resource(name)
    probe = @config["ids_from"]
    if probe
      @system.get_location(probe).id_for_resource(name)
    else
      raise "no id for this resource" unless get_resource_record(name)["id"]
    end
  end

end
