# A system is an assembly of a workspace, a content repository, a
# publishing server, and a staging server.

require 'graph'

class System
  class << self
    def system(tag)
      @systems = {} unless @systems
      unless @systems.key?(tag)
        @systems[tag] = System.new(tag)
      end
      @systems[tag]
    end
  end

  def initialize(tag)
    raise("No configuration tag specified (try CONF=test)") unless tag
    @tag = tag
  end

  def get_config
    return @config if @config
    @config = YAML.load(File.read("config/config.yml"))[@tag]
    raise("No configuration found with tag #{@tag}") unless @config
    @config
  end

  def get_publishing_url
    publishing_url ||= get_config["publishing"]["url"]
    publishing_url += "/" unless publishing_url.end_with?("/")
    publishing_url
  end

  def get_publishing_token
    return @publishing_token if @publishing_token
    path = get_config["publishing"]["update_token_file"]
    raise("No token file specified") unless path
    @publishing_token = File.read(path).strip
    @publishing_token
  end

  def get_graph
    return @graph if @graph
    query_fn = Graph.via_http(get_publishing_url, get_publishing_token)
    @graph = Graph.new(query_fn)
    @graph
  end

  def get_workspace_root        # For all resources
    return @workspace_root if @workspace_root
    @workspace_root = get_config["workspace"]["path"]
    @workspace_root
  end

  def get_repository_url
    @repository_url ||= get_config["repository"]["url"]
    @repository_url += "/" unless @repository_url.end_with?("/")
    @repository_url
  end

  def get_stage_scp
    if @stage_scp
      @stage_scp += "/" unless @stage_scp.end_with?("/")
    else
      @stage_scp = get_config["stage"]["scp"]
    end
    @stage_scp
  end

  def get_stage_url
    if @stage_url
      @stage_url += "/" unless @stage_url.end_with?("/")
    else
      @stage_url = get_config["stage"]["url"]
    end
    @stage_url
  end

end
