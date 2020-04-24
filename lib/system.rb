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

  def get_graph
    return @graph if @graph
    method = get_config["cypher"]["method"]
    if method == "eol_api"   # Use EOL web site v3 API

      eol_api_url = get_config["api"]["url"]
      raise("No API (publishing) site specified") unless eol_api_url

      token_path = get_config["api"]["update_token_file"]
      raise("No token file specified") unless token_path
      token = File.read(token_path).strip

      @graph = Graph.via_http(eol_api_url, token)
    elsif method == "neography"     # Talk to neo4j directly
      @graph = Graph.via_neography(get_config["neography"]["url"])
    else
      raise "unrecognized cypher method #{method}"
    end
    @graph
  end

  def get_workspace_root        # For all resources
    return @workspace_root if @workspace_root
    @workspace_root = get_config["workspace"]["path"]
    @workspace_root
  end

  def get_repository_url
    @repository_url = get_config["repository"]["url"]
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
