# A system is an assembly of a workspace, a content repository, a
# publishing server, and a staging server.

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

end
