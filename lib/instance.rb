# The 'system' provides:
#   a local darwin core archive cache, keyed by master id
#   possibly some local files (e.g. config.yml).

# An instance selects:
#   a content repository, used for page id mappings
#   (optional) a publishing site
#   a staging server

# An assembly selects:
#   an instance
#   a graphdb

# A location has:
#   resources (either publishing or harvesting)

require 'system'

class Instance

  def self.instance(tag)
    System.system().get_instance_config(tag)
  end

  def initialize(system, config, tag)
    @system = system
    @config = config or {}
    @name = tag
  end

  def name; @name; end

  def get_location(role)
    if @config.include?(role)
      loc = @system.get_location(@config[role])
      raise "No location in role #{role} called #{loc.name}" unless loc
    else
      loc = @system.get_location(role)
      raise "No #{role} in #{@name}" unless loc
    end
    loc
  end

  def get_opendata_dwca(landing_url, resource_name)
    @system.get_opendata_dwca(landing_url, resource_name)
  end

  def get_resource_by_id(id)
    get_location("publishing").get_resource_by_id(id)
  end

end

