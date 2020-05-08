# The 'system' provides:
#   a source of master ids for resources
#     (production publishing site, with patches from config.yml)
#   a local darwin core archive cache, keyed by master id
#   possibly some local files (e.g. config.yml).

# An assembly selects:
#   a content repository, used for page id mappings
#   (optional) a publishing site, used for UI on graphdb
#   local workspace for converting dwca to graphdb
#     [keyed by name of page id mapping source??]
#   a staging server, populated from the local workspace
#   a graphdb (neo4j triple store)

require 'system'
require 'graph'
require 'open-uri'
require 'net/http'

class Assembly

  def initialize(system, config, tag)
    @system = system
    @config = config or {}
    @assembly_name = tag
  end

  def get_location(role)
    if @config.include?(role)
      loc = @system.get_location(@config[role])
      raise "No location in role #{role} called #{loc.name}" unless loc
    else
      loc = @system.get_location(role)
      raise "No #{role} in #{@assembly_name}" unless loc
    end
    loc
  end

  def get_workspace        # For all purposes
    get_location("workspace").get_path
  end

  def get_graph
    get_location("graphdb").get_graph
  end

  def get_resource(name)
    resource = @system.get_resource(name)
    resource
  end

  def id_for_resource(name)
    @system.id_for_resource(name)
  end

end
