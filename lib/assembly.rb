# The 'system' provides:
#   a source of master ids for resources
#     (production publishing site, with patches from config.yml)
#   a local darwin core archive cache, keyed by master id
#   possibly some local files (e.g. config.yml).

# An assembly selects:
#   an instance (content + publishing + staging)
#   a graphdb (neo4j triple store)

require 'system'
require 'graph'
require 'open-uri'
require 'net/http'

require 'instance'

class Assembly

  def self.assembly(tag)
    System.system().get_assembly(tag)
  end

  def initialize(system, config, tag)
    @system = system
    @config = config or {}
    @assembly_name = tag
    @instance = system.get_instance(@config["instance"])
  end

  def name; @assembly_name; end

  def get_instance
    @system.get_instance(@config["instance"])
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
    get_instance.get_workspace
  end

  def get_opendata_dwca(landing_url, resource_name)
    @system.get_opendata_dwca(landing_url, resource_name)
  end

  def get_graph(writable = false)
    get_location("graphdb").get_graph(writable)
  end

  # Graphdb and/or publishing id.  They're the same when both exist.
  def graphdb_id_for_resource(name)
    rec = get_resource_record(name)
    return rec["id"] if rec
  end

  # Id in repository database.
  def repo_id_for_resource(name)
    gid = graphdb_id_for_resource(name)
    rec = get_location("publishing").get_resource_record_by_id(id)
    raise "No publishing record with name #{name} in #{@assembly_name} for #{name}" \
      unless rec
    rid = rec["repository_id"]
    raise "No repository record with id #{id} in #{@assembly_name} for #{name}" \
      unless rid
    rid
  end

end
