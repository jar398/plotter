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

  def self.assembly(tag)
    System.system().get_assembly(tag)
  end

  def initialize(system, config, tag)
    @system = system
    @config = config or {}
    @assembly_name = tag
    @instance_name = tag
  end

  def name; @assembly_name; end

  def instance_name
    # FRAGILE KLUDGE, fix by establishing instances as first-class
    @config["publishing"].split("_")[0]
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
    @system.get_workspace
  end

  def get_opendata_dwca(landing_url, resource_name)
    @system.get_opendata_dwca(landing_url, resource_name)
  end

  def get_staging_location    # ????
    get_location("staging")
  end

  def get_graph
    get_location("graphdb").get_graph
  end

  def get_resource(name)
    record = get_resource_record(name)
    if record
      resource_from_record(record)
    end
  end

  # apparently this isn't used anywhere?
  def get_resource_record(name)
    record = @system.get_resource_record(name) || {}
    loc = get_location("publishing")
    record2 = loc.get_resource_record(name) || {}
    puts "# sys: #{record["name"]} pub: #{record2["id"]}"
    record = record.merge(record2) do |key, oldval, newval|
      if oldval != newval
        puts "** Conflict over value of resource property #{key}"
        puts "** Old = #{oldval}, new = #{newval}.  Keeping old"
        oldval
      else
        newval
      end
    end
    unless record.key?("name")
      # This is assemblyish code, not instance code (an instance
      # has no graph)... deal
      gid = get_graph.resource_id_from_name(name)
      {"name" => name,
       "id" => gid,
       "instance" => @assembly_name}
    end
  end

  def resource_from_record(record)
    pid = record["id"]
    # TBD: generate random publishing id if none found in config...
    rid = record["repository_id"]
    suffix = (rid ? ".#{rid}" : "")
    qid = "#{@instance_name}.#{pid}suffix"
    record["qualified_id"] = qid
    resource = Resource.new(self, record)
    resource
  end

  # For ID= on rake command line...
  def get_resource_by_id(id)
    rec = get_location("publishing").get_resource_record_by_id(id)
    rec ||= @system.get_resource_record_by_id(id)
    if rec
      resource_from_record(rec)
    else
      # TBD: Query a graphdb by id to get name...
      raise "** No resource with publishing id #{id} in #{@assembly_name}."\
            unless record
    end
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
