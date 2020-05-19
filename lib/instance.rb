# The 'system' provides:
#   a source of master ids for resources
#     (production publishing site, with patches from config.yml)
#   a local darwin core archive cache, keyed by master id
#   possibly some local files (e.g. config.yml).

# An instance selects:
#   a content repository, used for page id mappings
#   (optional) a publishing site
#   a staging server

# An assembly selects:
#   an instance
#   a graphdb

require 'system'

class Instance

  def self.instance(tag)
    System.system().get_instance(tag)
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

  def get_workspace        # For all purposes
    File.join(@system.get_workspace, @name)
  end

  def get_opendata_dwca(landing_url, resource_name)
    @system.get_opendata_dwca(landing_url, resource_name)
  end

  def get_staging_location    # ????
    get_location("staging")
  end

  def get_resource(name)
    record = get_resource_record(name)
    if record
      resource_from_record(record)
    end
  end

  # For ID= on rake command line...
  def get_resource_by_id(id)
    get_resource(id_to_name(id))
  end

  def id_to_name(id)
    rec = @system.get_resource_record_by_id(id)
    rec ||= get_location("publishing").get_resource_record_by_id(id)
    if rec
      rec["name"]
    else
      raise "** No resource with id #{id} in #{@name}."
    end
  end

  # apparently this isn't used anywhere?
  def get_resource_record(name)
    record = @system.get_resource_record(name) || {}
    loc = get_location("publishing")
    record2 = loc.get_resource_record(name) || {}
    record = record.merge(record2) do |key, oldval, newval|
      if oldval != newval
        puts "** Conflict over value of resource property #{key}."
        puts "** Keeping configured = #{oldval}, ignoring publishing = #{newval}."
        oldval
      else
        newval
      end
    end
  end

  def resource_from_record(record)
    pid = record["id"]
    # TBD: generate random publishing id if none found in config...
    rid = record["repository_id"]
    suffix = (rid ? ".#{rid}" : "")
    qid = "#{@name}.#{pid}suffix"
    record["qualified_id"] = qid
    # cache it?
    resource = Resource.new(self, record)
    resource
  end

end

