# A system is the topmost description of what's going on.
# Three kinds of things:
#   locations  - places on earth where information can be found
#   resources  - info entities with presences in multiple places
#   assemblies - decisions as to which locations fill roles

# The workspace contains: (ID is always a 'master id')
#   resources/ID/dwca/ID.zip            - web cache  - files loaded from web (esp. DwCAs), keyed by master id
#   resources/ID/dwca/unpacked/foo.tsv  - unpacked dwca area
#   resources/ID/stage/bar.csv          - keyed by master id

require 'assembly'
require 'location'
require 'resource'

# Singleton, I suppose...

class System

  class << self
    # Class variable: @system

    def system(config = nil)
      return @system if @system
      config = YAML.load(File.read("config/config.yml")) unless config
      config = config || raise("No configuration provided")
      @system = System.new(config)
      @system                   # singleton I suppose
    end

    def copy_from_internet(url, path)
      workspace = File.basename(path)
      # Download the archive file from Internet if it's not
      `rm -rf #{workspace}`
      STDERR.puts "Copying #{url} to #{path}"
      # This is really clumsy... ought to stream it, or use curl or wget
      open(url, 'rb') do |input|
        File.open(path, 'wb') do |file|
          file.write(input.read)
        end
      end
      raise('Did not download') unless File.exist?(path) && File.size(path).positive?
      STDERR.puts "... #{File.size(path)} octets"
      path
    end
  end

  def initialize(config)    # config could have been json serialized
    @config = config
    @assemblies = {}
    config["assemblies"].each do |tag, config|
      @assemblies[tag] = Assembly.new(self, config, tag)
    end
    @locations = {}
    config["locations"].each do |tag, config|
      @locations[tag] = Location.new(self, config, tag)
    end
    initialize_resources(config["resources"])
  end

  def get_workspace
    dir = get_location("workspace").get_path
    FileUtils.mkdir_p(dir)
    dir
  end

  def initialize_resources(record_list)
    records = {}   # by name
    record_list.each do |record|
      records[record["name"]] = record
    end
    more = get_location("prod_publishing").get_resource_records
    more.each do |record|
      name = record["name"]
      if records.key?(name)
        records[name].merge!(record) do |key, oldval, newval|
          puts "** Value conflict (#{oldval}->#{newval}) for key #{key}" \
            unless oldval == newval
          oldval
        end
      else
        records[name] = record
      end
    end

    @resources = {}  # by name
    @resources_by_id = {}
    records.each do |name, record|
      res = Resource.new(self, record)
      @resources[record["name"]] = res
      id = record["id"]
      @resources_by_id[id] = res if id
    end
  end

  def get_assembly(tag)
    a = @assemblies[tag]
    raise "No such assembly: #{tag}" unless a
    a
  end

  def get_location(tag)
    @locations[tag]
  end

  def get_resource(name)
    unless @resources.include?(name)
      @resources[name] = Resource.new(self, {"name" => name})
    end
    @resources[name]
  end

  # Resource from master resource id (usu. production publishing site)

  def get_resource_from_id(id)
    return @resources_by_id[id] if @resources_by_id.include?(id)
    rec = get_location("prod_publishing").get_resource_record_by_id(id)
    @resources[rec["name"]] = Resource.new(self, rec)
  end
end
