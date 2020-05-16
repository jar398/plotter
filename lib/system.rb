# Resource in the sense of repository resource *snapshot* (particular
# version with respect to page id mappings and so on)

# A system is the topmost description of what's going on.
# Three kinds of things:
#   locations  - places on earth where information can be found
#   resources  - info entities with presences in multiple places
#   assemblies - decisions as to which locations fill roles

require 'assembly'
require 'location'
require 'resource'
require 'nokogiri'

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
    config["locations"].each do |loc_tag, config|
      @locations[loc_tag] = Location.new(self, config, loc_tag)
    end
    @dwcas = {}
    @resource_records = {}
    @resource_records_by_id = {}
    config["resources"].each do |record|
      @resource_records[record["name"]] = record
      if record.key?("id")
        @resource_records_by_id[record["id"]] = record
      end
    end
  end

  def get_workspace
    dir = get_location("workspace").get_path
    FileUtils.mkdir_p(dir)
    dir
  end

  def get_assembly(tag)
    a = @assemblies[tag]
    raise "No such assembly: #{tag}" unless a
    a
  end

  def get_location(tag)
    @locations[tag]
  end

  # Specially configured resources from config.yml
  def get_resource_record(name)
    if @resource_records.include?(name)
      @resource_records[name]
    end
  end

  # Specially configured resources from config.yml
  def get_resource_record_by_id(id)
    if @resource_records_by_id.include?(id)
      @resource_records_by_id[id]
    end
  end

@resource_records_by_id

  # If you don't have an opendata landing page (which provides a
  # uuid), just generate a random number (in hex).
  def get_dwca(url, landing_page, resource_name)
    id = landing_page[-8..]     # low order 8 bits of landing page uuid
    return @dwcas[id] if @dwcas[id]
    dir = File.join(get_workspace, "dwca", id)
    FileUtils.mkdir_p(dir)
    dwca = Dwca.new(dir,
                    url,
                    {"url": url,
                     "landing_page": landing_page,
                     "resource_name": name,
                     "id": id})
    @dwcas[id] = dwca
    dwca
  end

  def get_opendata_dwca(opendata_url, resource_name)
    dwca_url = get_dwca_url(opendata_url)
    get_dwca(dwca_url, opendata_url, resource_name)
  end

  # Adapted from harvester app/models/resource/from_open_data.rb.
  # The HTML file is small; no need to cache it.
  def get_dwca_url(opendata_url)
    begin
      raw = open(opendata_url)
    rescue Net::ReadTimeout => e
      fail_with(e)
    end
    fail_with(Exception.new('GET of URL returned empty result.')) if raw.nil?
    html = Nokogiri::HTML(raw)
    html.css('p.muted a').first['href']
  end

end
