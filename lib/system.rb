# Resource in the sense of repository resource *snapshot* (particular
# version with respect to page id mappings and so on)

# A system is the topmost description of what's going on.
# Three kinds of things:
#   locations  - places on earth where information can be found
#   resources  - info entities with presences in multiple locations
#   instances  - publishing + repository locations
#   assemblies - pairing of an instance with a graphdb

require 'yaml'
require 'nokogiri'
require 'location'
require 'trait_bank'
require 'resource'

# Singleton, I suppose...

class System

  class << self
    # Class variable: @system

    def system(config = nil)
      return @system if @system
      config = YAML.load(File.read("config/config.yml")) unless config
      raise("No configuration provided") unless config
      @system = System.new(config)
      @system                   # singleton I suppose
    end

    # Utility to copy a single file from WWW to file system
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

    # Load YAML or JSON from the web or from a local file
    def load_json(specifier)    # file name or URL
      # First get the literal string
      if specifier.is_a?(URI)
        puts "# GET #{url}"
        strng = Net::HTTP.get(url)
      elsif is_url?(specifier)
        url = specifier
        puts "# Get #{url}"
        strng = Net::HTTP.get(URI.parse(url))
        # TBD: check whether success return.
      else
        strng = File.read(specifier)
      end
      # Now parse the string to get JSON-like data
      if specifier.include?(".yml")
        YAML.parse(strng)
      elsif specifier.include?(".json")    # This case might not be needed
        JSON.parse(strng)
      else
        puts "Specifier is not .yaml or .json: #{specifier}"
        nil
      end
    end

    def is_url?(specifier)
      specifier.include?("://")
    end
  end

  def initialize(config)    # config could have been json serialized
    @config = config
    @locations = {}
    raise "No locations configured" unless config["locations"]
    config["locations"].each do |loc_tag, config|
      # Kludge!  There has to be a better way to determine this
      if config.include?("neo4j") || config.include?("via_api")
        @locations[loc_tag] = TraitBank.new(self, config, loc_tag)
      else
        @locations[loc_tag] = Location.new(self, config, loc_tag)
      end
    end
    @dwcas = {}
  end

  def get_workspace_root
    dir = get_location("workspace").get_path
    FileUtils.mkdir_p(dir)
    dir
  end

  def workspace_path(relative)
    p = File.join(get_workspace_root, relative)
    FileUtils.mkdir_p(File.dirname(p))
    p
  end

  def staging_url(relative)
    "#{get_staging_location.get_url}/#{relative}"
  end

  def get_staging_location; get_location("staging"); end

  def get_trait_bank(tag)
    raise("No trait bank name provided") unless tag
    loc = get_location(tag)
    raise "No such trait bank: #{tag}" unless loc
    return loc
  end

  # Returns nil if no such location ... things depend on this I think
  def get_location(tag)
    @locations[tag]
  end

  # ---------- DwCA stuff

  # If you don't have an opendata landing page (which provides a
  # uuid), just generate a random number (in hex).
  def get_dwca(url, landing_page, resource_name = nil)
    id = landing_page[-8..]     # low order 8 bits of landing page uuid
    return @dwcas[id] if @dwcas[id]
    rel = File.join("dwca", id)
    dir = workspace_path(rel)
    FileUtils.mkdir_p(dir)
    dwca = Dwca.new(dir,
                    url,
                    {"url": url,
                     "landing_page": landing_page,
                     "resource_name": resource_name,
                     "id": id})
    @dwcas[id] = dwca
    dwca
  end

  def get_opendata_dwca(opendata_lp_url, resource_name = nil)
    dwca_url = get_dwca_url(opendata_lp_url)
    get_dwca(dwca_url, opendata_lp_url, resource_name)
  end

  # Adapted from harvester app/models/resource/from_open_data.rb.
  # The HTML file is small; no need to cache it.
  def get_dwca_url(opendata_lp_url)
    raise "No DwCA landing page URL provided" unless opendata_lp_url
    begin
      raw = open(opendata_lp_url)
    rescue Net::ReadTimeout => e
      fail_with(e)
    end
    fail_with(Exception.new('GET of URL returned empty result.')) if raw.nil?
    html = Nokogiri::HTML(raw)
    html.css('p.muted a').first['href']
  end

  # ----------------------------------------------------------------------
  # Stage: copy file(s) from workspace to staging server.

  # Relative is a relative path to be joined to the stage url.
  # It should not end in a '/'.

  def stage(relative)
    raise "Relative path ends in /" if relative.end_with?("/")
    local = workspace_path(relative) # /.../test/relative
    prepare_manifests(local)

    spec    = get_staging_location.get_rsync_specifier
    command = get_staging_location.get_rsync_command
    #remote = "#{spec}/#{relative}"
    #line = "#{command} \"#{local}\" \"#{remote}\""
    line = "#{command} \"#{get_workspace_root}/./#{relative}\" \"#{spec}/\""
    STDERR.puts("# #{line}")
    stdout_string, status = Open3.capture2("#{line}")
    STDERR.puts("# Status: [#{status}] stdout: [#{stdout_string}]")
    raise "rsync command yielded nonzero status #{status}" unless status == 0

    # Return URL for subsequent access via LOAD CSV
    staging_url(relative)
  end

  # For each directory in a tree, write a .manifest.json that lists the
  # files in the directory.  This makes the tree traversable by a
  # web client.

  def prepare_manifests(path)
    if File.directory?(path)
      # Prepare one manifest
      names = Dir.glob("*", base: path)
      names = names.select{|name| not name.start_with?(".")}
      # tbd: filter out dotfiles
      if path.end_with?(".chunks")
        man = File.join(path, ".manifest.json")
        STDERR.puts "Writing #{man}"
        File.write(man, JSON.generate(names))
      end
      # Recur
      names.each {|name| prepare_manifests(File.join(path, name))}
    end
  end

  def read_manifest(url)
    base_url = url + ".chunks/"
    response = Net::HTTP.get_response(URI(base_url + ".manifest.json"))
    if response.kind_of? Net::HTTPSuccess
      names = JSON.parse(response.body)
      puts "#{names.size} chunks"
      names.collect{|name| base_url + name}
    else
      STDERR.puts "No manifest for #{base_url}"
      nil
    end
  end

end
