# Workspace structure:
#   ID/ID.tgz or ID/ID.zip  - downloaded archive file
#   ID/unpack/              - transient area used for unpacking
#   ID/archive/             - directory holding archive files
#   ID/archive/meta.xml     - for example
#   ID/normalized/          - result of transduction

require 'csv'
require 'net/http'
require 'fileutils'
require 'nokogiri'
require 'yaml'
require 'json'

require_relative 'term'
require_relative 'table'

class Resource

  def initialize(workspace_root = nil)
    @workspace_root = workspace_root || File.join(ENV['HOME'], ".reaper_workspace")
    @config = nil
  end

  def get_config
    @config ||= YAML.load(File.read("config/secrets.yml"))
    @config
  end

  def bind_to_stage(stage_url, stage_scp = nil)
    @stage_scp = stage_scp || "varela:public_html/tmp/"
    @stage_url = stage_url || "http://varela.csail.mit.edu/~jar/tmp/"
    @stage_scp += "/" unless @stage_scp.end_with?("/")
    @stage_url += "/" unless @stage_url.end_with?("/")
  end

  def stage_scp; @stage_scp; end
  def stage_url; @stage_url; end

  def bind_to_publishing(publishing_url, publishing_id = nil, token)
    publishing_url ||= get_config["development"]["host"]["url"]
    publishing_url += "/" unless publishing_url.end_with?("/")
    @publishing_url = publishing_url
    @publishing_id = publishing_id.to_i if publishing_id
    @workspace = File.join(@workspace_root, @publishing_id.to_s)

    if token
      query_fn = Graph.via_http(publishing_url, token)
      @graph = Graph.new(query_fn)
    end

    puts "Publishing site is #{@publishing_url}, id is #{publishing_id || '?'}"
  end

  def graph
    raise("No graph, probably because no token") unless @graph
    @graph
  end

  def publishing_id; @publishing_id; end
  def publishing_url; @publishing_url; end

  def bind_to_repository(repository_url, repository_id = nil)
    repository_url ||= get_config["development"]["repository"]["url"]
    repository_url += "/" unless repository_url.end_with?("/")

    if repository_id
      name = "?"
    else
      record = get_record(@publishing_id)
      if record
        name = record["name"]    
        repository_id = record["repository_id"]
      else
        raise("Unknown resource #{repository_id}; REPOSITORY_ID must be specified") unless repository_id
      end
    end
    puts("Resource #{@publishing_id}/#{repository_id || "?"} = #{name}")

    @repository_id = repository_id.to_i if repository_id
    @repository_url = repository_url
    puts "Repository site is #{repository_url}, id is @{repository_id || '?'}"
  end

  def get_record(publishing_id)
    # Get the resource record, if any, from the publisher's resource list
    records = JSON.parse(Net::HTTP.get(URI.parse("#{@publishing_url}/resources.json")))
    records_index = {}
    records["resources"].each do |record|
      id = record["id"].to_i
      records_index[id] = record
    end
    records_index[publishing_id]
  end

  # Record a decision as to which opendata resource this resource
  # should be associated with.
  def bind_to_opendata(url)
    @archive_url = url
  end

  def map_to_page_id(tnu_id)
    @page_id_map[tnu_id]
  end

  def workspace_dir(dir_name)
    dir = File.join(@workspace, dir_name)
    unless Dir.exist?(dir)
      puts "Creating directory #{dir}"
      FileUtils.mkdir_p(dir) 
    end
    dir
  end

  def archive_path(name)
    File.join(workspace_dir("archive"), name)
  end

  def publish_path(name)
    File.join(workspace_dir("publish"), name)
  end

  # Similar to ResourceHarvester.new(self).start
  #  in app/models/resource_harvester.rb

  def harvest
    parse_manifest
    get_page_id_map

    vt = @tables[Term.vernacular_name]      # a Table
    raise("Cannot find vernaculars table (I see #{@tables.keys})") unless vt
    vt.harvest_vernaculars(nil, nil)

    # similarly for other types... maybe particular types should be selectable...

  end

  # Copy the publish/ directory out to the staging host
  def stage(dir_name = "publish")
    local_publish_path = File.join(@workspace, dir_name)
    Dir.glob("*.chunks", base: local_publish_path).each do |chunks_dir_name|
      local_chunks_path = File.join(local_publish_path, chunks_dir_name)
      # Write the chunks list to a file for benefit of publishing
      man = File.join(local_chunks_path, "manifest.json")
      puts "Writing #{man}"
      File.write(man, JSON.generate(Dir.glob("*.csv", base: local_chunks_path)))
    end
    stage_publish_path = "#{stage_scp}#{publishing_id.to_s}-#{dir_name}"
    STDERR.puts("Copying #{local_publish_path} to #{stage_publish_path}")
    stdout_string, status = Open3.capture2("rsync -va #{local_publish_path}/ #{stage_publish_path}/")
    puts "Status: [#{status}] stdout: [#{stdout_string}]"
  end


  # Similar to eol_website app/models/trait_bank/slurp.rb
  def publish
    #   Use two LOAD CSV commands to move information from the
    #   table to the graphdb.
    #   . Create new graphdb nodes as needed.
    #   . Create relationships as needed.
    #   Similar code, but very complicated: build_nodes in slurp.rb.
    # LOAD CSV WITH HEADERS FROM '#{url}' AS row ...
    # Need to do something just like what painter.rb does: chunk the
    # csv file, etc.
    publish_vernaculars
  end

  def publish_vernaculars
    nil
  end

  # Adapted from harvester app/models/resource/from_open_data.rb parse
  def parse_manifest
    html = noko_parse(@archive_url)
    archive_url = html.css('p.muted a').first['href']
    file = load_archive_if_needed(archive_url)
    dest = workspace_dir("archive")
    unpack_file(file, dest)
    from_xml(File.join(dest, "meta.xml"))
  end

  # Get the information that we'll need out of the meta.xml file
  # Returns a hash from term URIs to table elements
  # Adapted from harvester app/models/resource/from_meta_xml.rb self.analyze
  def from_xml(filename)
    doc = File.open(filename) { |f| Nokogiri::XML(f) }
    table_element = doc.css('archive table')
    @table_configs = {}
    @tables = {}
    table_element.each do |table_element|
      row_type = table_element['rowType']
      if @table_configs.key?(row_type)
        STDERR.put("Not yet implemented: multiple files for same row type #{row_type}")
      else
        @table_configs[row_type] = table_element
        @tables[row_type] =
          Table.new(archive_path(table_element.css("location").first.text),
                    table_element['fieldsTerminatedBy'].gsub("\\t", "\t"),
                    table_element['ignoreHeaderLines'].to_i,
                    parse_fields(table_element),
                    self)
      end
    end
  end

  def parse_fields(table_element)
    fields_for_this_table = {}
    table_element.css('field').each do |field|
      i = field['index'].to_i
      key = field['term']
      fields_for_this_table[key] = i
    end
    fields_for_this_table
  end

  # Adapted from harvester app/models/drop_dir.rb
  # File is either something.tgz or something.zip
  def unpack_file(file, dest)
    temp = File.join(File.dirname(file), "unpack")
    ext = File.extname(file)
    FileUtils.mkdir_p(temp) unless Dir.exist?(temp)
    if ext.casecmp('.tgz').zero?
      untgz(file, temp)
    elsif ext.casecmp('.zip').zero?
      unzip(file, temp)
    else
      raise("Unknown file extension: #{basename}#{ext}")
    end
    # Remove archive's top-level directory, if there is one
    source = tuck(temp)
    if File.exists?(dest)
      puts "Removing #{dest}"
      `rm -rf #{dest}` 
    end
    puts "Moving #{source} to #{dest}"
    FileUtils.mv(source, dest)
    if File.exists?(temp)
      puts "Removing #{temp}"
      `rm -rf #{temp}`
    end
    # TBD: delete what remains of temp
    dest
  end

  def untgz(file, dir)
    res = `cd #{dir} && tar xvzf #{file}`
  end

  def unzip(file, dir)
    # NOTE: -u for "update and create if necessary"
    # NOTE: -q for "quiet"
    # NOTE: -o for "overwrite files WITHOUT prompting"
    res = `cd #{dir} && unzip -quo #{file}`
  end

  # Similar to `flatten` in harvester app/models/drop_dir.rb
  def tuck(dir)
    if File.exist?(File.join(dir, "meta.xml"))
      dir
    else
      winners = Dir.children(dir).filter do |child|
        cpath = File.join(dir, child)
        (File.directory?(cpath) and
         File.exist?(File.join(cpath, "meta.xml")))
      end
      raise("Cannot find meta.xml in #{dir}") unless winners.size == 1
      winners[0]
    end
  end

  # Adapted from harvester app/models/resource/from_open_data.rb
  def load_archive_if_needed(archive_url)
    ext = 'tgz'
    ext = 'zip' if archive_url.match?(/zip$/)
    path = File.join(@workspace, "#{@publishing_id}.#{ext}")

    # Reuse previous file only if URL matches
    file_holding_url = File.join(@workspace, "archive_url")
    valid = false
    if File.exists?(file_holding_url)
      old_url = File.read(file_holding_url)
      if old_url == archive_url
        valid = true 
      else
        STDERR.puts "Different URL this time!"
      end
    end

    if valid && File.exist?(path) && File.size(path).positive?
      STDERR.puts "Using previously downloaded archive.  rm -r #{@workspace} to force reload."
    else
      `rm -rf #{@workspace}`
      FileUtils.mkdir_p(@workspace)
      STDERR.puts "Copying #{archive_url} to #{path}"
      require 'open-uri'
      File.open(path, 'wb') do |file|
        open(archive_url, 'rb') do |input|
          file.write(input.read)
        end
      end
      raise('Did not download') unless File.exist?(path) && File.size(path).positive?
      File.write(file_holding_url, archive_url)
    end
    STDERR.puts "... #{File.size(path)} octets"
    path
  end

  # Adapted from harvester app/models/resource/from_open_data.rb
  def noko_parse(archive_url)
    require 'open-uri'
    begin
      raw = open(archive_url)
    rescue Net::ReadTimeout => e
      fail_with(e)
    end
    fail_with(Exception.new('GET of URL returned empty result.')) if raw.nil?
    Nokogiri::HTML(raw)
  end

  # Cache the resource's resource_pk to page id map in memory

  def get_page_id_map
    return @page_id_map if @page_id_map

    page_id_map = {}

    tt = @tables[Term.tnu]      # a Table
    raise("Cannot find TNU table (I see #{@tables.keys})") unless tt
    if tt.field?(Term.page_id)
      puts "Page id assignments are in the TNU table"
      # get mapping from tnu_id table
      tnu_id_column = tt.column_for_field(Term.tnu_id)
      page_id_column = tt.column_for_field(Term.page_id)
      tt.open_csv_in(tt.location).each do |row|
        page_id_map[row[tnu_id_column]] = row[page_id_column].to_i
      end
    else
      STDERR.puts "Getting page ids for #{@repository_id} from #{@repository_url}"

      # Fetch the resource's node/resource_pk/taxonid to page id map
      # using the web service; put it in a hash for easy lookup.
      # TBD: Need to do this in chunks of at most 500000 (100000 => 6 seconds)

      # e.g. https://beta-repo.eol.org/service/page_id_map/600

      service_url = "#{@repository_url}service/page_id_map/#{@repository_id}"
      STDERR.puts "Request URL = #{service_url}"

      service_uri = URI(service_url)

      limit = 100000
      skip = 0
      all = 0

      loop do
        count = 0
        use_ssl = (service_uri.scheme == 'https')

        # Wait, what about %-escaping ????
        path_and_query = "#{service_uri.path}?#{service_uri.query}&limit=#{limit}&skip=#{skip}"

        Net::HTTP.start(service_uri.host, service_uri.port, :use_ssl => use_ssl) do |http|
          response = http.request_get(path_and_query, {"Accept:" => "text/csv"})
          STDERR.puts response.body if response.code != '200'
          # Raise error if not success (poorly named method)
          response.value

          CSV.parse(response.body, headers: true) do |row|
            count += 1
            all += 1
            tnu_id = row["resource_pk"]
            page_id = row["page_id"].to_i
            page_id_map[tnu_id] = page_id
            if all < 5
              puts "#{tnu_id} -> #{page_id}"
              puts "No TNU id: #{row}" unless tnu_id
              puts "No page id: #{row}" unless page_id
            end
          end
        end
        break if count < limit
        skip += limit
        STDERR.puts "Got chunk #{skip}, going for another"
      end
    end
    STDERR.puts "Got #{page_id_map.size} page ids"
    @page_id_map = page_id_map
    @page_id_map
  end

  def choose_temp_directory(id)
    temp_dir = "/tmp/reaper/${id}"
    Dir.mkdir(temp_dir) unless Dir.exist?(temp_dir)
    temp_dir
  end

end
