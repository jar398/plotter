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

require 'term'
require 'table'
require 'dwca'

class Resource

  def initialize(workspace_root: nil,
                 workspace: nil,
                 publishing_url: nil,
                 publishing_id: nil,
                 publishing_token: nil,
                 repository_url: nil,
                 repository_id: nil,
                 stage_scp: nil,
                 stage_url: nil,
                 config: nil,
                 dwca: nil,
                 opendata_url: nil,
                 dwca_url: nil,
                 dwca_path: nil)
    @workspace_root = workspace_root
    @workspace = workspace
    @publishing_url = publishing_url
    @publishing_id = publishing_id ? publishing_id.to_i : nil
    @publishing_token = publishing_token
    @repository_url = repository_url
    @repository_id = repository_id ? repository_id.to_i : nil
    @stage_scp = stage_scp
    @stage_url = stage_url
    @config = config
    @dwca = dwca || Dwca.new(get_workspace,
                             opendata_url: opendata_url,
                             dwca_url: dwca_url,
                             dwca_path: dwca_path)
  end

  def get_publishing_id
    return @publishing_id if @publishing_id
    raise("A publishing_id is needed but none was given")
  end

  def get_workspace_root        # For all resources
    return @workspace_root if @workspace_root
    @workspace_root = File.join(ENV['HOME'], ".reaper_workspace")
    @workspace_root
  end

  def get_workspace             # For this resource
    return @workspace if @workspace
    @workspace = File.join(get_workspace_root, get_publishing_id.to_s)
    @workspace
  end

  def get_config
    return @config if @config
    @config = YAML.load(File.read("config/secrets.yml"))
    @config
  end

  def get_publishing_url
    return @publishing_url if @publishing_url
    @publishing_url = get_config["development"]["host"]["url"]
    @publishing_url
  end

  def get_repository_id
    return @repository_id if @repository_id
    record = get_record(get_publishing_id)
    if record
      @repository_id = record["repository_id"].to_i
      @repository_id
    else
      raise("Publishing resource #{get_publishing_id} has no repository id")
    end
  end

  def get_repository_url
    return @repository_url if @repository_url
    @repository_url = get_config["development"]["host"]["url"]
    @repository_url
  end

  def get_stage_scp
    if @stage_scp
      @stage_scp += "/" unless @stage_scp.end_with?("/")
    else
      @stage_scp = "varela:public_html/eol/"
    end
    @stage_scp
  end

  def get_stage_url
    if @stage_url
      @stage_url += "/" unless @stage_url.end_with?("/")
    else
      @stage_url = "http://varela.csail.mit.edu/~jar/eol/"
    end
    @stage_url
  end

  def get_graph
    return @graph if @graph
    raise("A token is required, but none was provided") unless @token
    query_fn = Graph.via_http(publishing_url, publishing_token)
    @graph = Graph.new(query_fn)
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
      record = get_record(get_publishing_id)
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

  def dir_in_workspace(dir_name)
    dir = File.join(@workspace, dir_name)
    unless Dir.exist?(dir)
      puts "Creating directory #{dir}"
      FileUtils.mkdir_p(dir) 
    end
    dir
  end

  def archive_path(name)
    File.join(dir_in_workspace("archive"), name)
  end

  def local_staging_path(name)
    File.join(dir_in_workspace("stage"), name)
  end

  # Similar to ResourceHarvester.new(self).start
  #  in app/models/resource_harvester.rb

  def harvest
    @dwca.get_unpacked
    get_page_id_map

    vt = @dwca.get_tables[Term.vernacular_name]      # a Table
    raise("Cannot find vernaculars table (I see #{@tables.keys})") unless vt
    harvest_vernaculars(vt)

    # similarly for other types... maybe particular types should be selectable...

  end

  def harvest_vernaculars(vt)
    # open self.location, get a csv reader

    terms = [Term.tnu_id,
             Term.vernacular_namestring,
             Term.language]
    # These column headings will be used in LOAD CSV commands
    out_header = ["page_id", "namestring", "language"]
    indexes = terms.collect{|term| vt.column_for_field(term)}
    puts "Indexes are #{indexes}"

    counter = 0
    csv_in = vt.open_csv_in(vt.location)
    csv_out = vt.open_csv_out(local_staging_path("vernaculars.csv"),
                              out_header)
    csv_in.each do |row_in|
      row_out = indexes.collect{|index| row_in[index]}
      tnu_id = row_out[0]
      if counter < 10
        puts "No TNU id: '#{row_in}'" unless tnu_id
        puts "No namestring: '#{row_in}'" unless row_out[1]
        puts "No language: '#{row_in}'" unless row_out[2]
      end
      if tnu_id
        page_id = map_to_page_id(tnu_id)
        if page_id
          row_out[0] = page_id
          csv_out << row_out
          puts row_out if counter < 5
        else
          puts "No page id for TNU id #{tnu_id}" if counter < 10
        end
      end
      counter += 1
    end
    csv_out.close
    csv_in.close

    puts "#{counter} data rows in csv file"

    # TBD: Write new csv file for use with LOAD CSV
  end


  # Copy the publish/ directory out to the staging host.
  # dir_name specifies a subdirector of the workspace.

  def stage(dir_name = "stage")
    local_staging_path = File.join(@workspace, dir_name)
    prepare_manifests(local_staging_path)
    stage_specifier = "#{get_stage_scp}#{publishing_id.to_s}-#{dir_name}"
    STDERR.puts("Copying #{local_staging_path} to #{stage_specifier}")
    stdout_string, status = Open3.capture2("rsync -va #{local_staging_path}/ #{stage_specifier}/")
    puts "Status: [#{status}] stdout: [#{stdout_string}]"
  end

  # For each directory in a tree, write a manifest.json that lists the
  # files in the directory.  This makes the tree traversable from by a
  # web client.

  def prepare_manifests(path)
    if File.directory?(path)
      # Prepare one manifest
      names = Dir.glob("*", base: path)
      man = File.join(path, "manifest.json")
      puts "Writing #{man}"
      File.write(man, names)
      # Recur
      names.each {|name| prepare_manifests(File.join(path, name))}
    end
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

  def erase
    erase_vernaculars
  end

  def erase_vernaculars
    query = "MATCH (v:Vernacular {resource_id: #{publishing_id}})
             DETACH DELETE v
             RETURN COUNT(v)
             LIMIT 10000000"
    r = get_graph.run_query(query)
    count = r ? r["data"][0][0] : 0
    STDERR.puts("Erased #{count} relationships")
  end

  def publish
    publish_vernaculars
  end

  def publish_vernaculars
    url = "#{get_stage_url}#{publishing_id.to_s}-publish/vernaculars.csv"
    query = "LOAD CSV WITH HEADERS FROM '#{url}'
             AS row
             WITH row, toInteger(row.page_id) AS page_id
             MERGE (p:Page {page_id: page_id})-[:vernacular]->
                   (:Vernacular {namestring: row.namestring,
                                 language: row.language,
                                 resource_id: #{publishing_id}})
             RETURN COUNT(row)
             LIMIT 1"
    r = get_graph.run_query(query)
    count = r ? r["data"][0][0] : 0
    STDERR.puts("Merged #{count} relationships from #{url}")
  end

  # Adapted from harvester app/models/resource/from_open_data.rb parse
  def parse_manifest
    html = noko_parse(@archive_url)
    archive_url = html.css('p.muted a').first['href']
    file = load_archive_if_needed(archive_url)
    dest = dir_in_workspace("archive")
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

    tt = @dwca.get_tables[Term.tnu]      # a Table
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

end
