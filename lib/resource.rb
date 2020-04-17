# Workspace structure:
#   ID/ID.tgz or ID/ID.zip  - downloaded archive file
#   ID/unpack/              - transient area used for unpacking
#   ID/archive/             - directory holding archive files
#   ID/archive/meta.xml     - for example
#   ID/normalized/          - result of transduction

require 'csv'
require 'net/http'
require 'fileutils'
require 'json'

require 'term'
require 'table'
require 'dwca'
require 'graph'
require 'claes'
require 'property'

class Resource

  def initialize(system: nil,
                 # Specific to this resource
                 workspace: nil,
                 publishing_id: nil,
                 repository_id: nil,
                 dwca: nil,
                 opendata_url: nil,
                 dwca_url: nil,
                 dwca_path: nil)
    @system = system

    @workspace = workspace
    @publishing_id = publishing_id ? publishing_id.to_i : nil
    @repository_id = repository_id ? repository_id.to_i : nil
    @dwca = dwca || Dwca.new(get_workspace,
                             opendata_url: opendata_url,
                             dwca_url: dwca_url,
                             dwca_path: dwca_path)
  end

  def get_publishing_id
    return @publishing_id if @publishing_id
    raise("A publishing_id is needed but none was given")
  end

  def get_workspace             # For this resource
    return @workspace if @workspace
    @workspace = File.join(@system.get_workspace_root, get_publishing_id.to_s)
    @workspace
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

  def system; @system; end

  def get_stage_url
    @system.get_stage_url
  end

  def get_graph
    @system.get_graph
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

  def harvest_vernaculars
    @dwca.get_unpacked
    get_page_id_map

    vt = @dwca.get_table(Claes.vernacular_name)

    # open self.location, get a csv reader

    props = [Property.tnu_id,
             Property.vernacular_string,
             Property.language_code]
    # Input column position, in output order
    in_positions = props.collect{|prop| vt.column_for_property(prop)}
    puts "Positions in input are #{in_positions}"

    out_positions = {}          # there must be an easier way
    (0...props.size).each{|pos| out_positions[props[pos]] = pos}

    counter = 0
    csv_in = vt.open_csv_in

    out_table = Table.new(property_positions: out_positions,
                          path: local_staging_path("vernaculars.csv"))
    csv_out = out_table.open_csv_out
    csv_in.each do |row_in|
      row_out = in_positions.collect{|pos| row_in[pos]}
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
    stage_specifier = "#{@sytem.get_stage_scp}#{get_publishing_id.to_s}-#{dir_name}"
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
      if path.end_with?(".chunks")
        man = File.join(path, "manifest.json")
        puts "Writing #{man}"
        File.write(man, names)
      end
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
    query = "MATCH (r:Resource {resource_id: #{get_publishing_id}})
             MATCH (v:Vernacular)-[:supplier]->(r)
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

    # Make sure the resource node is there
    get_graph.run_query(
      "MERGE (r:Resource {resource_id: #{get_publishing_id}})
       RETURN r:resource_id
       LIMIT 1")

    url = "#{get_stage_url}#{get_publishing_id.to_s}-stage/vernaculars.csv"
    query = "LOAD CSV WITH HEADERS FROM '#{url}'
             AS row
             WITH row, toInteger(row.page_id) AS page_id
             MATCH (r:Resource {resource_id: #{get_publishing_id}})
             MERGE (:Page {page_id: page_id})-[:vernacular]->
                   (:Vernacular {namestring: row.namestring,
                                 language: row.language})-[:supplier]->
                   (r)
             RETURN COUNT(row)
             LIMIT 1"
    r = get_graph.run_query(query)
    count = r ? r["data"][0][0] : 0
    STDERR.puts("Merged #{count} relationships from #{url}")
  end

  # Cache the resource's resource_pk to page id map in memory
  # [might want to cache it in the file system as well]

  def get_page_id_map
    return @page_id_map if @page_id_map

    page_id_map = {}

    tt = @dwca.get_table(Claes.tnu)      # a Table
    if tt.column?(Property.page_id)
      puts "Page id assignments are in the TNU table"
      # get mapping from tnu_id table
      tnu_id_column = tt.column_for_property(Property.tnu_id)
      page_id_column = tt.column_for_property(Property.page_id)
      tt.open_csv_in.each do |row|
        page_id_map[row[tnu_id_column]] = row[page_id_column].to_i
      end
    else
      repository_url = @system.get_repository_url
      STDERR.puts "Getting page ids for #{@repository_id} from #{repository_url}"

      # Fetch the resource's node/resource_pk/taxonid to page id map
      # using the web service; put it in a hash for easy lookup.
      # TBD: Need to do this in chunks of at most 500000 (100000 => 6 seconds)

      # e.g. https://beta-repo.eol.org/service/page_id_map/600

      service_url = "#{repository_url}service/page_id_map/#{@repository_id}"
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
