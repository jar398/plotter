# For workspace structure, see README.md

require 'csv'
require 'net/http'
require 'fileutils'
require 'json'

require 'table'
require 'dwca'
require 'graph'
require 'claes'
require 'property'

class Resource

  def initialize(instance, rec)
    @instance = instance
    raise "gotta have a name at least" unless rec["name"]
    @config = rec               # Publishing resource record (from JSON/YAML)
  end

  # ---------- Various 'identifiers'...

  def name; @config["name"]; end

  def get_qualified_id
    pid = @config["id"]
    # TBD: generate random publishing id if none found in config...
    rid = @config["repository_id"]
    suffix = (rid ? ".#{rid}" : "")
    qid = "#{@instance.name}.#{pid}#{suffix}"
    @config["qualified_id"] = qid
    qid
  end

  def graphdb_id
    @config["id"]
  end

  def repository_id
    @config["repository_id"]
  end

  # ---------- 

  def get_workspace             # For this resource, with its multiple presences
    dir = File.join(@instance.get_workspace,
                    "resources",
                    get_qualified_id)
    FileUtils.mkdir_p(dir)
    dir
  end

  def get_staging_dir
    dir = File.join(@instance.get_workspace,
                    "staging",
                    get_qualified_id)
    FileUtils.mkdir_p(dir)
    dir
  end


  # ---------- Processing stage 1: copy DWCA from opendata to workspace

  # Need one of dwca_path (local), dwca_url (remote opendata)

  def get_dwca
    opendata_url = @config["landing_page"]
    unless opendata_url
      raise "No landing_page provided for resource #{name} (#{get_qualified_id})"
    end
    @instance.get_opendata_dwca(opendata_url, name)
  end

  # ---------- Processing stage 2: map taxon ids occurring in the Dwca
  # (it might be a good idea to cache this locally)

  def map_to_page_id(taxon_id)
    get_page_id_map[taxon_id]
  end

  # ---------- Processing stage 3: workspace to workspace conversion...
  #   convert local unpacked copy of dwca to files for graphdb

  def fetch
    get_dwca.ensure_unpacked          # Extract meta.xml and so on
  end

  # ---------- Harvesting (stage 3)

  # Similar to ResourceHarvester.new(self).start
  #  in app/models/resource_harvester.rb

  def harvest
    vt = get_dwca.get_table(Claes.vernacular_name)
    if vt
      harvest_table(vt,
                    [Property.page_id,
                     Property.vernacular_string,
                     Property.language_code,
                     Property.is_preferred_name])
    end
  end

  def harvest_table(vt, props)
    fetch                       # Get the DwCA and unpack it
    props = vt.get_property_vector    # array of Property objects
    raise "No properties (columns)" unless props
    puts "# Found these columns:\n  #{props.collect{|p|(p ? p.name : '?')}}"

    # For resource 40, input columns are vernacularName, language, taxonID

    if props.include?(Property.page_id)
      # The output file wants a page_id.  If there is no page_id column,
      # figure out the page_id from the taxon_id column.
      page_id_position = vt.column_for_property(Property.page_id)
      taxon_id_position = vt.column_for_property(Property.taxon_id)
      if page_id_position != nil
        puts "# Found page id column in input table at position #{page_id_position}"
        puts "#  Will use page id column from this input"
      else
        if taxon_id_position == nil
          raise("Found neither taxon nor page id in input table")
        end
        puts "# Page id column not found in input table"
        puts "#  Will use page id map from taxon file or content server"
        get_page_id_map
      end
    end

    # Where these columns are in the input
    mapping = props.collect do |prop|
      [prop, vt.column_for_property(prop)]
    end
    puts "# Input position for each output position: #{mapping.collect{|x,y|y}}"

    # Staging directory for this kind of task
    dir = File.join(get_staging_dir, "vernaculars")
    FileUtils.mkdir_p(dir)
    fname = "vernaculars.csv"
    out_table = Table.new(property_vector: props,
                          location: fname,
                          path: File.join(dir, fname))

    counter = 0
    csv_in = vt.open_csv_in
    csv_out = out_table.open_csv_out
    csv_in.each do |row_in|

      row_out = mapping.collect do |pair|
        (prop, in_pos) = pair
        # in_pos = column of this property in the input table, if any
        if in_pos != nil
          value = row_in[in_pos]
          if value
            value
          else
            puts "** No #{prop.name} at row #{counter}" if counter < 10
            if prop == Property.is_preferred_name
              value = 0
            else
              -123
            end
          end
        elsif prop == Property.page_id
          # No column for page id.  Map taxon id to it.
          taxon_id = row_in[taxon_id_position]
          if not taxon_id
            puts "** No taxon id at #{taxon_id_position} for row #{row_in}" if counter < 10
          end
          page_id = map_to_page_id(taxon_id)
          if page_id
            page_id
          else
            puts "** No page id for taxon id #{taxon_id}" if counter < 10
            -345
          end
        elsif prop == Property.is_preferred_name
          # Default value associated with this property
          1
        else
          puts "** Need column for property #{prop.name}" if counter < 10
          -456
        end
      end
      csv_out << row_out
      counter += 1
    end
    puts "#{counter} data rows in csv file"
    csv_out.close
    csv_in.close
  end

  # ---------- Processing stage 4: copy workspace to staging 
  # area on server

  # For staging area location and structure see ../README.md

  def get_staging_url_prefix    # specific to this resource
    prefix_all = @instance.get_staging_location.get_url
    "#{prefix_all}#{get_qualified_id}-"
  end

  def get_staging_rsync_prefix    # specific to this resource
    prefix_all = @instance.get_staging_location.get_rsync_location
    # maybe switch to - instead of / ?
    "#{prefix_all}#{get_qualified_id}-"
  end

  # Copy the staging/TAG.PID.RID/ directory out to the staging host.

  def stage(instance = nil)
    copy_to_stage("vernaculars")
  end

  def copy_to_stage(relative)
    local = File.join(get_staging_dir, relative)
    remote = "#{get_staging_rsync_prefix}#{relative}"
    command = @instance.get_staging_location.get_rsync_command

    prepare_manifests(local)

    STDERR.puts("# Copying #{local} to #{remote}")
    stdout_string, status = Open3.capture2("#{command} #{local}/ #{remote}/")
    puts "Status: [#{status}] stdout: [#{stdout_string}]"
  end

  # Get a resource id that the graphdb will understand correctly.

  def get_id_for_graphdb(assembly)
    id = @config["id"]
    raise "No graphdb resource id for name #{self.name}" unless id
    id
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

  # ---------- Processing stage 5: compute delta

  # TBD

  # ---------- Processing stage 6: erase previous version's stuff

  def count(assembly); count_vernaculars(assembly); end

  def count_vernaculars(assembly)
    id = get_id_for_graphdb(assembly)
    query = "MATCH (r:Resource {resource_id: #{id}})
             MATCH (v:Vernacular)-[:supplier]->(r)
             RETURN COUNT(v)
             LIMIT 1"
    r = assembly.get_graph.run_query(query)
    count = r ? r["data"][0][0] : "?"
    puts("#{count} vernacular records")
  end

  def erase(assembly)
    erase_vernaculars(assembly)
  end

  def erase_vernaculars(assembly)
    id = get_id_for_graphdb(assembly)
    query = "MATCH (r:Resource {resource_id: #{id}})
             MATCH (v:Vernacular)-[:supplier]->(r)
             DETACH DELETE v
             RETURN COUNT(v)
             LIMIT 10000000"
    r = assembly.get_graph(writable = true).run_query(query)
    if r
      count = r["data"][0][0]
      STDERR.puts("Erased #{count} relationships")
      count
    else
      puts "No query result"
      0
    end
  end

  # ---------- Processing stage 7: graphdb LOAD CSV from stage

  def publish
    publish_vernaculars
  end

  def publish_vernaculars(assembly) # slurp
    url = "#{get_staging_url_prefix}vernaculars/vernaculars.csv"
    puts "# Staging URL is #{url}"

    id_in_graph = get_id_for_graphdb(assembly)

    # Make sure the resource node is there
    assembly.get_graph.run_query(
      'MERGE (r:Resource {resource_id: #{id_in_graph}
                          name: "#{self.name}"})
       RETURN r.resource_id
       LIMIT 1')

    # Need to chunk this.
    query = "LOAD CSV WITH HEADERS FROM '#{url}'
             AS row
             WITH row, toInteger(row.page_id) AS page_id
             MATCH (r:Resource {resource_id: #{id_in_graph}})
             MERGE (:Page {page_id: page_id})-[:vernacular]->
                   (:Vernacular {string: row.vernacular_string,
                                 language_code: row.language_code,
                                 is_preferred_name: row.is_preferred_name})-[:supplier]->
                   (r)
             RETURN COUNT(row)
             LIMIT 1"
    r = assembly.get_graph(writable = true).run_query(query)
    count = r ? r["data"][0][0] : 0
    STDERR.puts("Merged #{count} relationships from #{url}")
  end

  # ---------- Taxon id to page id map

  # We use the repository server for its page_id_map service

  # Cache the resource's resource_pk to page id map in memory
  # [might want to cache it in the file system as well]

  def get_page_id_map
    return @page_id_map if @page_id_map

    page_id_map = {}

    tt = get_dwca.get_table(Claes.taxon)      # a Table
    if tt.is_column(Property.page_id)
      puts "\nThere are page id assignments in the #{tt.location} table"
      # get mapping from taxon_id table
      taxon_id_column = tt.column_for_property(Property.taxon_id)
      page_id_column = tt.column_for_property(Property.page_id)
      tt.open_csv_in.each do |row|
        page_id_map[row[taxon_id_column]] = row[page_id_column].to_i
      end
    else
      repository_url = get_url_for_repository
      id_in_repository = @instance.repo_id_for_resource(name)
      STDERR.puts "Getting page ids for #{id_in_repository} from #{repository_url}"

      # Fetch the resource's node/resource_pk/taxonid to page id map
      # using the web service; put it in a hash for easy lookup.
      # TBD: Need to do this in chunks of at most 500000 (100000 => 6 seconds)

      # e.g. https://beta-repo.eol.org/service/page_id_map/600

      service_url = "#{repository_url}service/page_id_map/#{id_in_repository}"
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
            taxon_id = row["resource_pk"]
            page_id = row["page_id"].to_i
            page_id_map[taxon_id] = page_id
            if all < 5
              puts "#{taxon_id} -> #{page_id}"
              puts "No TNU id: #{row}" unless taxon_id
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

  def get_url_for_repository
    repository_url = @instance.get_location("repository").get_url
    repository_url += "/" unless repository_url.end_with?("/")
    repository_url
  end

end
