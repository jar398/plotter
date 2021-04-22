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

  def initialize(rec, loc)
    @location = loc
    raise "gotta have a name at least" unless rec["name"]
    @config = rec               # Publishing resource record (from JSON/YAML)
  end

  # ---------- Various 'identifiers'...

  def name; @config["name"]; end
  def id; @config["id"]; end
  def location; @location; end

  def graphdb_id
    @config["id"]
  end

  # ---------- 

  # Path to be combined with workspace root or staging URL
  def relative_path(basename)
    @location.relative_path(File.join("resources",
                                      id.to_s,
                                      basename))
  end

  def workspace_path(relative)
    @location.workspace_path(relative)
  end

  # This is not a function of the resource or location... maybe
  # shouldn't even be a method on this class
  def staging_url(relative)
    # error if resource id not in relative string??
    @location.staging_url(relative)
  end

  # ----------

  def receive_metadata(hash)
    @config = @location.merge_records(@config, hash, id)
  end

  # ----------

  # Assume ids are consistent between graphdb and publishing
  def get_publishing_resource
    @location.assert_graphdb
    @location.get_publishing_location.get_own_resource(id)
  end

  def get_repository_resource
    @location.assert_publishing
    rid = @config["repository_id"]
    return nil unless rid
    @location.get_repository_location.get_own_resource(rid)
  end

  # ---------- Processing stage 1: copy DWCA from opendata to workspace

  # Need one of dwca_path (local), dwca_url (remote opendata)

  def get_landing_page_url
    lp_url = @config["opendataUrl"]
    unless lp_url
      rid = id
      loc = @location
      rec = loc.get_own_resource_record(rid)
      raise "No repository resource record for #{name} = #{rid}" \
        unless rec
      lp_url = rec["opendataUrl"]
      raise "No landing page URL for '#{name}' in #{@location.name}" unless lp_url
    end
    lp_url
  end

  def get_dwca
    @location.system.get_opendata_dwca(get_landing_page_url, name)
  end

  # ---------- Processing stage 2: map taxon ids occurring in the Dwca
  # (it might be a good idea to cache this locally)

  def map_to_page_id(taxon_id)
    get_page_id_map[taxon_id]
  end

  # ---------- Processing stage 3: workspace to workspace conversion...
  #   convert local unpacked copy of dwca to files for graphdb

  def fetch
    @location.assert_repository
    get_dwca.ensure_unpacked          # Extract meta.xml and so on
  end

  # ---------- Harvesting (stage 3)

  # Similar to ResourceHarvester.new(self).start
  #  in app/models/resource_harvester.rb

  def harvest
    rr = get_publishing_resource.get_repository_resource
    vern_table = rr.get_dwca.get_table(Claes.vernacular_name)
    if vern_table
      rr.harvest_table(vern_table,
                       [Property.page_id,
                        Property.vernacular_string,
                        Property.language_code,
                        Property.is_preferred_name])
    end
  end

  # Map table from DwCA to table suitable for ingestion using LOAD CSV.
  # Convert column names to "pet names" from the Property objects.

  # Table for now is always a vernaculars table, but props tells us
  # which properties to extract (what the columns will be)

  def harvest_table(htable, out_props)
    fetch                       # Get the DwCA and unpack it
    in_props = htable.get_property_vector    # array of Property objects
    raise "No properties (columns) for table" unless in_props
    puts "Input properties are: #{in_props.collect{|p|p.name}}"

    # Output table goes in staging area for this kind of task
    basename = "vernaculars.csv"
    relative = relative_path(File.join("vernaculars", basename))
    out_table = Table.new(property_vector: out_props,
                          basename: basename,
                          path: workspace_path(relative))

    # Prepare for mapping node ids to page ids, which we do if we have
    # a node id and want a page id
    taxon_id_position = htable.column_for_property(Property.taxon_id)
    page_id_position = htable.column_for_property(Property.page_id)
    translate_ids = false
    if in_props.include?(Property.taxon_id) && out_props.include?(Property.page_id)
      puts "# Will translate node ids to page ids"
      get_page_id_map 
      translate_ids = true
    end

    # Where the output columns are in the input
    mapping = out_props.collect do |out_prop|
      col = htable.column_for_property(out_prop)
      unless col ||
             (translate_ids && out_prop == Property.page_id) ||
             out_prop == Property.is_preferred_name
        raise "No input column for needed output property #{out_prop.name}"
      end
      [out_prop, col]
    end
    puts "# Input position for each output position: #{mapping.collect{|x,y|y}}"

    # For resource 40, input columns are vernacularname, language, taxonid
    # (but check the meta.xml in case the header changes)

    counter = 0
    csv_in = htable.open_csv_in
    csv_out = out_table.open_csv_out
    csv_in.each do |row_in|
      row_out = mapping.collect do |pair|
        (out_prop, in_pos) = pair
        # in_pos = column of this property in the input table, if any
        if in_pos != nil
          value = row_in[in_pos]
          if value
            value
          else
            puts "** No #{out_prop.name} at row #{counter}" if counter < 10
            if out_prop == Property.is_preferred_name
              value = 0
            else
              -123
            end
          end
        elsif out_prop == Property.page_id
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
        elsif out_prop == Property.is_preferred_name
          # Default value associated with this property
          1
        else
          puts "** Need column for property #{out_prop.name}" if counter < 10
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

  # ---------- Processing stage 4: copy ... stuff ... to
  # staging area on server

  def stage
    @location.assert_repository
    @location.system.stage(relative_path("."))
  end

  # ---------- Processing stage 5: compute delta

  # TBD

  # ---------- Processing stage 6: erase previous version's stuff

  def count; count_vernaculars; end

  def count_vernaculars
    query = "MATCH (r:Resource {resource_id: #{id}})
             MATCH (v:Vernacular)-[:supplier]->(r)
             RETURN COUNT(v)
             LIMIT 1"
    r = @location.get_graph.run_query(query)
    count = r ? r["data"][0][0] : "?"
    puts("#{count} vernacular records")
  end

  def erase_vernaculars
    query = "MATCH (r:Resource {resource_id: #{id}})
             MATCH (v:Vernacular)-[:supplier]->(r)
             DETACH DELETE v
             RETURN COUNT(v)
             LIMIT 10000000"
    r = @location.get_graph.run_query(query)
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

  def publish_vernaculars     # slurp
    rr = get_publishing_resource.get_repository_resource

    rel = rr.relative_path("vernaculars/vernaculars.csv")
    url = rr.staging_url(rel)
    puts "# Staging URL is #{url}"

    id_in_graph = id

    # Make sure the resource node is there
    @location.get_graph.run_query(
      "MERGE (r:Resource {resource_id: #{id_in_graph},
                          name: \"#{self.name}\"})
       RETURN r.resource_id
       LIMIT 1")

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
    r = @location.get_graph.run_query(query)
    count = r ? r["data"][0][0] : 0
    STDERR.puts("Merged #{count} relationships from #{url}")
  end

  # ---------- Taxon (node) id to page id map

  # We use the repository server for its page_id_map service

  # Cache the resource's resource_pk to page id map in memory
  # [might want to cache it in the file system as well]

  def get_page_id_map
    @location.assert_repository
    return @page_id_map if @page_id_map

    path = page_id_map_path
    if File.exist?(path)
      puts "Reading page id map from #{path}"
      csv = CSV.open(path, "r:UTF-8", col_sep: ",", quote_char: '"')
      csv.shift
      page_id_map = {}
      csv.each do |row_in|
        (node_id, page_id) = row_in
        page_id_map[node_id] = page_id.to_i
      end
      @page_id_map = page_id_map
    else
      puts "Writing page id map to #{path}"
      @page_id_map = fetch_page_id_map
      csv_out = CSV.open(path, "w:UTF-8")
      csv_out << ["resource_pk", "page_id"]
      @page_id_map.each do |node_id, page_id|
        csv_out << [node_id, page_id.to_i]
      end
      csv_out.close
    end
    @page_id_map
  end

  def page_id_map_path
    workspace_path(relative_path("page_id_map.csv"))
  end

  # Method applicable to a repository resource

  def fetch_page_id_map
    page_id_map = {}

    tt = get_dwca.get_table(Claes.taxon)      # a Table
    if false && tt.is_column(Property.page_id)
      puts "\nThere are page id assignments in the #{tt.basename} table"
      # get mapping from taxon_id table
      taxon_id_column = tt.column_for_property(Property.taxon_id)
      page_id_column = tt.column_for_property(Property.page_id)
      tt.open_csv_in.each do |row|
        page_id_map[row[taxon_id_column]] = row[page_id_column].to_i
      end
    else
      repository_url = get_url_for_repository
      STDERR.puts "Getting page ids for #{id} from #{repository_url}"

      # Fetch the resource's node/resource_pk/taxonid to page id map
      # using the web service; put it in a hash for easy lookup.
      # TBD: Need to do this in chunks of at most 500000 (100000 => 6 seconds)

      # e.g. https://beta-repo.eol.org/service/page_id_map/600

      service_url = "#{repository_url}service/page_id_map/#{id}"
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
    page_id_map
  end

  # This method is applicable to repository resources
  def get_url_for_repository
    repository_url = @location.get_url
    repository_url += "/" unless repository_url.end_with?("/")
    repository_url
  end

  def show_info
    puts "In graphdb:"
    puts "  id: #{id}"
    puts "  name: #{name}"
    rel = relative_path("")
    puts "  relative path: #{rel}"
    puts "  workspace path: #{workspace_path(rel)}"
    puts "  staging url: #{staging_url(rel)}"

    pr = get_publishing_resource
    puts "In publishing instance:"
    puts "  id: #{pr.id}"

    rr = pr.get_repository_resource
    rr.show_repository_info
  end

  def show_repository_info
    puts "In repository instance:"
    puts "  versions: #{versions}"
    puts "  id: #{id}"
    rrel = relative_path("")
    puts "  relative path: #{rrel}"
    puts "  workspace path: #{workspace_path(rrel)}"
    puts "  staging url: #{staging_url(rrel)}"
    puts "Opendata landing page URL: #{get_landing_page_url}"
    puts ""
  end

  def versions
    gotcha = @location.get_own_resource_records.values.select do |r|
      r["name"] == name
    end
    gotcha.collect{|r| r["id"]}.sort
  end

end
