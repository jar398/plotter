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
require 'resource'

class GraphResource < Resource

  def get_repository_resource
    get_publishing_resource.get_repository_resource
  end

  # ----------

  # Assume ids are consistent between graphdb and publishing
  def get_publishing_resource
    @location.assert_graphdb
    @location.get_publishing_location.get_own_resource(id)
  end

  # ----------

  def receive_metadata(hash)
    @config = @location.merge_records(@config, hash, id)
  end

  # ---------- Processing stage 1: copy DWCA from opendata to workspace

  # All resources come from opendata ... except the ones that don't.

  def get_dwca
    # Just need to add a case for local file and we're on our way
    if @config["dwca"]
      specifier = @config["dwca"]
      # Had better be stable !!!  Maybe use sha as key?
      key = File.basename(specifier)
      @location.system.get_dwca(specifier, key, name)
    else
      opendata_lp_url = get_landing_page_url
      raise "No DwCA specified for #{id}(#{name}) in #{@location.name}" \
        unless opendata_lp_url
      @location.system.get_dwca_via_landing_page(opendata_lp_url, name)
    end
  end

  # Specifier = one of dwca_path (local), dwca_url (remote opendata)

  def get_landing_page_url
    @config["opendataUrl"] ||
      get_repository_resource.get_landing_page_url
  end

  # ---------- Processing stage 2: map taxon ids occurring in the Dwca
  # (it might be a good idea to cache this locally)

  def map_to_page_id(taxon_id)
    get_page_id_map[taxon_id]
  end

  # ---------- Processing stage 3: workspace to workspace conversion...
  #   convert local unpacked copy of dwca to files for graphdb
  # Returns directory containing data files

  def fetch
    get_dwca.ensure_unpacked          # Extract meta.xml and so on
  end

  def dwca_directory
    get_dwca.get_unpacked_loc          # Extract meta.xml and so on
  end

  # ---------- Harvesting (stage 3)

  # Similar to ResourceHarvester.new(self).start
  #  in app/models/resource_harvester.rb

  def harvest_vernaculars
    vern_table = get_dwca.get_table(Claes.vernacular_name)
    if vern_table
      harvest_table(vern_table,
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
          if out_prop == Property.is_preferred_name
            (value == nil) || (value.to_i > 0)
          elsif value != nil
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
          true
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
    # Table can be large.  Default chunk size is 100000
    out_table.split(chunk_size = 50000)
  end

  # ---------- Processing stage 4: copy ... stuff ... to
  # staging area on server

  def stage
    @location.assert_repository
    @location.system.stage(relative_path("."))
  end

  def stage_vernaculars
    @location.assert_repository
    @location.system.stage(relative_path("vernaculars"))
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

  # Assumes it has already been staged (on staging server)...

  def publish_vernaculars     # slurp
    rr = get_repository_resource

    rel = rr.relative_path("vernaculars/vernaculars.csv")
    url = rr.staging_url(rel)
    puts "# Staging URL is #{url}"

    # Make sure the resource node is there
    id_in_graph = id
    @location.get_graph.run_query(
      "MERGE (r:Resource {resource_id: #{id_in_graph}})
       RETURN r.resource_id
       LIMIT 1")

    table = Table.new(url: "#{url}")
    table.get_part_urls.each do |part_url|
      query = "LOAD CSV WITH HEADERS FROM '#{part_url}'
               AS row
               WITH row,
                    toInteger(row.page_id) AS page_id,
                    toBoolean(row.is_preferred_name) AS pref
               MATCH (r:Resource {resource_id: #{id_in_graph}})
               MATCH (p:Page {page_id: page_id})
               MERGE (p)-[:vernacular]->
                     (v:Vernacular {string: row.vernacular_string,
                                    language_code: row.language_code})-[:supplier]->
                     (r)
               SET v.is_preferred_name = pref
               RETURN COUNT(v)
               LIMIT 1"
      r = @location.get_graph.run_query(query)
      count = r ? r["data"][0][0] : 0
      STDERR.puts("Merged #{count} relationships from #{part_url}")
    end
  end

  # ---------- Taxon (node) id to page id map

  def get_page_id_map
    get_repository_resource.get_page_id_map
  end

  def page_id_map_path
    get_repository_resource.page_id_map_path
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
    puts "Opendata landing page URL: #{get_landing_page_url}"
  end

end
