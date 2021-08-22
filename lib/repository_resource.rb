
require 'resource'

class RepositoryResource < Resource

  def get_landing_page_url
    rec = @location.get_own_resource_record(id)
    raise "No repository resource record for #{id}(#{name})" \
      unless rec
    lp_url = rec["opendataUrl"]
    raise "No landing page URL for #{id}(#{name}) in #{@location.name}" unless lp_url
    lp_url
  end

  def show_repository_info
    puts "In repository instance:"
    puts "  versions: #{versions}"
    puts "  id: #{id}"
    rrel = relative_path("")
    puts "  relative path: #{rrel}"
    puts "  workspace path: #{workspace_path(rrel)}"
    puts "  staging url: #{staging_url(rrel)}"
    puts ""
  end

  def versions
    gotcha = @location.get_own_resource_records.values.select do |r|
      r["name"] == name
    end
    gotcha.collect{|r| r["id"]}.sort
  end

  # We use the repository server for its page_id_map service

  # Cache the resource's resource_pk to page id map in memory
  # [might want to cache it in the file system as well]

  def get_page_id_map
    @location.assert_repository
    return @page_id_map if @page_id_map

    path = page_id_map_path
    STDERR.puts "Reading page id map from #{path}"
    csv = CSV.open(path, "r:UTF-8", col_sep: ",", quote_char: '"')
    csv.shift
    page_id_map = {}
    csv.each do |row_in|
      (node_id, page_id) = row_in
      page_id_map[node_id] = page_id.to_i
    end
    @page_id_map = page_id_map
    @page_id_map
  end

  # Fetches page id map if it's not cached yet

  def page_id_map_path
    path = workspace_path(relative_path("page_id_map.csv"))
    unless File.exist?(path)
      STDERR.puts "Writing page id map to #{path}"
      @page_id_map = fetch_page_id_map
      csv_out = CSV.open(path, "w:UTF-8")
      csv_out << ["resource_pk", "page_id"]
      @page_id_map.each do |node_id, page_id|
        csv_out << [node_id, page_id.to_i]
      end
      csv_out.close
    end
    path
  end

  # Method applicable to a repository resource

  def fetch_page_id_map
    page_id_map = {}

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
            STDERR.puts "#{taxon_id} -> #{page_id}"
            STDERR.puts "No TNU id: #{row}" unless taxon_id
            STDERR.puts "No page id: #{row}" unless page_id
          end
        end
      end
      break if count < limit
      skip += limit
      STDERR.puts "Got chunk #{skip}, going for another"
    end
    STDERR.puts "Got #{page_id_map.size} page ids" 
    page_id_map
  end

  def unused_code(dwca)
    tt = dwca.get_table(Claes.taxon)      # a Table
    if false && tt.is_column(Property.page_id)
      STDERR.puts "\nThere are page id assignments in the #{tt.basename} table"
      # get mapping from taxon_id table
      taxon_id_column = tt.column_for_property(Property.taxon_id)
      page_id_column = tt.column_for_property(Property.page_id)
      tt.open_csv_in.each do |row|
        page_id_map[row[taxon_id_column]] = row[page_id_column].to_i
      end
    end
  end
end
