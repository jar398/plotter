# What is a table?  Well, it has rows and columns.  Maybe a definite
# number of them, or maybe the number can change, I don't know.  But it
# has some kind of identity.  It can be read from one or more places and
# written in one or more places.  It can be read or written in various
# formats.

# This class, however, is about a table in a particular file format,
# in a particular location in the file system and/or a location
# accessible via HTTP.

# TBD: 
#   1. Fetch a remote table locally, using HTTP GET.
#   2. Store a local table remotely, using scp.

class Table
  class << self
    def positionalize(position_map)  # position_map : property -> position
      props = Array.new(position_map.values.max + 1)
      position_map.each {|prop, pos| props[pos] = prop}
      puts "# Column properties: #{props.collect{|prop|prop.name}}"
      props
    end

    def depositionalize(position_array)
      m = {}
      position_array.zip((0...position_array.size)).each do |pair|
        (prop, i) = pair
        i > 0
        prop.name
        m[prop] = i
      end
      m
    end
  end

  def initialize(properties: nil,  # array of Property
                 property_positions: nil,  # maps Property to column number
                 header: nil,
                 location: nil,
                 path: nil,
                 url: nil,      # for reading over the web...
                 stage: nil,
                 separator: ',',
                 ignore_lines: 1,
                 claes: nil)
    properties = Table.positionalize(property_positions) unless properties
    @properties = properties
    property_positions = Table.depositionalize(properties) unless property_positions
    @property_positions = property_positions     # URI to column index
    @property_positions.collect do |prop, pos|
      raise "bogus position" unless pos >= 0
    end
    @claes = claes
    @location = location    # file basename, or nil
    if not separator
      # "you should use double quote when you want to escape a character"
      separator = (location.end_with?('.csv') ? ',' : "\t")
    end
    if separator.length > 1
      separator = "\t"
    else
    end
    @separator = separator
    @ignore_lines = ignore_lines

    @header = header        # raw header as it occurs in the file (array), or nil
    @path = path            # full pathname to file location, or nil
    @url = url              # URL where we can retrieve the table, or nil
    @stage = stage          # staging location for scp, or nil
  end

  def claes; @claes; end
  def location; @location || File.basename(@path); end

  def get_properties; @properties; end

  def is_column(prop)
    @property_positions.key?(prop)
  end

  # Nil if column not present...
  def column_for_property(prop)
    @property_positions[prop]
  end

  # List of paths: the the chunks, if split, or the single main csv
  # file, if unsplit

  def get_part_paths
    # Could transfer the file locally...
    raise("Not on local filesystem: #{@url}") unless @path
    dir = @path + ".chunks"
    if File.exists?(dir)
      File.glob("#{dir}/*.csv")
    elsif File.exists?(@path)
      [@path]
    else
      raise("Cannot find any csv files for #{@path}")
    end
  end

  # List of URLs: the chunks, if split, or the main file, if unsplit

  def get_part_urls
    raise("Not on Internet: #{@path}") unless @url
    base_url = @url + ".chunks/"
    response = Net::HTTP.get_response(URI(base_url + "manifest.json"))
    if response.kind_of? Net::HTTPSuccess
      names = JSON.parse(response.body)
      puts "#{names.size} chunks"
      names.collect{|name| base_url + name}
    else
      [@url]
    end
  end

  def fetch                     # get_what
    raise("No URL for this table: #{@path}") unless @url
    raise("No local path for this table: #{@url}") unless @path
    raise("NYI: copy #{@url} to #{@path}")
  end

  def store
    raise("No stage for this table: #{@path}") unless @stage
    raise("No local path for this table: #{@stage}") unless @path
    raise("NYI: copy #{@path} to #{@stage}")
  end

  def open_csv_in(part_path = @path)
    puts "Reading table at #{@part_path}"
    # But, see Resource.get_page_id_map and Paginator.
    quote_char = (@separator == "\t" ? "\x00" : '"')

    chunks_dir = part_path + ".chunks"
    if File.exist?(chunks_dir)
      chunk_paths = File.glob(File.join(chunks_dir, "*.csv"))
      puts "Found #{chunk_paths.length} chunks"
      STDERR.puts "Chunked input not yet implemented: #{chunks_dir}"
    end
    if File.exist?(part_path)
      csv = CSV.open(part_path, "r:UTF-8", col_sep: @separator, quote_char: quote_char)

      (0...@ignore_lines).each do |counter|
        row = csv.shift
        @header = row unless @header
        puts "discarding header row #{row}"
      end
    else
      raise "Found neither #{path_part} nor #{chunks_dir}"
    end
    csv
  end

  # Return the raw header, or synthesize one if unknown

  def get_header
    return @header if @header
    # Make up column headings based on column properties
    header =
      @properties.collect do |prop|
        if prop
          STDERR.puts("URI has no short name: #{prop.uri}") unless prop.name
          (prop.name || prop.uri.split("/")[-1])
        else
          "?"
        end
      end
    puts "Header: #{header.join(',')}"
    @header = header
    @header
  end

  def open_csv_out(part_path = @path)
    puts "Writing table to #{part_path}"
    csv = CSV.open(part_path, "w:UTF-8")
    header = get_header
    (0...@ignore_lines).each{|n| csv << header}
    csv
  end

  def split(chunk_size = 100000)
    dir = @path + ".chunks"
    return dir if File.exists?(@dir)

    raise("No CSV file for table: #{@path}") unless File.exists?(@path)
    File.mkdir(dir) unless File.exists?(dir)

    first_line = `head -1 "#{@path}"`
    puts "split: First line is #{first_line}"

    `tail --lines=+2 "#{@path}" | split --lines=#{chunk_size} - "#{dir}"/`
    File.glob("#{dir}/??").each do |name|
      raw = File.join(dir, name)
      dest = "#{raw}.csv"
      puts "Adding header line to #{raw} to get #{dest}"
      `(echo "#{first_line}"; cat "#{raw}") >"#{dest}"`
      FileUtils.rm(raw)
    end
    dir
  end

end
