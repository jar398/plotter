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

    # This returns the inverse of the position_map i.e. a vector
    # whose indexes are positions of columns holding property values

    def to_property_vector(position_map)  # position_map : property -> position
      prop_vec = Array.new(position_map.values.max + 1)
      position_map.each {|prop, pos| prop_vec[pos] = prop}
      #puts "# Column properties: #{prop_vec.collect{|prop|prop.name}}"
      raise "No fields ??" unless prop_vec.length > 0
      prop_vec
    end

    def depositionalize(position_array)
      m = {}
      position_array.zip((0...position_array.size)).each do |pair|
        (prop, i) = pair
        i > 0
        prop.name
        m[prop] = i
      end
      raise "No fields ??" unless m.length > 0
      m
    end
  end

  def initialize(property_vector: nil,  # array of Property
                 property_positions: nil,  # maps Property to column number
                 header: nil,
                 basename: nil,
                 path: nil,
                 url: nil,      # for reading over the web...
                 stage: nil,
                 separator: ',',
                 ignore_lines: 1,  # header
                 claes: nil)
    # Provide either one of the following
    @property_vector = property_vector
    @property_positions = property_positions    # Property to column position

    @claes = claes
    @basename = basename    # file basename, or nil
    if not separator
      # "you should use double quote when you want to escape a character"
      separator = (basename.end_with?('.csv') ? ',' : "\t")
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
  def basename; @basename || File.basename(@path); end
  def path; @path; end
  def header; @header; end

  # @property_vector is an array of Property objects

  def get_property_vector
    return @property_vector if @property_vector 
    @property_vector = Table.to_property_vector(@property_positions)
    @property_vector
  end

  # @property_positions is a hash from Property to position

  def get_property_positions
    return @property_positions if @property_positions
    @property_positions = Table.depositionalize(@property_vector)
    @property_positions
  end

  def is_column(prop)
    get_property_positions.key?(prop)
  end

  # Nil if column not present...
  def column_for_property(prop)
    get_property_positions[prop]
  end

  # List of paths: the chunks, if split, or the single main csv
  # file, if unsplit

  def get_part_paths
    # Could transfer the file locally...
    raise("Not on local filesystem: #{@url}") unless @path
    dir = @path + ".chunks"
    if File.exist?(dir)
      STDERR.put "Getting .csv files from #{dir}"
      File.glob("#{dir}/*.csv")
    elsif File.exist?(@path)
      STDERR.put "Getting single .csv file #{@path}"
      [@path]
    else
      raise("Cannot find any csv files for #{@path}")
    end
  end

  # List of URLs: the chunks, if split, or the main file, if unsplit

  def get_part_urls
    raise("Not on Internet: #{@basename} #{path}") unless @url
    # System.system.read_manifest(@url)
    base_url = @url + ".chunks/"
    response = Net::HTTP.get_response(URI(base_url + ".manifest.json"))
    if response.kind_of? Net::HTTPSuccess
      names = JSON.parse(response.body)
      STDERR.puts "#{names.size} chunks"
      names.collect{|name| base_url + name}
    else
      [@url]
    end
  end

  def fetch                     # get_what
    raise("No URL for this table: #{@path}") unless @url
    raise("No local path for this table: #{@url}") unless @path
    raise("NYI: fetch #{@path} from #{@url}")
  end

  def store
    raise("No stage for this table: #{@basename}") unless @stage
    raise("No local path for this table: #{@basename}") unless @path
    raise("NYI: copy #{@path} to #{@stage}")
    # something.copy_to_stage ... ?
  end

  def open_csv_in(part_path = @path)
    STDERR.puts "Reading table at #{part_path}"
    # But, see Resource.get_page_id_map and Paginator.
    quote_char = (@separator == "\t" ? "\x00" : '"')

    chunks_dir = part_path + ".chunks"
    if File.exist?(chunks_dir)
      chunk_paths = File.glob(File.join(chunks_dir, "*.csv"))
      STDERR.puts "Found #{chunk_paths.length} chunks"
      STDERR.puts "Chunked input not yet implemented: #{chunks_dir}"
    end
    if File.exist?(part_path)
      csv = CSV.open(part_path, "r:UTF-8", col_sep: @separator, quote_char: quote_char)

      (0...@ignore_lines).each do |counter|
        row = csv.shift
        @header = row unless @header
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
    pv = get_property_vector
    if pv
      header =
        pv.collect do |prop|
          if prop
            STDERR.puts("URI has no short name: #{prop.uri}") unless prop.name
            (prop.name || prop.uri.split("/")[-1])
          else
            "?"
          end
        end
      STDERR.puts "Header: #{header.join(',')}"
      @header = header
    else
      open_csv_in.close     # Side effect: sets @header
    end
    @header
  end

  def open_csv_out(part_path = @path)
    STDERR.puts "Writing table to #{part_path}"
    csv = CSV.open(part_path, "w:UTF-8")
    header = get_header
    (0...@ignore_lines).each{|n| csv << header}
    csv
  end

  def split(chunk_size = 100000)
    raise "Local path not specified for table" unless @path
    raise "No CSV file for table: #{@path}" unless File.exist?(@path)
    dir = @path + ".chunks"
    FileUtils.mkdir_p(dir)

    first_line = `head -1 "#{@path}"`
    STDERR.puts "split: First line is #{first_line}"

    `tail --lines=+2 "#{@path}" | split --lines=#{chunk_size} - "#{dir}"/`
    Dir.glob("#{dir}/??").each do |raw|
      dest = "#{raw}.csv"
      STDERR.puts "Adding header line to get #{dest}"
      `(echo "#{first_line}"; cat "#{raw}") >"#{dest}"`
      FileUtils.rm(raw)
    end
    STDERR.puts "split: Removing #{@path}"
    FileUtils.rm(@path)
    dir
  end

  def show_info
    get_header
    puts "# Table location: #{path}"
    # Also show path to table?
    puts "file,field_name,property_uri,property_name"
    (0..(@header.size)).each do |i| 
      field = @header[i]
      prop = get_property_vector[i]
      if prop
        puts "#{@basename},#{field},\"#{prop.uri}\",#{prop.name}"
      elsif field
        puts "#{@basename},#{field},,"
      end
    end
    puts "\n"
  end

end
