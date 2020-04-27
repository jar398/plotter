# Darwin Core archive management / manipulation

require 'net/http'
require 'fileutils'
require 'nokogiri'
require 'open-uri'

require 'table'
require 'property'

class Dwca

  def initialize(workspace, dwca_path: nil, dwca_url: nil)
    @dwca_workspace = File.join(workspace, "dwca")
    @unpacked = File.join(@dwca_workspace, "unpacked")
    @dwca_path = dwca_path         # where the dwca is stored in local file system
    @dwca_url = dwca_url      # where the dwca is stored on the Internet
    @tables = nil
  end

  def get_workspace
    unless Dir.exist?(@dwca_workspace)
      puts "Creating directory #{@dwca_workspace}"
      FileUtils.mkdir_p(@dwca_workspace) 
    end
    @dwca_workspace
  end

  def get_unpacked
    return @unpacked if File.exists?(File.join(@unpacked, "meta.xml"))
    unpack_archive(get_archive)
  end

  # Get the DWCA from the Internet, if haven't done so already
  # Adapted from harvester app/models/resource/from_open_data.rb

  def get_dwca_path
    # If we already know where it is (or should be) locally, use that location
    return @dwca_path if @dwca_path
    # We don't know the path.  Need to figure out the extension from
    # the URL.
    ext = @dwca_url.match?(/zip$/) ? 'zip' : 'tgz'
    @dwca_path = File.join(@dwca_workspace, "dwca.#{ext}")
    @dwca_path
  end

  def get_archive
    # Figure out where the dwca file is supposed to be locally
    dwca_path = get_dwca_path
    # Use existing archive if it's there
    if url_valid? && File.exist?(dwca_path) && File.size(dwca_path).positive?
      STDERR.puts "Using previously downloaded archive.  rm -r #{@dwca_workspace} to force reload."
    else
      # Download the archive file from Internet if it's not
      `rm -rf #{@dwca_workspace}`
      ws = get_workspace    # create the directory
      STDERR.puts "Copying #{@dwca_url} to #{dwca_path}"
      File.open(dwca_path, 'wb') do |file|
        open(@dwca_url, 'rb') do |input|
          file.write(input.read)
        end
      end
      raise('Did not download') unless File.exist?(dwca_path) && File.size(dwca_path).positive?
      File.write(File.join(ws, "dwca_url"), @dwca_url)
    end
    STDERR.puts "... #{File.size(dwca_path)} octets"
    @dwca_path = dwca_path
    dwca_path
  end

  def url_valid?
    # Reuse previous file only if URL matches
    file_holding_url = File.join(@dwca_workspace, "dwca_url")
    valid = false
    if File.exists?(file_holding_url)
      old_url = File.read(file_holding_url)
      if old_url == @dwca_url
        valid = true 
      else
        STDERR.puts "Different URL this time!"
      end
    end
  end

  # Adapted from harvester app/models/drop_dir.rb
  # File is either something.tgz or something.zip
  def unpack_archive(archive)
    dest = @unpacked
    dir = File.dirname(dest)
    FileUtils.mkdir_p(dir) unless Dir.exist?(dir)
                      
    temp = File.join(get_workspace, "tmp")
    FileUtils.mkdir_p(temp) unless Dir.exist?(temp)

    ext = File.extname(archive)
    if ext.casecmp('.tgz').zero?
      untgz(archive, temp)
    elsif ext.casecmp('.zip').zero?
      unzip(archive, temp)
    else
      raise("Unknown file extension: #{basename}#{ext}")
    end
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
    dest
  end

  def untgz(archive, dir)
    res = `cd #{dir} && tar xvzf #{archive}`
  end

  def unzip(archive, dir)
    # NOTE: -u for "update and create if necessary"
    # NOTE: -q for "quiet"
    # NOTE: -o for "overwrite files WITHOUT prompting"
    res = `cd #{dir} && unzip -quo #{archive}`
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

  # @tables maps Claes objects to Table objects

  def get_tables
    return @tables if @tables
    get_unpacked
    @tables = from_xml(File.join(@unpacked, "meta.xml"))
    @tables
  end

  def get_table(claes)
    uri = claes.uri
    tables = get_tables
    probe = tables[claes]
    return probe if probe
    raise("No table for class #{claes.name}.")
  end

  # Get the information that we'll need out of the meta.xml file
  # Returns a hash from classes ('claeses') to table elements
  # Adapted from harvester app/models/resource/from_meta_xml.rb self.analyze
  def from_xml(filename)
    doc = File.open(filename) { |f| Nokogiri::XML(f) }
    tables = doc.css('archive table').collect do |table_element|
      row_type = table_element['rowType']    # a URI
      location = table_element.css("location").first.text
      positions = parse_fields(table_element)
      sep = table_element['fieldsTerminatedBy']
      ig = table_element['ignoreHeaderLines']
      Table.new(property_positions: positions,
                location: location,
                path: File.join(@unpacked, location),
                separator: sep,
                ignore_lines: (ig ? ig.to_i : 0),
                claes: Claes.get(row_type))
    end
    claes_to_table = {}
    # TBD: Complain if a claes has multiple tables???
    tables.each {|table| claes_to_table[table.claes] = table}
    claes_to_table
  end

  # Parse <field> elements, returning hash from Property to 
  # small integer

  def parse_fields(table_element)
    positions_for_this_table = {}
    table_element.css('field').each do |field|
      i = field['index'].to_i
      prop = Property.get(field['term'])
      positions_for_this_table[prop] = i
    end
    positions_for_this_table
  end

end
