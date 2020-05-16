# Darwin Core archive management / manipulation

require 'fileutils'
require 'nokogiri'
require 'open-uri'
require 'json'

require 'table'
require 'property'
require 'system'

class Dwca

  def initialize(dwca_workspace, dwca_url, properties)
    @dwca_workspace = dwca_workspace
    @dwca_url = dwca_url      # where the dwca is stored on the Internet
    @properties = properties
    @tables = nil
  end

  def get_workspace
    unless File.exists?(@dwca_workspace)
      FileUtils.mkdir_p(@dwca_workspace)
      # check for existing & merge properties???
      File.write(File.join(get_workspace, "properties.json"),
                 JSON.dumps(@properties))
    end
    @dwca_workspace
  end

  # Where the DwCA is found on the internet
  def get_dwca_url
    raise("No DWCA_URL was specified") unless @dwca_url
    @dwca_url
  end

  # Where the DwCA file is stored in local file system
  def get_dwca_path
    ws = get_workspace
    if @dwca_url
      ext = @dwca_url.end_with?('.zip') ? 'zip' : 'tgz'
      path = File.join(ws, "dwca.#{ext}")
    else
      zip = File.join(ws, "dwca.zip")
      return zip if File.exists?(zip)
      tgz = File.join(ws, "dwca.tgz")
      return zip if File.exists?(tgz)
      raise "No DWCA_PATH or DWCA_URL was specified / present"
    end
  end

  # Where the unpacked files should be put
  def get_unpacked_loc
    dir = File.join(get_workspace, "unpacked")
    FileUtils.mkdir_p(dir)
    dir
  end

  # Ensure that the unpack/ directory is populated from the archive file.

  def ensure_unpacked
    dir = get_unpacked_loc
    return dir if File.exists?(File.join(dir, "meta.xml"))

    # Files aren't there.  Ensure that the archive is present locally,
    # then unpack it.

    unpack_archive(ensure_archive_local_copy(dir), dir)
    # We can delete the zip file afterwards if we want... won't be needed
  end

  def ensure_archive_local_copy(dir)
    url = get_dwca_url
    path = get_dwca_path

    # Use existing archive if it's there
    if url_valid?(url) && File.exist?(path) && File.size(path).positive?
      raise "Using previously downloaded archive.  rm -r #{get_workspace} to force reload."
    else
      System.copy_from_internet(url, path)
    end

    path
  end

  def url_valid?(dwca_url)
    return false unless dwca_url
    # Reuse previous file only if URL matches
    file_holding_url = File.join(get_workspace, "dwca_url")
    valid = false
    if File.exists?(file_holding_url)
      old_url = File.read(file_holding_url)
      if old_url == dwca_url
        valid = true 
      else
        STDERR.puts "Different URL this time!"
      end
    end
  end

  # Adapted from harvester app/models/drop_dir.rb
  # archive_file is either something.tgz or something.zip
  def unpack_archive(archive_file, dir)
    temp = File.join(get_workspace, "tmp")
    FileUtils.mkdir_p(temp) unless Dir.exist?(temp)

    ext = File.extname(archive_file)
    if ext.casecmp('.tgz').zero?
      untgz(archive_file, temp)
    elsif ext.casecmp('.zip').zero?
      unzip(archive_file, temp)
    else
      raise("Unknown file extension: #{basename}#{ext}")
    end
    source = tuck(temp)
    if File.exists?(dir)
      puts "Removing #{dir}"
      `rm -rf #{dir}` 
    end
    puts "Moving #{source} to #{dir}"
    FileUtils.mv(source, dir)
    if File.exists?(temp)
      puts "Removing #{temp}"
      `rm -rf #{temp}`
    end
    dir
  end

  def untgz(archive_file, dir)
    res = `cd #{dir} && tar xvzf #{archive_file}`
  end

  def unzip(archive_file, dir)
    # NOTE: -u for "update and create if necessary"
    # NOTE: -q for "quiet"
    # NOTE: -o for "overwrite files WITHOUT prompting"
    res = `cd #{dir} && unzip -quo #{archive_file}`
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
    ensure_unpacked
    path = File.join(get_unpacked_loc, "meta.xml")
    puts "Processing #{path}"
    @tables = from_xml(path)
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
                path: File.join(get_unpacked_loc, location),
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
