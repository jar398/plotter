# What is a table?  Well, it has rows and columns.  Maybe a definite
# number of them, or maybe the number can change, I don't know.  But it
# has some kind of identity.  It can be read from one or more places and
# written in one or more places.  It can be read or written in various
# formats.

# This class, however, is about a table in a particular file format,
# in a particular location in the file system.

class Table
  # config is just the <table> element.  parsed on demand.
  def initialize(location, separator, n_ignore, fields, resource)
    @resource = resource
    @location = location
    @fields = fields     # URI to column index
    @separator = separator
    @n_ignore = n_ignore
  end

  def field?(name)
    @fields.key?(name)
  end

  def column_for_field(name)
    @fields[name]
  end

  def location
    @location
  end

  # This method doesn't really belong in this class

  def harvest_vernaculars(source, dest)
    # open self.location, get a csv reader

    terms = [Term.tnu_id,
             Term.vernacular_namestring,
             Term.language]
    # These column headings will be used in LOAD CSV commands
    out_header = ["page_id", "namestring", "language"]
    indexes = terms.collect{|term| @fields[term]}
    puts "Indexes are #{indexes}"

    counter = 0
    csv_in = open_csv_in(@location)
    csv_out = open_csv_out(@resource.publish_path("vernaculars.csv"),
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
        page_id = @resource.map_to_page_id(tnu_id)
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

  def open_csv_out(dest, header)
    puts "Writing #{dest}"
    csv = CSV.open(dest, "w:UTF-8")
    csv << header
    csv
  end

  def open_csv_in(source)
    quote_char = (@separator == "\t" ? "\x00" : '"')
    csv = CSV.open(source, "r:UTF-8", col_sep: @separator, quote_char: quote_char)
    (0...@n_ignore).each do |counter|
      row = csv.shift
      puts "discarding header row #{row}"
    end
    csv
  end
end

