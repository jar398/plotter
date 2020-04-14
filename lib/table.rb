# What is a table?  Well, it has rows and columns.  Maybe a definite
# number of them, or maybe the number can change, I don't know.  But it
# has some kind of identity.  It can be read from one or more places and
# written in one or more places.  It can be read or written in various
# formats.

# This class, however, is about a table in a particular file format,
# in a particular location in the file system.

class Table
  # config is just the <table> element.  parsed on demand.
  def initialize(location, separator, n_ignore, fields, dwca)
    @dwca = dwca
    @location = location
    @fields = fields     # URI to column index
    @separator = separator
    @n_ignore = n_ignore
  end

  def field?(term)
    @fields.key?(term)
  end

  def column_for_field(term)
    @fields[term]
  end

  def location
    @location
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

