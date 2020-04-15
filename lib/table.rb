# What is a table?  Well, it has rows and columns.  Maybe a definite
# number of them, or maybe the number can change, I don't know.  But it
# has some kind of identity.  It can be read from one or more places and
# written in one or more places.  It can be read or written in various
# formats.

# This class, however, is about a table in a particular file format,
# in a particular location in the file system.

class Table
  # config is just the <table> element.  parsed on demand.
  def initialize(fields, path, separator = ",", ignore_lines = 1)
    @path = path
    @fields = fields     # URI to column index
    @separator = separator
    @ignore_lines = ignore_lines
  end

  def field?(term)
    @fields.key?(term)
  end

  def column_for_field(term)
    @fields[term]
  end

  def path
    @path
  end

  def open_csv_out
    puts "Writing #{@path}"
    csv = CSV.open(@path, "w:UTF-8")
    header = ['?' * @fields.size]
    @fields.keys.each{|key| header[@fields[key] = key.split("/")[-1]]}
    puts "Header: #{header.join(' | ')}"
    csv << header
    csv
  end

  def open_csv_in
    quote_char = (@separator == "\t" ? "\x00" : '"')
    csv = CSV.open(@path, "r:UTF-8", col_sep: @separator, quote_char: quote_char)
    (0...@ignore_lines).each do |counter|
      row = csv.shift
      puts "discarding header row #{row}"
    end
    csv
  end
end

