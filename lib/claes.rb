require 'denotable'
require 'registry'

class Claes < Denotable
  # inherits: initialize, etc.

  class << self

    MAPPING = {"Taxon" => "http://rs.tdwg.org/dwc/terms/Taxon",
               "VernacularName" => "http://rs.gbif.org/terms/1.0/VernacularName"}
    puts "initializing classes"
    MAPPING.each do |name, uri|
      Claes.get(uri, name)
    end

    def get(uri, name = nil)
      Registry.by_uri(uri) || Claes.new(uri, name)
    end

    # Particular classes mentioned in the ruby code
    def tnu; named("Taxon"); end
    def vernacular_name; named("VernacularName"); end

  end

end

