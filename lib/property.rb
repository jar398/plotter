require 'denotable'
require 'registry'

# The names are going to occur as selectors on rows in cypher queries
# (e.g. 'row.tnu')... so they all need to be valid cypher identifiers

class Property < Denotable
  # inherits: initialize, etc.

  class << self

    MAPPING = {"vernacular_string" => "http://rs.tdwg.org/dwc/terms/vernacularName",
               "language_code" => "http://purl.org/dc/terms/language",
               "taxon_id" => "http://rs.tdwg.org/dwc/terms/taxonID",
               "page_id" => "http://eol.org/schema/EOLid",
               "starts_at" => "https://eol.org/schema/terms/starts_at",
               "stops_at" => "https://eol.org/schema/terms/stops_at"}
    puts "initializing properties"
    MAPPING.each do |name, uri|
      Property.get(uri, name)
    end

    def get(uri, name = nil)
      prop = Registry.by_uri(uri)
      if prop
        prop
      else
        puts "Registering #{uri} as #{name || '(anonymous)'}"
        Property.new(uri, name)
      end
    end

    # Particular properties mentioned in the ruby code

    # Properties in tables generally
    def vernacular_string; named("vernacular_string"); end
    def language_code; named("language_code"); end
    def tnu_id; named("taxon_id"); end
    def page_id; named("page_id"); end

    # Predicate property on MetaData nodes
    def starts_at; named("starts_at"); end
    def stops_at; named("stops_at"); end

  end
end

