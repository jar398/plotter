require 'denotable'
require 'registry'

class Claes < Denotable
  # inherits: initialize, etc.

  class << self

    def get(uri, name = nil)
      Registry.by_uri(uri) || Claes.new(uri, name)
    end

    MAPPING = {"Taxon" => "http://rs.tdwg.org/dwc/terms/Taxon",
               "VernacularName" => "http://rs.gbif.org/terms/1.0/VernacularName",
               "Occurrence" => "http://rs.tdwg.org/dwc/terms/Occurrence",
               "MeasurementOrFact" => "http://rs.tdwg.org/dwc/terms/MeasurementOrFact",
               "Resource" => "data:,EOL data source"}

    MAPPING.each do |name, uri|
      Claes.get(uri, name)
    end

    # Particular classes mentioned in the ruby code
    def taxon; named("Taxon"); end
    def vernacular_name; named("VernacularName"); end
    def resource; named("Resource"); end
    def measurement_or_fact; named("MeasurementOrFact"); end

  end

end

