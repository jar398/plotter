require 'denotable'
require 'registry'

# The names are going to occur as selectors on rows in cypher queries
# (e.g. 'row.taxon')... so they all need to be valid cypher identifiers

class Property < Denotable
  # inherits: initialize, etc.

  class << self

    MAPPING = {"taxon_id" => "http://rs.tdwg.org/dwc/terms/taxonID",
               "page_id" => "http://eol.org/schema/EOLid",
               "parent_page_id" => "http://eol.org/schema/parentEOLid",
               "scientific_name" => "http://rs.tdwg.org/dwc/terms/scientificName",

               # These are DH columns
               "source" => "http://purl.org/dc/terms/source",
               "further_information" => "http://rs.tdwg.org/ac/terms/furtherInformationURL",
               "accepted" => "http://rs.tdwg.org/dwc/terms/acceptedNameUsageID",
               "parent" => "http://rs.tdwg.org/dwc/terms/parentNameUsageID",
               "higher_classification" => "http://rs.tdwg.org/dwc/terms/higherClassification",
               "rank" => "http://rs.tdwg.org/dwc/terms/taxonRank",
               "taxonomic_status" => "http://rs.tdwg.org/dwc/terms/taxonomicStatus",
               "taxon_remarks" => "http://rs.tdwg.org/dwc/terms/taxonRemarks",
               "dataset_id" => "http://rs.tdwg.org/dwc/terms/datasetID",
               "canonical" => "http://rs.gbif.org/terms/1.0/canonicalName",
               "annotations" => "http://eol.org/schema/EOLidAnnotations",
               "landmark" => "http://eol.org/schema/Landmark",

               # Vernaculars
               "vernacular_string" => "http://rs.tdwg.org/dwc/terms/vernacularName",
               "language_code" => "http://purl.org/dc/terms/language",
               "is_preferred_name" => "http://rs.gbif.org/terms/1.0/isPreferredName",

               # Trait metadata
               "starts_at" => "https://eol.org/schema/terms/starts_at",
               "stops_at" => "https://eol.org/schema/terms/stops_at",

               # Resource list sync
               "label" => "http://www.w3.org/2000/01/rdf-schema#label",
               "comment" => "http://www.w3.org/2000/01/rdf-schema#comment",
               "resource_id" => "data:,EOL resource id",
               "resource_version_id" => "data:,EOL resource version id"
              }

    def get(uri, hint = nil)
      Registry.by_uri(uri) || Property.new(uri, hint)
    end

    MAPPING.each do |name, uri|
      Property.get(uri, name)
    end

    # Particular properties mentioned in the ruby code

    # Properties in tables generally
    def taxon_id; named("taxon_id"); end
    def page_id; named("page_id"); end

    def vernacular_string; named("vernacular_string"); end
    def language_code; named("language_code"); end
    def is_preferred_name; named("is_preferred_name"); end

    # Predicate property on MetaData nodes
    def starts_at; named("starts_at"); end
    def stops_at; named("stops_at"); end

    # Resource properties
    def label; named("label"); end
    def comment; named("comment"); end
    def resource_id; named("resource_id"); end
    def resource_version_id; named("resource_version_id"); end

  end
end
