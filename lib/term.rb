class Term
  class << self

    # Tables (classes)
    def tnu; "http://rs.tdwg.org/dwc/terms/Taxon"; end
    def vernacular_name; "http://rs.gbif.org/terms/1.0/VernacularName"; end

    # Columns of tables
    def vernacular_namestring; "http://rs.tdwg.org/dwc/terms/vernacularName"; end
    def language; "http://purl.org/dc/terms/language"; end
    def tnu_id; "http://rs.tdwg.org/dwc/terms/taxonID"; end
    def page_id; "http://eol.org/schema/EOLid"; end

    # Metadata terms
    def starts_at; "https://eol.org/schema/terms/starts_at"; end
    def stops_at; "https://eol.org/schema/terms/stops_at"; end

  end
end

