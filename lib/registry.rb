
# URI registry.
# Things that can be registered are said to be 'denotable'.

class Registry

  class << self

    def register(obj)
      @index_by_uri = {} unless @index_by_uri
      @index_by_name = {} unless @index_by_name
      uri = obj.uri
      raise("No URI") unless uri
      raise("Bad URI: #{uri}") unless uri.include?("://")
      # What if there's already something indexed under uri or name?
      @index_by_uri[uri] = obj
      @index_by_name[obj.name] = obj if obj.name
      obj
    end

    def deregister(obj)
      @index_by_name.delete(obj.name)
      @index_by_uri.delete(obj.uri)
    end

    def by_uri(uri)
      @index_by_uri = {} unless @index_by_uri
      @index_by_uri[uri]
    end

    def by_name(name)
      @index_by_name = {} unless @index_by_name
      @index_by_name[name]
    end

  end
end
