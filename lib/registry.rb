
# URI registry.
# Things that can be registered are said to be 'denotable'.

class Registry

  class << self

    # name may be overridden if it collides with name of another denotable.
    # Returns key, which then becomes the object's 'name'.
    def register(obj)
      @index_by_uri = {} unless @index_by_uri
      @index_by_name = {} unless @index_by_name

      raise("No URI") unless obj.uri
      raise("Bad URI: #{obj.uri}") unless obj.uri.include?(":")

      lc_uri = obj.uri.downcase

      have = @index_by_uri[obj.uri] || @index_by_uri[lc_uri]
      if have
        raise "Redundant registration of #{uri}" if obj != have
      end
      @index_by_uri[obj.uri] = obj
      @index_by_uri[lc_uri] = obj
      have = @index_by_name[obj.name]
      if have
        raise "Redundant registration under #{obj.name}" if obj != have
      end
      @index_by_name[obj.name] = obj
      obj
    end

    def deregister(obj)
      @index_by_name.delete(obj.name)
      @index_by_uri.delete(obj.uri)
      @index_by_uri.delete(obj.uri.downcase)
    end

    def by_uri(uri)
      @index_by_uri = {} unless @index_by_uri
      @index_by_uri[uri] || @index_by_uri[uri.downcase]
    end

    def by_name(name)
      @index_by_name = {} unless @index_by_name
      @index_by_name[name]
    end

  end
end
