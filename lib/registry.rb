
# URI registry.
# Things that can be registered are said to be 'denotable'.

class Registry

  class << self

    def register(obj)
      @index_by_uri = {} unless @index_by_uri
      @index_by_name = {} unless @index_by_name
      raise("No URI") unless obj.uri
      raise("Bad URI: #{uri}") unless obj.uri.include?("://")

      if @index_by_uri[obj.uri] == nil
        @index_by_uri[obj.uri] = obj
        if obj.name
          @index_by_name[obj.name] = obj  
        else
          puts("URI has no cypher name: #{obj.uri}")
        end
      end

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
