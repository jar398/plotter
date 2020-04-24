require 'registry'

# Things that can have URIs

class Denotable

  class << self
    def get(uri, name = nil)
      Registry.by_uri(uri) ||
        Registry.register(Denotable.new(uri, name))
    end

    def named(name)
      Registry.by_name(name) || raise("failed to find denotable with name #{name}")
    end
  end

  def uri; @uri; end
  def name; @name; end

  def initialize(uri, name = nil)
    @uri = uri
    @name = name
    # puts "Registered #{name} with URI #{uri}"
    Registry.register(self)
  end

end
