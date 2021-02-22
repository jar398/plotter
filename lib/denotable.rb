require 'registry'

# Things that can have URIs

class Denotable

  class << self
    def named(name)
      Registry.by_name(name) || raise("failed to find denotable with name #{name}")
    end
  end

  def uri; @uri; end
  def name; @name; end

  # Cf. Property.new
  # name is a hint.
  def initialize(uri, hint = nil)
    raise "Already registered #{uri}" if Registry.by_uri(uri)
    raise "Ill-formed URI #{uri}" unless uri.include?(":")
    @uri = uri
    unless hint
      s1 = uri.split('#')
      if s1.size > 1
        hint = s1[-1]
      else
        hint = uri.split('/')[-1]
      end
    end
    name = hint
    counter = 1
    while Registry.by_name(name)
      name = "#{hint}-#{counter}"
      counter += 1
    end
    if name != hint
      puts "Using non-colliding #{name} for #{uri}"
    end
    Registry.register(self)
    @name = name
  end

end
