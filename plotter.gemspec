
lib = File.expand_path("../lib", __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require "plotter/version"

Gem::Specification.new do |spec|
  spec.name          = "plotter"
  spec.version       = Plotter::VERSION
  spec.authors       = ["Jonathan A. Rees"]
  spec.email         = ["jar398@mumble.net"]

  spec.summary       = %q{This is a short summary, because RubyGems requires one.}
  spec.description   = %q{To do: Write a longer description or delete this line.}
  spec.homepage      = "https://github.com/jar398/plotter"
  spec.license       = "MIT"

  # Prevent pushing this gem to RubyGems.org. To allow pushes either set the 'allowed_push_host'
  # to allow pushing to a single host or delete this section to allow pushing to any host.
  # if spec.respond_to?(:metadata)
  #   spec.metadata["allowed_push_host"] = "TODO: Set to 'http://mygemserver.com'"
  #   spec.metadata["homepage_uri"] = spec.homepage
  #   spec.metadata["source_code_uri"] = "https://github.com/jar398/plotter/"
  #   spec.metadata["changelog_uri"] = "https://github.com/jar398/plotter/commits/"
  # else
  #   raise "RubyGems 2.0 or newer is required to protect against " \
  #     "public gem pushes."
  # end

  # Specify which files should be added to the gem when it is released.
  # The `git ls-files -z` loads the files in the RubyGem that have been added into git.
  spec.files         = Dir.chdir(File.expand_path('..', __FILE__)) do
    `git ls-files -z`.split("\x0").reject { |f| f.match(%r{^(test|spec|features)/}) }
  end
  spec.bindir        = "exe"
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  # dependabot alert 2021-06-23
  spec.add_development_dependency "bundler", ">= 2.2.10"
  spec.add_development_dependency "rake", "~> 12.3.3"
  spec.add_development_dependency "minitest", "~> 5.0"
  spec.add_development_dependency "nokogiri"

  # I'm shooting in the dark here.  Some of these might not be needed.
  spec.add_development_dependency "faraday", ">= 1.4.3"
  spec.add_development_dependency "faraday_middleware", ">= 1.0.0"
  spec.add_development_dependency "faraday-net_http", ">= 1.0.1"
  spec.add_development_dependency "faraday-net_http_persistent", ">= 1.1.0"
  spec.add_development_dependency "faraday-em_synchrony", ">= 1.0.0"
  spec.add_development_dependency "faraday-em_http", ">= 1.0.0"
  spec.add_development_dependency "faraday-excon", ">= 1.1.0"
  spec.add_development_dependency "rubyzip", ">= 2.2.0"


  # OGM (object graph mapper for Neo4J), via @mvitale at eol/eol_website.
  # I couldn't get these to work, so suppressing dependencies for now
  # spec.add_development_dependency "activegraph", "~> 10.0.0"
  # spec.add_development_dependency "neo4j-ruby-driver", "~> 1.7.0" 

end
