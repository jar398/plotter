require 'resource'

namespace :resource do

  def get_resource
    ENV['CONF'] || raise("Please provide env var CONF")
    ENV['ID'] || raise("Please provide env var ID")
    Resource.new(
      system: System.system(ENV['CONF']),
      publishing_id: ENV['ID'],
      repository_id: ENV['REPOSITORY_ID'],
      opendata_url: ENV['OPENDATA_URL'])
  end

  desc "Load resource from opendata and store vernaculars on staging site"
  task :harvest do 
    ENV['OPENDATA_URL'] || STDERR.puts("No OPENDATA_URL given.")
    r = get_resource
    r.harvest
    get_resource.stage
  end

  task :stage do
    get_resource.stage
  end

  task :erase do
    get_resource.erase
  end

  task :publish do
    get_resource.publish
  end

  task :fetch do
    get_resource.fetch
  end

  task :count do
    get_resource.count
  end

end
