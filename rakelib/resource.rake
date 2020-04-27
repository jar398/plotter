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
  ENV['OPENDATA_URL'] || raise("Please provide env var OPENDATA_URL")
  task :harvest do 
    r = get_resource
    r.harvest_vernaculars
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

  task :count do
    get_resource.count
  end

end
