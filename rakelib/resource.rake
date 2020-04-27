require 'resource'

namespace :resource do

  def get_resource
    Resource.new(
      system: System.system(ENV['CONF']),
      publishing_id: ENV['ID'],
      repository_id: ENV['REPOSITORY_ID'])
  end

  desc "Load resource from opendata and store vernaculars on staging site"
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
