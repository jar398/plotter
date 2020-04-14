require 'resource'

namespace :reap do
  desc 'a b c'
  task :reap do 
    resource = Resource.new(ENV['WORKSPACE_ROOT'])
    resource.bind_to_opendata(ENV['URL'])
    resource.bind_to_publishing(ENV['PUBLISH'], ENV['ID'])
    resource.bind_to_repository(ENV['REPOSITORY'], ENV['REPOSITORY_ID'])
    resource.harvest
  end

  task :stage do
    resource = Resource.new(ENV['WORKSPACE_ROOT'])
    resource.bind_to_publishing(ENV['PUBLISH'], ENV['ID'])
    resource.bind_to_stage(nil, ENV['STAGE_SCP_LOCATION'])
    resource.stage
  end

  task :erase do
    resource = Resource.new(ENV['WORKSPACE_ROOT'])
    resource.bind_to_publishing(ENV['PUBLISH'], ENV['ID'], ENV['TOKEN'])
    resource.bind_to_stage(ENV['STAGE_WEB_LOCATION'])
    resource.erase
  end

  task :publish do
    resource = Resource.new(ENV['WORKSPACE_ROOT'])
    resource.bind_to_publishing(ENV['PUBLISH'], ENV['ID'], ENV['TOKEN'])
    resource.bind_to_stage(ENV['STAGE_WEB_LOCATION'])
    resource.publish
  end
end

