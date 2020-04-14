
require 'resource'
require 'painter'
require 'graph'

testing_resource = 99999

namespace :paint do
  desc 'a b c'
  task :count do 
    resource = Resource.new
    resource.bind_to_publishing(ENV['SERVER'],
                                ENV['ID'] || testing_resource,
                                ENV['TOKEN'])
    p = Painter.new(resource.graph)
    p.count(resource)
    false
  end

  desc 'a b c d'
  task :merge do 
    resource = Resource.new
    resource.bind_to_publishing(ENV['SERVER'], ENV['ID'] || testing_resource)
    resource.bind_to_stage(ENV['STAGE_SCP_LOCATION'],
                           ENV['STAGE_WEB_LOCATION'])
    Painter.new.merge(resource)
  end

end
