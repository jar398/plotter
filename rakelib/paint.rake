
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
    Painter.new(resource.graph).count(resource)
  end

  desc 'a b c'
  task :qc do 
    resource = Resource.new
    resource.bind_to_publishing(ENV['SERVER'],
                                ENV['ID'] || testing_resource,
                                ENV['TOKEN'])
    Painter.new(resource.graph).qc(resource)
  end

  desc 'a b c d'
  task :infer do 
    resource = Resource.new
    resource.bind_to_publishing(ENV['SERVER'],
                                ENV['ID'] || testing_resource,
                                ENV['TOKEN'])
    Painter.new(resource.graph).infer(resource)
  end

  desc 'move inferences to staging site'
  task :stage do 
    resource = Resource.new
    resource.bind_to_publishing(ENV['SERVER'],
                                ENV['ID'] || testing_resource)
    resource.bind_to_stage(nil,
                           ENV['STAGE_SCP_LOCATION'])
    Painter.new(resource.graph).stage(resource)
  end

  desc 'store inferences from staging site into the graphdb'
  task :publish do 
    resource = Resource.new
    resource.bind_to_publishing(ENV['SERVER'],
                                ENV['ID'] || testing_resource,
                                ENV['TOKEN'])
    resource.bind_to_stage(ENV['STAGE_WEB_LOCATION'])
    Painter.new(resource.graph).publish(resource)
  end

  desc 'remove inferences'
  task :clean do 
    resource = Resource.new
    resource.bind_to_publishing(ENV['SERVER'],
                                ENV['ID'] || testing_resource,
                                ENV['TOKEN'])
    Painter.new(resource.graph).clean(resource)
  end

  desc 'for debugging'
  task :populate do 
    resource = Resource.new
    resource.bind_to_publishing(ENV['SERVER'],
                                ENV['ID'] || testing_resource,
                                ENV['TOKEN'])
    Painter.new(resource.graph).populate(resource)
  end

end
