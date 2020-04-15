
require 'resource'
require 'painter'
require 'graph'

testing_resource = 99999

namespace :paint do
  desc 'a b c'
  task :count do 
    resource = Resource.new(
      publishing_url: ENV['SERVER'],
      publishing_id: ENV['ID'] || testing_resource,
      publishing_token: ENV['TOKEN'])
    Painter.new(resource.get_graph).count(resource)
  end

  desc 'a b c'
  task :qc do 
    resource = Resource.new(
      publishing_url: ENV['SERVER'],
      publishing_id: ENV['ID'] || testing_resource,
      publishing_token: ENV['TOKEN'])
    Painter.new(resource.get_graph).qc(resource)
  end

  desc 'a b c d'
  task :infer do 
    resource = Resource.new(
      publishing_url: ENV['SERVER'],
      publishing_id: ENV['ID'] || testing_resource,
      publishing_token: ENV['TOKEN'])
    Painter.new(resource.get_graph).infer(resource)
  end

  desc 'move inferences to staging site'
  task :stage do 
    resource = Resource.new(
      publishing_url: ENV['SERVER'],
      publishing_id: ENV['ID'] || testing_resource,
      stage_scp: ENV['STAGE_SCP'])
    Painter.new(resource.get_graph).stage(resource)
  end

  desc 'store inferences from staging site into the graphdb'
  task :publish do 
    resource = Resource.new(
      publishing_url: ENV['SERVER'],
      publishing_id: ENV['ID'] || testing_resource,
      publishing_token: ENV['TOKEN'],
      stage_url: ENV['STAGE_URL'])
    Painter.new(resource.get_graph).publish(resource)
  end

  desc 'remove inferences'
  task :clean do 
    resource = Resource.new(
      publishing_url: ENV['SERVER'],
      publishing_id: ENV['ID'] || testing_resource,
      publishing_token: ENV['TOKEN'])
    Painter.new(resource.get_graph).clean(resource)
  end

  desc 'for debugging'
  task :populate do 
    resource = Resource.new(
      publishing_url: ENV['SERVER'],
      publishing_id: ENV['ID'] || testing_resource,
      publishing_token: ENV['TOKEN'])
    Painter.new(resource.get_graph).populate(resource)
  end

end
