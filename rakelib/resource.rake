require 'resource'
require 'assembly'

namespace :resource do

  def get_assembly
    System.system.get_assembly(ENV['CONF'])
  end

  def get_resource
    id = ENV['ID'] || raise("Please provide env var ID")
    tag = ENV['CONF'] || raise("Please provide env var CONF")
    get_assembly.get_instance.get_resource_by_id(id.to_i)
  end

  desc "Load resource from opendata and store vernaculars on staging site"
  task :prepare do 
    get_resource.harvest
    get_resource.stage
  end

  desc "Put onto staging site"
  task :stage do
    get_resource.stage(get_assembly)
  end

  desc "Erase all of this resource's contributed information from graphdb"
  task :erase do
    get_resource.erase(get_assembly)
  end

  desc "Load into graphdb from staging site"
  task :publish do
    get_resource.publish(get_assembly)
  end

  desc "Get resource DwCA from opendata (subtask)"
  task :fetch do
    get_resource.fetch
  end

  desc "Number of ... whats?"
  task :count do
    get_resource.count(get_assembly)
  end

end
