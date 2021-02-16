require 'system'
require 'resource'

namespace :resource do

  def get_assembly
    tag = ENV['CONF'] || raise("Please provide env var CONF (e.g. 'test')")
    System.system.get_assembly(tag)
  end

  def get_resource
    id = ENV['ID'] || raise("Please provide env var ID")
    tag = ENV['CONF'] || raise("Please provide env var CONF")
    get_assembly.get_resource_by_id(id.to_i)
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

  desc "Extract page id map to a file"
  task :map do
    get_resource.get_page_id_map()
  end

  desc "Show miscellaneous information about a resource"
  task :info do
    get_resource.info()
  end

end
