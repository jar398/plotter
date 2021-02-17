require 'system'
require 'resource'

namespace :resource do

  def get_trait_bank
    tag = ENV['CONF'] || raise("Please provide env var CONF (e.g. 'test')")
    System.system.get_trait_bank(tag)
  end

  def get_resource
    id = ENV['ID'] || raise("Please provide env var ID")
    tag = ENV['CONF'] || raise("Please provide env var CONF")
    get_trait_bank.get_resource_by_id(id.to_i)
  end

  def get_repo                  # utility
    get_resource.get_publishing_resource.get_repository_resource
  end

  desc "Load resource from opendata and store vernaculars on staging site"
  task :prepare do 
    get_resource.harvest
    get_repo.stage
  end

  desc "Put onto staging site"
  task :stage do
    get_repo.stage
  end

  desc "Erase all of this resource's contributed information from graphdb"
  task :erase do
    get_resource.erase
  end

  desc "Load into graphdb from staging site"
  task :publish do
    get_resource.publish
  end

  desc "Get resource DwCA from opendata (subtask)"
  task :fetch do
    get_repo.fetch
  end

  desc "Number of ... whats?"
  task :count do
    get_resource.count(get_trait_bank)
  end

  desc "Extract page id map to a file"
  task :map do
    get_resource.get_publishing_resource.get_repository_resource.get_page_id_map()
  end

  desc "Show miscellaneous information about a resource"
  task :info do
    get_resource.info()
  end

end
