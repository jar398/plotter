require 'system'
require 'resource'

namespace :resource do

  def get_trait_bank
    tag = ENV['CONF'] || raise("Please provide env var CONF (e.g. 'test')")
    System.system.get_trait_bank(tag)
  end

  def get_resource
    id = ENV['ID'] || raise("Please provide env var ID")
    get_trait_bank.get_resource(id.to_i)
  end

  def get_repo_resource                  # utility
    tb = get_trait_bank
    rid = ENV['REPO_ID']
    if rid
      repo = tb.get_publishing_location.get_repository_location
      repo.get_own_resource(rid.to_i)
    else
      puts "** No REPO_ID, getting latest version of ID=#{ENV['ID']}"
      get_resource.get_publishing_resource.get_repository_resource
    end
  end

  desc "Load resource from opendata and store vernaculars on staging site"
  task :prepare do 
    get_resource.harvest
    get_repo_resource.stage
  end

  desc "Put onto staging site"
  task :stage do
    get_repo_resource.stage
  end

  desc "Erase vernaculars from graphdb, for one resource"
  task :erase do
    get_resource.erase
  end

  desc "Load vernaculars into graphdb from staging site"
  task :publish do
    get_resource.publish_vernaculars
  end

  desc "Display number of vernacular records in graphdb"
  task :count do
    get_resource.count
  end

end
