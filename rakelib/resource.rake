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
  task :prepare_vernaculars do 
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
  task :publish_vernaculars do
    get_resource.publish_vernaculars
  end

  desc "Get resource DwCA from opendata (subtask)"
  task :fetch do
    get_repo.fetch
  end

  desc "Number of vernacular records in graphdb"
  task :count_vernaculars do
    get_resource.count
  end

  desc "Extract page id map to a file"
  task :map do
    get_repo.get_page_id_map() 
    path = get_repo.page_id_map_path
    puts "Page id map is at #{path}"
  end

  desc "Show miscellaneous information about a resource"
  task :info do
    get_resource.info()
  end

  task :foo do
    pub = get_trait_bank.get_publishing_location
    repo = pub.get_repository_location
    pids = pub.get_own_resource_records.keys
    puts "#{pids.size} resources in publishing repo"
    pids.sort.each do |pid|
      pr = pub.get_resource_by_id(pid)
      rr = pr.get_repository_resource
      if rr
        vs = rr.versions
        if vs.size > 1 #pid % 17 == 0
          puts("Pub #{pid} -> repo #{rr.id} in #{vs} #{rr.name}")
        end
      end
    end
  end

end
