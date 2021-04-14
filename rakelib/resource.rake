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

  def get_in_repo                  # utility
    tb = get_trait_bank
    rid = ENV['REPO_ID']
    if rid
      repo = tb.get_publishing_location.get_repository_location
      repo.get_resource(rid.to_i)
    else
      puts "** No REPO_ID, getting latest version of ID=#{ENV['ID']}"
      get_resource.get_publishing_resource.get_repository_resource
    end
  end

  desc "Load resource from opendata and store vernaculars on staging site"
  task :prepare_vernaculars do 
    get_resource.harvest
    get_in_repo.stage
  end

  desc "Put onto staging site"
  task :stage do
    get_in_repo.stage
  end

  desc "Erase vernaculars from graphdb, for one resource"
  task :erase do
    get_resource.erase
  end

  desc "Load vernaculars into graphdb from staging site"
  task :publish_vernaculars do
    get_resource.publish_vernaculars
  end

  desc "Get resource DwCA from opendata (subtask)"
  task :fetch do
    get_in_repo.fetch
  end

  desc "Number of vernacular records in graphdb"
  task :count_vernaculars do
    get_resource.count
  end

  desc "Extract resource's page id map from repository, writing to file"
  task :map do
    get_in_repo.get_page_id_map() 
    path = get_in_repo.page_id_map_path
    puts "Page id map is at #{path}"
  end

  desc "Show miscellaneous information about a resource"
  task :info do
    get_resource.show_info()
  end

  desc "Show miscellaneous information about a resource version"
  task :version_info do
    get_in_repo.show_repository_info()
  end

  task :tables do
    tables = get_in_repo.get_dwca.get_tables.values
    tables.each do |t|
      # Do this up front for less cluttered output
      t.get_header
    end
    # List of tables
    # It would be better to use a CSV writer
    puts "\nfile,class,local_path"
    tables.each do |t|
      puts "#{t.basename},\"#{t.claes.uri}\",\"#{t.path}\""
    end
    puts "\n"
    tables.each do |t|
      t.show_info
    end
  end

end
