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
      puts "No REPO_ID, getting latest version of ID=#{ENV['ID']}"
      get_resource.get_publishing_resource.get_repository_resource
    end
  end

  desc "Get resource DwCA from opendata (subtask)"
  task :fetch do
    get_resource.fetch
  end

  desc "Print location of directory containing resource's unpacked DwCA"
  task :dwca_directory do
    STDOUT.puts get_resource.dwca_directory
  end

  desc "Extract resource's page id map from repository, writing to file"
  task :map do
    r = get_resource
    r.get_page_id_map
    path = r.page_id_map_path(r.get_dwca)
    STDERR.puts "Page id map is at #{path}"
  end

  desc "Path to where page id map will be (or is) stored"
  task :map_path do
    r = get_resource
    puts get_resource.page_id_map_path(r.get_dwca)
  end

  desc "Show miscellaneous information about a resource"
  task :info do
    get_resource.show_info
  end

  desc "Show miscellaneous information about a repo resource version"
  task :version_info do
    get_repo_resource.show_repository_info
  end

  desc "List of resource's tables in CSV form"
  task :tables do
    tables = get_resource.get_dwca.get_tables.values
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
