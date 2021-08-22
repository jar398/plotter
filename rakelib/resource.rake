require 'system'
require 'resource'
require 'claes'

namespace :resource do

  def get_trait_bank
    tag = ENV['CONF'] || raise("Please provide env var CONF (e.g. 'CONF=test')")
    System.system.get_trait_bank(tag)
  end

  def get_resource
    id = ENV['ID'] || raise("Please provide env var ID")
    get_trait_bank.get_resource(id.to_i)
  end

  desc "Get resource DwCA from opendata (subtask)"
  task :fetch do
    get_resource.fetch
  end

  desc "Print location of file containing Taxon rows"
  task :taxa_path do
    dwca = get_resource.get_dwca
    paths = dwca.get_table(Claes.taxon).get_part_paths
    # Maybe should err if more than one path?
    raise("Too many paths") unless paths.size == 1
    STDOUT.puts paths.join(" ")
  end

  desc "Print location of resource's taxon table directory containing resource's unpacked DwCA"
  task :dwca_directory do
    STDOUT.puts get_resource.dwca_directory
  end

  desc "Extract resource's page id map from repository, writing to file"
  task :map do
    STDOUT.puts get_resource.page_id_map_path
  end

  desc "Path to where page id map will be (or is) stored"
  task :map_path do
    STDOUT.puts get_resource.page_id_map_path
  end

  desc "Show miscellaneous information about a resource"
  task :info do
    get_resource.show_info
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
