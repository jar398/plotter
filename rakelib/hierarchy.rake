# Manipulate dynamic hierarchy, see hierarchy.rb

require 'system'
require 'hierarchy'

namespace :hierarchy do

  desc "Create index(es) in graphdb of Page nodes"
  task :create_indexes do
    Hierarchy.new(get_in_repo).create_indexes()
  end

  desc "Load Pages from csv files"
  task :load do
    Hierarchy.new(get_in_repo).load_pages_table
  end

  # Not sure these make sense any more

  task :patch_parents do
    file = ENV['CHANGES'] || raise("Please provide env var CHANGES (change.csv file)")
    Hierarchy.new(get_in_repo).patch_parents(file)
  end
  task :patch do
    dir = ENV['PATCH'] || raise("Please provide env var PATCH (directory)")
    Hierarchy.new(get_in_repo).patch(dir)
  end
  task :delete do
    file = ENV['PAGES'] || raise("Please provide env var PAGES (file)")
    Hierarchy.new(get_in_repo).delete(file)
  end
  task :dump do
    file = ENV['DEST'] || raise("Please provide destination file name")
    Hierarchy.new(get_in_repo).dump(file)
  end

  def get_in_repo                  # utility
    tb = get_trait_bank
    rid = ENV['REPO_ID'] || raise("Please provide env var REPO_ID")
    repo = tb.get_publishing_location.get_repository_location
    repo.get_own_resource(rid.to_i)
  end
  def get_trait_bank(tag)
    tag = ENV['CONF'] || raise("Please provide env var CONF (e.g. 'CONF=test')")
    System.system.get_trait_bank(tag)
  end

end
