# Manipulate dynamic hierarchy, see hierarchy.rb

require 'system'
require 'hierarchy'

namespace :hierarchy do

  def get_trait_bank(tag)
    tag = ENV['CONF'] || raise("Please provide env var CONF (e.g. 'CONF=test')")
    System.system.get_trait_bank(tag)
  end

  desc "Create index(es) in graphdb of Page nodes"
  task :create_indexes do
    trait_bank = get_trait_bank
    Hierarchy.new(trait_bank).create_indexes()
  end

  desc "Load Pages from a csv file"
  task :load do
    rr = get_in_repo
    trait_bank = get_trait_bank
    Hierarchy.new(trait_bank).load(rr)
  end
  task :patch_parents do
    file = ENV['CHANGES'] || raise("Please provide env var CHANGES (change.csv file)")
    trait_bank = get_trait_bank
    Hierarchy.new(trait_bank).patch_parents(file)
  end
  task :patch do
    dir = ENV['PATCH'] || raise("Please provide env var PATCH (directory)")
    trait_bank = get_trait_bank
    Hierarchy.new(trait_bank).patch(dir)
  end
  task :delete do
    file = ENV['PAGES'] || raise("Please provide env var PAGES (file)")
    trait_bank = get_trait_bank
    Hierarchy.new(trait_bank).delete(file)
  end
  task :dump do
    file = ENV['DEST'] || raise("Please provide destination file name")
    trait_bank = get_trait_bank
    Hierarchy.new(trait_bank).dump(file)
  end

  def get_in_repo                  # utility
    tb = get_trait_bank
    rid = ENV['REPO_ID'] || raise("Please provide env var REPO_ID")
    repo = tb.get_publishing_location.get_repository_location
    repo.get_resource_by_id(rid.to_i)
  end

end
