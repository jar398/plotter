# Manipulate dynamic hierarchy, see hierarchy.rb

require 'system'
require 'hierarchy'

namespace :hierarchy do
  desc "Create index(es) in graphdb of Page nodes"
  task :create_indexes do
    get_hierarchy.create_indexes()
  end

  desc "Load Pages from csv files"
  task :load do
    get_hierarchy.load
  end

  desc "Synchronize Page metadata with csv files"
  task :sync_metadata do
    get_hierarchy.sync_metadata
  end

  # Not sure these make sense any more

  task :patch_parents do
    file = ENV['CHANGES'] || raise("Please provide env var CHANGES (change.csv file)")
    get_hierarchy.patch_parents(file)
  end
  task :patch do
    dir = ENV['PATCH'] || raise("Please provide env var PATCH (directory)")
    get_hierarchy.patch(dir)
  end
  task :delete do
    file = ENV['PAGES'] || raise("Please provide env var PAGES (file)")
    get_hierarchy.delete(file)
  end
  task :dump do
    file = ENV['DEST'] || raise("Please provide destination file name")
    get_hierarchy.dump(file)
  end

  def get_hierarchy
    tag = ENV['CONF'] || raise("Please provide env var CONF (e.g. 'CONF=test')")
    tb = System.system.get_trait_bank(tag)
    rid = ENV['REPO_ID']
    if rid
      repo = tb.get_publishing_location.get_repository_location
      resource = repo.get_own_resource(rid.to_i)
    else
      id = ENV['ID']
      id || raise("Please provide env var REPO_ID or ID")
      pub = tb.get_publishing_location
      pub_resource = pub.get_own_resource(id.to_i)
      resource = pub_resource.get_repository_resource
    end
    Hierarchy.new(resource, tb)
  end

end
