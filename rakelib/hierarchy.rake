# Manipulate dynamic hierarchy, see hierarchy.rb

require 'system'
require 'hierarchy'

namespace :hierarchy do

  def get_assembly(tag)
    tag = ENV['CONF'] || raise("Please provide env var CONF (e.g. 'test')")
    System.system.get_assembly(tag)
  end

  desc "Create index(es) for dynamic hierarchy"
  task :create_indexes do
    assem = get_assembly
    Hierarchy.new(assem).create_indexes()
  end

  desc "Load pages from a file"
  task :load do
    file = ENV['PAGES'] || raise("Please provide env var PAGES (file)")
    assem = get_assembly
    Hierarchy.new(assem).load(file)
  end
  task :patch_parents do
    file = ENV['CHANGES'] || raise("Please provide env var CHANGES (change.csv file)")
    assem = get_assembly
    Hierarchy.new(assem).patch_parents(file)
  end
  task :patch do
    dir = ENV['PATCH'] || raise("Please provide env var PATCH (directory)")
    assem = get_assembly
    Hierarchy.new(assem).patch(dir)
  end
  task :delete do
    file = ENV['PAGES'] || raise("Please provide env var PAGES (file)")
    assem = get_assembly
    Hierarchy.new(assem).delete(file)
  end
  task :dump do
    file = ENV['DEST'] || raise("Please provide destination file name")
    assem = get_assembly
    Hierarchy.new(assem).dump(file)
  end

end
