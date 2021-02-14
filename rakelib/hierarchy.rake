# Manipulate dynamic hierarchy, see hierarchy.rb

require 'system'
require 'hierarchy'

namespace :hierarchy do

  desc "Load pages from a file"
  task :load do
    tag = ENV['CONF'] || raise("Please provide env var CONF (e.g. 'test')")
    file = ENV['PAGES'] || raise("Please provide env var PAGES (file)")
    assem = Assembly.assembly(tag)
    Hierarchy.new(assem).load(file)
  end
  task :patch_parents do
    tag = ENV['CONF'] || raise("Please provide env var CONF (e.g. 'test')")
    file = ENV['CHANGES'] || raise("Please provide env var CHANGES (change.csv file)")
    assem = Assembly.assembly(tag)
    Hierarchy.new(assem).patch_parents(file)
  end
  task :patch do
    tag = ENV['CONF'] || raise("Please provide env var CONF (e.g. 'test')")
    dir = ENV['PATCH'] || raise("Please provide env var PATCH (directory)")
    assem = Assembly.assembly(tag)
    Hierarchy.new(assem).patch(dir)
  end
  task :delete do
    tag = ENV['CONF'] || raise("Please provide env var CONF (e.g. 'test')")
    file = ENV['PAGES'] || raise("Please provide env var PAGES (file)")
    assem = Assembly.assembly(tag)
    Hierarchy.new(assem).delete(file)
  end
  task :dump do
    tag = ENV['CONF'] || raise("Please provide env var CONF (e.g. 'test')")
    file = ENV['DEST'] || raise("Please provide destination file name")
    assem = Assembly.assembly(tag)
    Hierarchy.new(assem).dump(file)
  end

end
