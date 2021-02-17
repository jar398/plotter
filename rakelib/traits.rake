# Required: specify CONF=tag  (test, dev, prod)

require 'system'
require 'traits_dumper'
require 'traits_loader'
require 'graph'

namespace :traits do

  def get_trait_bank
    tag = ENV['CONF'] || raise("Please provide env var CONF (e.g. 'test')")
    System.system.get_trait_bank(tag)
  end

  desc "Dump traits, either for entire hierarchy or a subtree"
  task :dump do
    trait_bank = get_trait_bank
    puts "Workspace is #{trait_bank.get_workspace}"

    clade = ENV['ID']           # page id, possibly nil
    tempdir = ENV['TEMP'] ||    # temp dir = where to put intermediate csv files
              File.join(trait_bank.get_workspace, "dump-#{clade || 'all'}")
    FileUtils.mkdir_p(tempdir)
    if ENV.key?('CHUNK')
      chunksize = ENV['CHUNK'].to_i    # possibly nil
    end
    dest = ENV['ZIP'] || trait_bank.get_workspace    # ?
    TraitsDumper.new(trait_bank.get_graph, chunksize, tempdir).dump_traits(dest, clade)
  end

  desc "Load traits from a traits dump into the graphdb"
  task :load do
    trait_bank = get_trait_bank
    TraitsLoader.new(trait_bank.get_graph).load_terms(ENV['TERMS'])
  end

  desc "Transfer resource metadata into the graphdb"
  task :sync_resource_metadata do
    trait_bank = get_trait_bank
    trait_bank.sync_resource_nodes
  end

end
