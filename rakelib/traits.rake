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
    TraitsLoader.new(get_trait_bank.get_graph).load_terms(ENV['TERMS'])
  end

  desc "Transfer resource metadata into the graphdb"
  task :sync_resource_metadata do
    get_trait_bank.sync_resource_nodes
  end

  desc "List resources have 2 or more versions"
  task :multiversion do
    tb = get_trait_bank
    pub = tb.get_publishing_location
    repo = pub.get_repository_location
    pids = pub.get_own_resource_records.keys
    puts "# #{pids.size} resources in publishing repo"
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
