# Required: specify CONF=tag  (test, dev, prod)

require 'system'
require 'traits_dumper'
require 'traits_loader'
require 'graph'

namespace :traits do

  def get_assembly
    tag = ENV['CONF'] || raise("Please provide env var CONF (e.g. 'test')")
    System.system.get_assembly(tag)
  end

  desc "Dump traits, either for entire hierarchy or a subtree"
  task :dump do
    assembly = get_assembly
    puts "Workspace is #{assembly.get_workspace}"

    clade = ENV['ID']           # page id, possibly nil
    tempdir = ENV['TEMP'] ||    # temp dir = where to put intermediate csv files
              File.join(assembly.get_workspace, "dump-#{clade || 'all'}")
    FileUtils.mkdir_p(tempdir)
    if ENV.key?('CHUNK')
      chunksize = ENV['CHUNK']    # possibly nil
    else
      chunksize = 10000
    end
    dest = ENV['ZIP'] || assembly.get_workspace    # ?
    TraitsDumper.new(clade, tempdir, chunksize, assembly.get_graph).dump_traits(dest)

  end

  desc "Load traits from a traits dump into the graphdb"
  task :load do
    assembly = get_assembly
    TraitsLoader.new(assembly.get_graph).load_terms(ENV['TERMS'])
  end

end
