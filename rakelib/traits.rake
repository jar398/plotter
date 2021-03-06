# Required: specify CONF=tag  (test, dev, prod)

require 'traits_dumper'
require 'graph'

namespace :traits do

  desc "Dump traits, either for entire hierarchy or a subtree"
  task :dump do
    assembly = Assembly.assembly(ENV['CONF'])

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

end
