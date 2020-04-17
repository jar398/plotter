# Required: specify CONF=tag  (test, dev, prod)

require 'traits_dumper'
require 'graph'

namespace :traits do

  desc "Dump traits, either for entire hierarchy or a subtree"
  task :dump do
    system = System.system(ENV['CONF'])

    server = ENV['SERVER'] || system.get_publishing_url
    token = ENV['TOKEN'] || system.get_publishing_token
    query_fn = Graph.via_http(server, token)

    clade = ENV['ID']           # page id, possibly nil
    tempdir = ENV['TEMP'] ||    # temp dir = where to put intermediate csv files
              File.join(system.get_workspace_root, "dump-#{clade || 'all'}")
    FileUtils.mkdir_p(tempdir)
    chunksize = ENV['CHUNK']    # possibly nil
    dest = ENV['ZIP']
    TraitsDumper.new(clade, tempdir, chunksize, query_fn).dump_traits(dest)

  end

end
