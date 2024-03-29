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
    area = System.system.workspace_path(trait_bank.relative_path("trait_dumps"))
    FileUtils.mkdir_p(area)
    ws = File.join(area, "tmp")
    clade = ENV['ROOT'] || ENV['ID']           # page id, possibly nil
    tempdir = ENV['TEMP'] ||    # temp dir = where to put intermediate csv files
              File.join(ws, "dump-#{clade || 'all'}")
    FileUtils.mkdir_p(tempdir)
    puts "Temporary files will be in #{tempdir}"
    if ENV.key?('CHUNK')
      chunksize = ENV['CHUNK'].to_i    # possibly nil
    end
    dest = ENV['ZIP'] || area    # ?
    TraitsDumper.new(trait_bank.get_graph, chunksize, tempdir).
      dump_traits(dest, clade)
  end

  desc "Load traits from a traits dump into the graphdb"
  task :load do
    TraitsLoader.new(get_trait_bank.get_graph).load_terms(ENV['TERMS'])
  end

  desc "Transfer basic resource metadata into Resource nodes in the graphdb:\
 resource_id,repository_id,name,description"
  task :sync_resource_metadata do
    get_trait_bank.sync_resource_nodes
  end

  desc "List resources that have 2 or more versions"
  task :multiversion do
    tb = get_trait_bank
    pub = tb.get_publishing_location
    repo = pub.get_repository_location
    pids = pub.get_own_resource_records.keys
    puts "# #{pids.size} resources in publishing repo"
    header_pending = true
    pids.sort.each do |pid|
      pr = pub.get_own_resource(pid)
      rr = pr.get_repository_resource
      if rr
        vs = rr.versions
        if vs.size > 1 #pid % 17 == 0
          vs.each do |vid|
            dwca_url = rr.get_dwca.get_dwca_url
            x = dwca_url.index("resource/")
            if x != nil
              dwca_url = dwca_url[x+9..] 
            else
              x = dwca_url.index("resources/")
              dwca_url = dwca_url[x+10..] if x != nil
            end
            dwca_tag = dwca_url
            if header_pending
              puts "id_in_publishing\tid_in_repository\tdwca_tag\tname"
              header_pending = false
            end
            puts "#{pid}\t#{vid}\t#{dwca_tag}\t#{rr.name}"
          end
        end
      end
    end
  end

  desc "Show resources that have traits"
  task :traitless do
    tb = get_trait_bank
    graph = tb.get_graph
    results = graph.run_query(
      "MATCH (r:Resource),
             (t:Trait)-[:supplier]->(r)
             RETURN DISTINCT r.resource_id
             LIMIT 50")
    puts "#{results["data"].size} resources with traits"
    ids = results["data"].each{|id| id.to_s}.sort
    ids.each do |result_row|
      id = result_row[0].to_i
      more = graph.run_query(
        "MATCH (r:Resource {resource_id: #{id}})
               <-[:supplier]-(t:Trait)
             RETURN COUNT(t)
             LIMIT 1")
      count = more["data"][0][0]
      r = tb.get_resource(id)
      rr = r.get_publishing_resource.get_repository_resource
      puts("#{id} #{rr.id} #{count} #{r.name}")
    end
  end

  desc "Refresh cached JSON file containing resource records"
  task :flush do
    sys = System.system
    ["prod", "beta"].each do |name|
      assem = sys.get_trait_bank(name)
      assem.get_location("publishing").flush_resource_records_cache
      assem.get_location("repository").flush_resource_records_cache
    end
  end

  desc "Prepare manifest for directory"
  task :manifest do
    dir = ENV['DIR']
    raise "Please specify DIR=..." unless dir
    System.system.prepare_manifests(dir)
  end

end
