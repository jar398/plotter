# rake dwca:table CLASS=Taxon OPENDATA=https://opendata.eol.org/dataset/tram-807-808-809-810-dh-v1-1/resource/02037fde-cc69-4f03-94b5-65591c6e7b3b

require 'system'
require 'claes'

namespace :dwca do

  desc "Get path to a particular .tsv or .csv file"
  task :table do
    classname = ENV['CLASS'] || raise("Please provide CLASS (e.g. CLASS=Taxon)")
    lp_url = ENV['OPENDATA'] || \
      raise("Please provide OPENDATA (opendata landing page URL)")

    dwca = System.system.get_dwca_via_landing_page(lp_url)
    clas = Denotable.named(classname)
    table = dwca.get_table(clas)
    STDOUT.puts(table.path)
  end

  desc "Show path that leads to unpacked DwCA contents"
  task :path do
    lp_url = ENV['OPENDATA'] || \
      raise("Please provide OPENDATA (opendata landing page URL)")
    dwca = System.system.get_dwca_via_landing_page(lp_url)
    STDOUT.puts(dwca.get_unpacked_loc)
  end

  desc "Show path that leads to unpacked taxon table"
  task :taxa_path do
    lp_url = ENV['OPENDATA'] || \
      raise("Please provide OPENDATA (opendata landing page URL)")
    dwca = System.system.get_dwca_via_landing_page(lp_url)
    paths = dwca.get_table(Claes.taxon).get_part_paths
    raise("Too many paths") unless paths.size == 1
    STDOUT.puts paths.join(" ")
  end

end
