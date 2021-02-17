require 'system'

namespace :instance do

  desc "Refresh cached things"
  task :flush do
    sys = System.system
    ["prod", "beta"].each do |name|
      assem = sys.get_trait_bank(name)
      assem.get_location("publishing").flush_resource_records_cache
      assem.get_location("repository").flush_resource_records_cache
    end
  end

end
