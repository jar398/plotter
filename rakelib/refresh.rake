
desc "Create or refresh config/resources.yml"

task :refresh do
  tag = ENV['CONF'] || raise("Please provide env var CONF (e.g. 'test')")
  trait_bank = System.system.get_trait_bank(tag)
  loc = trait_bank.get_location("concordance")
  Concordance.new(trait_bank).refresh
end
