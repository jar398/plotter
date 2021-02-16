
desc "Create or refresh config/resources.yml"

task :refresh do
  tag = ENV['CONF'] || raise("Please provide env var CONF (e.g. 'test')")
  assembly = System.system.get_assembly(tag)
  loc = assembly.get_location("concordance")
  Concordance.new(assembly).refresh
end
