
desc "Create or refresh config/resources.yml"

task :refresh do
  tag = ENV['CONF']
  if tag
    assembly = System.get_assembly(tag)
  else
    assembly = System.system
  end
  loc = assembly.get_location("concordance")
  Concordance.new(assembly).refresh
end
