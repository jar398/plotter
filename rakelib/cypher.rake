require 'csv'

task :cypher do
  tag = ENV['CONF']
  tag || raise("Please define the CONF environment variable")
  ENV['QUERY'] || raise("Please define the QUERY environment variable")
  results = System.system.get_trait_bank(tag).get_graph.run_query(ENV['QUERY'])

  csv = CSV.new(STDOUT)
  csv << results["columns"]
  results["data"].each do |row|
    csv << row
  end
end
