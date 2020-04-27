require 'csv'

task :cypher do
  ENV['CONF'] || raise("Please define the CONF environment variable")
  ENV['QUERY'] || raise("Please define the QUERY environment variable")
  results = System.system(ENV['CONF']).get_graph.run_query(ENV['QUERY'])

  csv = CSV.new(STDOUT)
  csv << results["columns"]
  results["data"].each do |row|
    csv << row
  end
end
