#

require 'graph'

class TraitsLoader

  def initialize(graph)
    @graph = graph
  end

  # terms_keys = ["uri", "name", "type", "parent_uri"]

  def load_terms(terms_file)
    puts "Creating Term index by uri"
    @graph.run_query("CREATE INDEX ON :Term(uri)")

    puts "Creating Term nodes"
    command1 = "USING PERIODIC COMMIT
                LOAD CSV WITH HEADERS FROM '#{terms_file}'
                AS row
                MERGE (t:Term {uri: row.uri})
                SET t.name = row.name
                SET t.type = row.type
                RETURN COUNT(t)
                LIMIT 1"
    r = @graph.run_query(command1)
    puts "#{r["data"][0][0]} Term nodes"

    puts "Linking Term nodes to their parents"
    command2 = "USING PERIODIC COMMIT
                LOAD CSV WITH HEADERS FROM '#{terms_file}'
                AS row
                MATCH (p:Term {uri: row.parent_uri})
                WHERE row.parent_uri IS NOT NULL
                MERGE (t:Term {uri: row.uri})-[:parent_term]->(p)
                RETURN COUNT(t)
                LIMIT 1"
    r = @graph.run_query(command2)
    puts "#{r["data"][0][0]} Term/parent relationships"
  end
end
