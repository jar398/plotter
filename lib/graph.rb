#require 'neography'

class Graph

  # Hmm.  If a neo4j URL is given in the configuration, try that.
  # Otherwise, go through the publishing server.

  # Default method for accessing the graph = EOL v3 API

  def self.via_http(eol_api_url, token)
    eol_api_url += "/" unless eol_api_url.end_with?("/")
    raise("Token not supplied") unless token
    query_fn = Proc.new {|cql| query_via_http(cql, eol_api_url, token)}
    Graph.new(query_fn)
  end

  # Deprecated - neography is no longer maintained
  # def self.via_neography(url)
  #   connection = Neography::Rest.new(url)
  #   query_fn = Proc.new {|cql| connection.execute_query(cql)}
  #   Graph.new(query_fn)
  # end

  def self.via_neo4j_directly(url)
    # Neography is dead and couldn't get direct_query to work.
    query_fn = Proc.new {|cql| query_via_transaction_api(cql, url)}
    Graph.new(query_fn)
  end

  # Copied from https://github.org/eol/eol_website/lib/trait_bank.rb
  def self.direct_query(q, params={})
    response = nil
    q.sub(/\A\s+/, "")
    response = ActiveGraph::Base.query(q, params, wrap: false)
    return nil if response.nil?

    response_a = response.to_a # NOTE: you must call to_a since the raw response only allows for iterating through once

    # Map neo4j-ruby-driver response to neography-like response
    cols = response_a.first&.keys || []
    data = response_a.map do |row|
      cols.map do |col|
        col_data = row[col]
        if col_data.respond_to?(:properties)
          { 
            'data' => col_data.properties.stringify_keys,
            'metadata' => { 'id' => col_data.id }
          }
        else
          col_data
        end
      end
    end

    result = { 
      'columns' => cols.map { |c| c.to_s }, # hashrocket for string keys
      'data' => data
    }

    result['plan'] = response.summary.plan.to_h unless response.summary.plan.nil?

    result
  end

  # Replace query_fn if desired with TraitBank::query, something using
  # neography, something using ActiveGraph, or the Cypher transaction
  # API

  def initialize(query_fn)
    @query_fn = query_fn
  end

  # This isn't deployed yet but seems a logical service to have
  # TBD: cache this information

  def resource_id_from_name(name)
    puts "** Checking graphdb to find id in graphdb for #{name}"
    # See if the graphdb knows about it already, by name
    r = run_query(
        'MATCH (r:Resource {name: "#{name}"})
         RETURN r.resource_id
         LIMIT 1')
    if r && r.include?("data") && r["data"].length > 0
      id = r["data"][0][0]
      puts("# Yes! Found resource #{id} by name.")
    else
      raise "** No result from resource-by-name query."
      # id = 9000 + rand(1000)
      # puts "** Assigning a random one: #{id}"
    end
  end

  # We also need stage_scp, stage_web if we're doing paged queries!
  # But maybe we're leaving that up to the Paginator class?

  def run_query(cql)
    json = @query_fn.call(cql)
    if json && json["data"].length > 100
      # Throttle load on server
      sleep(1)
    end
    json
  end

  # Do queries via Cypher Transaction API
  # wget "http://localhost:7474/db/neo4j/tx/commit" \
  #   --header "Authorization: Basic <base64 of user:password>" \
  #   --header "Accept: application/json;charset=UTF-8" \
  #   --header "Content-type: application/json" \
  #   --post-data='{"statements":[{"statement":"MATCH (p:Page) RETURN p.page_id LIMIT 1"}]}'

  def self.query_via_transaction_api(cql, server)
    uri = URI("#{server}/db/neo4j/tx/commit")
    use_ssl = (uri.scheme == "https")
    Net::HTTP.start(uri.host, uri.port, :use_ssl => use_ssl) do |http|
      request = Net::HTTP::Post.new(uri)
      request['Authorization'] = "Basic bmVvNGo6bmVvNGoy"
      # was: ;charset=UTF-8
      request['Accept'] = "application/json;charset=UTF-8"
      request['Content-type'] = "application/json"
      # The following only works for url encoding
      #request.set_form_data('statements': [{"statement": cql}])
      request.body = JSON.generate({'statements': [{"statement": cql}]})
      response = http.request(request)
      if response.is_a?(Net::HTTPSuccess)
        blob = JSON.parse(response.body)    # can return nil
        if blob["errors"].size > 0
          STDERR.puts("Errors: #{blob["errors"]}")
          nil
        else
          # Have
          #  {"results":[{"columns":["p.page_id"],"data":[{"row":[1],"meta":[null]}]}],"errors":[]}
          # Want
          #  {"columns": ["p.page_id"], "data": [["1"]]}
          foo = blob["results"][0]
          raise "no results" unless foo
          raise "no data" unless foo["data"]
          rows = foo["data"].collect{|x| x["row"]}
          {"columns" => foo["columns"], "data" => rows}
        end
      else
        # STDERR.puts("Headers: #{request.to_hash.inspect}")
        STDERR.puts("** HTTP response: #{response.code} #{response.message}")
        if response.code >= '300' && response.code < '400'
          STDERR.puts("** Location: #{response['Location']}")
        end
        # Ideally we'd print only those lines that have useful 
        # information (error message and backtrace).
        # /home/jar/g/eol_website/lib/painter.rb:297:in `block in merge': 
        #     undefined method `[]' for nil:NilClass (NoMethodError)
        #   from /home/jar/g/eol_website/lib/painter.rb:280:in `each'
        STDERR.puts(response.body)
        nil
      end
    end
  end


  # Do queries via REST API (deprecated)
  # Copied from painter.rb

  # A particular query method for doing queries using the EOL v3 API over HTTP
  # CODE FORKED FROM traits_dumper.rb ...

  # This uses POST for all commands; probably should use GET
  # (cacheable) for pure queries.
  def self.query_via_http(cql, server, token)
    # Need to be a web client.
    # "The Ruby Toolbox lists no less than 25 HTTP clients."
    escaped = CGI::escape(cql)
    # TBD: Ought to do GET if query is effectless.
    uri = URI("#{server}service/cypher?query=#{escaped}")
    use_ssl = (uri.scheme == "https")
    Net::HTTP.start(uri.host, uri.port, :use_ssl => use_ssl) do |http|
      request = Net::HTTP::Post.new(uri)
      request['Authorization'] = "JWT #{token}"
      request['Accept'] = "application/json"
      response = http.request(request)
      if response.is_a?(Net::HTTPSuccess)
        # REST API format
        # {"columns": ["columnname"], "data": [["value"]]}
        JSON.parse(response.body)    # can return nil
      else
        STDERR.puts("** HTTP response: #{response.code} #{response.message}")
        if response.code >= '300' && response.code < '400'
          STDERR.puts("** Location: #{response['Location']}")
        end
        # Ideally we'd print only those lines that have useful 
        # information (error message and backtrace).
        # /home/jar/g/eol_website/lib/painter.rb:297:in `block in merge': 
        #     undefined method `[]' for nil:NilClass (NoMethodError)
        #   from /home/jar/g/eol_website/lib/painter.rb:280:in `each'
        STDERR.puts(cql)
        STDERR.puts(response.body)
        nil
      end
    end
  end

end
