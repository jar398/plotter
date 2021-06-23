require 'base64'

class Graph

  def self.read_timeout; 10; end

  # Talk to neo4j directly using the neo4j transaction API.

  def self.via_neo4j_directly(url)
    # Neography is dead and couldn't get direct_query to work.
    query_fn = Proc.new {|cql| query_via_transaction_api(cql, url)}
    Graph.new(query_fn)
  end

  # Method for accessing the graph via EOL v3 API.  This proxies
  # neo4j requests via the v3 EOL web API (eol_website app /
  # publishing server)

  def self.via_http(eol_api_url, token)
    eol_api_url += "/" unless eol_api_url.end_with?("/")
    raise("Token not supplied") unless token
    query_fn = Proc.new {|cql| query_via_http(cql, eol_api_url, token)}
    Graph.new(query_fn)
  end

  # query_fn can be TraitBank::query, or something using ActiveGraph,
  # or something that uses the Cypher transaction API

  def initialize(query_fn)
    @query_fn = query_fn
  end

  # We also need stage_scp, stage_web if we're doing paged queries!
  # But maybe we're leaving that up to the Paginator class?
  # Returns nil on error, after printing message - is this right?

  # Possible exceptions: (may be different for direct-to-neo4j)
  #   Connection level:
  #     Errno::ECONNREFUSED: Failed to open TCP connection to localhost:7474
  #     Errno::ECONNREFUSED: Connection refused - connect(2) for "localhost" port 7474
  #     End of file reached (June 2021)
  #     Connection reset by peer (June 2021)
  #     Connection refused (June 2021)
  #   HTTP level:
  #     Net::HTTPBadGateway = 502 Bad Gateway (nginx) (May 2021)
  #     Net::HTTPGatewayTimeout
  #     Net::HTTPServerException: 400 "Bad Request"
  #       - cypher error, or missing LIMIT
  #       - auth problems show up with a different 4xx
  #     Other HTTP error (3xx, 4xx, 5xx)
  #   Neo4j level:
  #     cypher syntax - do not retry

  def run_query(cql, tries = 3, retry_interval = 1)
    json = nil
    while tries > 0 do
      retr = false
      loser = nil
      tries -= 1
      begin
        json = @query_fn.call(cql)
      rescue Errno::ECONNREFUSED => e
        retr = true; loser = e
      rescue Net::HTTPGatewayTimeout => e
        retr = true; loser = e
      rescue Net::HTTPBadGateway => e
        retr = true; loser = e
      rescue => e
        loser = e
        STDERR.puts "** Exception class = #{e.class}"
      end
      if retr and tries > 0
        STDERR.puts "** #{e}"
        STDERR.puts "** Will retry after #{retry_interval} seconds, up to #{tries} times"
        sleep(retry_interval)
        loser = nil
      end
      if loser
        raise loser 
      else
        if json && json["data"].length > 100
          # Throttle load on server
          sleep(1)
        end
      end
    end
    json
  end

  # Do queries via neo4j's Cypher Transaction API
  # wget "http://localhost:7474/db/neo4j/tx/commit" \
  #   --header "Authorization: Basic <base64 of user:password>" \
  #   --header "Accept: application/json;charset=UTF-8" \
  #   --header "Content-type: application/json" \
  #   --post-data='{"statements":[{"statement":"MATCH (p:Page) RETURN p.page_id LIMIT 1"}]}'

  def self.query_via_transaction_api(cql, server)
    uri = URI("#{server}/db/neo4j/tx/commit")
    use_ssl = (uri.scheme == "https")
    # https://stackoverflow.com/questions/15157553/set-read-timeout-for-the-service-call-in-ruby-nethttp-start
    # Cannot set headers using HTTP.start.
    # Can set them with using HTTP.new?
    session = Net::HTTP.new(uri.host, uri.port, :use_ssl => use_ssl,
                            :read_timeout => read_timeout)
    session.start do |http|
      request = Net::HTTP::Post.new(uri)

      body = JSON.generate({'statements': [{"statement": cql}]})
      request = http.post(uri, body)
      # The *request* might be of type Net::HTTPClientError (or any other HTTP error)
      raise "HTTP client error" \
        if request.kind_of? Net::HTTPClientError

      request['Authorization'] = "Basic #{Base64.encode64(uri.userinfo).strip}"
      request['Accept'] = "application/json;charset=UTF-8"
      request['Content-type'] = "application/json"
      request.body = body
      response = http.request(request)
      puts response.class
      response
    end

    # Raise exception if not a 200
    response.value()

    begin
        blob = JSON.parse(response.body)    # can return nil
        if blob["errors"].size > 0
          raise Neo4jError.new(blob)
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
    end
  end


  # A particular query method for doing queries using the EOL v3 API
  # over HTTP.  CODE FORKED FROM traits_dumper.rb ...

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
      # Raise exception if not 200
      response.value()
      if response.is_a?(Net::HTTPSuccess)
        # EOL REST API v3 format
        # {"columns": ["columnname"], "data": [["value"]]}
        JSON.parse(response.body)
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

  # Copied from https://github.org/eol/eol_website/lib/trait_bank.rb .
  # I couldn't get this code to work in plotter, probably because of a
  # missing or incorrect dependency.  But this code ought to work if
  # run inside the eol_website application.

  def self.direct_query(q, params={})
    response = nil
    q.sub(/\A\s+/, "")
    response = ActiveGraph::Base.query(q, params, wrap: false)
    # Not sure what activegraph does on HTTP or Neo4j error?
    raise "ActiveGraph error" if response.nil?

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

  class Neo4jError < RuntimeError
    def initialize(blob)
      @blob = blob
    end
    def message
      begin
        errors = @blob["errors"]
        reports = errors.map{|x| "#{x["code"]} #{x["message"]}"}
        "Neo4j error(s) - #{reports.join(" | ")}"
      rescue
        STDERR.puts "** Unprintable exception"
      end
    end
  end

end
