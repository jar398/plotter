require 'base64'
require 'faraday'
require 'faraday_middleware'

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

  def self.via_eol_server(eol_api_url, token)
    eol_api_url += "/" unless eol_api_url.end_with?("/")
    raise("Token not supplied") unless token
    query_fn = Proc.new {|cql| query_via_eol_server(cql, eol_api_url, token)}
    Graph.new(query_fn)
  end

  # query_fn can be TraitBank::query, or something using ActiveGraph,
  # or something that uses the Cypher transaction API

  def initialize(query_fn)
    @query_fn = query_fn
  end

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

  def run_query(cql, tries = 3, retry_interval = 2, doze = 1)
    json = nil
    while tries > 0 do
      retr = false
      loser = nil
      tries -= 1
      begin
        json = @query_fn.call(cql)
        raise "Null response ???" if json == nil
        raise Neo4jError.new(json) if Graph.errorful(json)
        # Throttle requests to decrease server load
        sleep(doze) if doze > 0
        json
      rescue Faraday::ConnectionFailed => e
        retr = true; loser = e
      rescue Errno::ECONNREFUSED => e
        retr = true; loser = e
      rescue Retriable => e
        retr = true; loser = e
      # Net::HTTPGatewayTimeout and so on ...
      rescue => e
        loser = e
      end
      if retr and tries > 0
        STDERR.puts "** #{e}"
        STDERR.puts "** Will retry after #{retry_interval} seconds, up to #{tries} times"
        sleep(retry_interval)
      elsif loser
        raise loser 
      end
    end
  end

  # Do queries via neo4j's Cypher Transaction API
  # wget "http://localhost:7474/db/neo4j/tx/commit" \
  #   --header "Authorization: Basic <base64 of user:password>" \
  #   --header "Accept: application/json;charset=UTF-8" \
  #   --header "Content-type: application/json" \
  #   --post-data='{"statements":[{"statement":"MATCH (p:Page) RETURN p.page_id LIMIT 1"}]}'

  def self.query_via_transaction_api(cql, server)
    uri = URI("#{server}/db/neo4j/tx/commit")
    path = uri.path
    userinfo = uri.userinfo
    blob = nil

    Faraday::Connection.new do |conn|
      conn.use FaradayMiddleware::FollowRedirects
      conn.adapter(:net_http) # NB: Last middleware must be the adapter
      # Make a request
      headers = {}
      headers['Authorization'] = "Basic #{Base64.encode64(userinfo).strip}"
      headers['Accept'] = "application/json;charset=UTF-8"
      headers['Content-type'] = "application/json"
      body = JSON.generate({'statements': [{"statement": cql}]})
      response = conn.post(uri, body = body, headers = headers)
      code = response.status
      message = response.reason_phrase
      maybe_raise_http_error(code, message)
      # Success
      blob = JSON.parse(response.body)    # can return nil
      if errorful("errors")
        blob
      else
        # Have
        #  {"results":[{"columns":["p.page_id"],"data":[{"row":[1],"meta":[null]}]}],"errors":[]}
        # Want
        #  {"columns": ["p.page_id"], "data": [["1"]]}
        results = blob["results"][0]
        raise "No results" unless results
        raise "No data" unless results["data"]
        rows = results["data"].collect{|x| x["row"]}
        blob = {"columns" => results["columns"], "data" => rows}
      end
    end
    blob
  end

  # A particular query method for doing queries using the EOL v3 API
  # over HTTP.  CODE FORKED FROM traits_dumper.rb ...

  # TBD: This uses POST for all commands; probably should use GET
  # (cacheable) for pure queries.

  def self.query_via_eol_server(cql, server, token)
    # Need to be a web client.
    # "The Ruby Toolbox lists no less than 25 HTTP clients."
    escaped = CGI::escape(cql)
    # was: uri = URI("#{server}service/cypher?query=#{escaped}")
    path = "service/cypher?query=#{escaped}"

    blob = nil
    Faraday::Connection.new do |conn|
      conn.use FaradayMiddleware::FollowRedirects
      conn.adapter(:net_http) # NB: Last middleware must be the adapter
      headers = {}
      headers['Authorization'] = "JWT #{token}"
      headers['Accept'] = "application/json"
      # no body
      response = conn.post(path,
                           headers = {:headers=>headers})
      code = response.status
      message = response.reason_phrase
      # do we need to handle redirects??
      maybe_raise_http_error(code, message)
      # Success
      blob = JSON.parse(response.body)    # can return nil
    end
    blob
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

  def self.errorful(blob)
    blob.include?("errors") && blob["errors"].size > 0
  end

  class Neo4jError < RuntimeError
    def initialize(blob)
      @blob = blob
    end
    def message
      begin
        raise "Erroneous error #{@blob}" unless errorful("errors")
        errors = @blob["errors"]
        reports = errors.map{|x| "#{x["code"]} #{x["message"]}"}
        "Neo4j error(s) - #{reports.join(" | ")}"
      rescue
        "** Unprintable Neo4jError exception"
      end
    end
  end

  def self.maybe_raise_http_error(code, message)
    if code == nil
      raise "HTTP request not made ???"
    elsif code == 200
      nil
    elsif code == 408 || code == 502 || code == 503 || code == 504
      # HTTP 408 Request Timeout
      # HTTP 502 Bad Gateway
      # HTTP 503 Service Unavailable
      # HTTP 504 Gateway Timeout
      raise Retriable.new(code, message)
    elsif code >= '300' && code < '400'
      raise "HTTP redirect: #{code} #{message}\n  Location: #{response['Location']}"
    else
      # Also include selected info from response.body ?
      raise "HTTP response: #{code} #{message}"
    end
  end

  class Retriable < RuntimeError
    def initialize(code, message)
      @code = code
      @message = message
    end
    def message
      "HTTP error - #{@code} #{@message}"
    end
  end

end
