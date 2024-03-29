require 'base64'
require 'faraday'
require 'faraday_middleware'

class Graph

  def self.read_timeout; 100; end
  def self.open_timeout; 20; end

  # Talk to neo4j directly using the neo4j transaction API.

  def self.via_neo4j_directly(graphdb_name, url, user, password)
    raise "No neo4j user specified" unless user
    raise "No neo4j password specified" unless password
    uri = URI("#{url}/db/#{graphdb_name}/tx/commit")
    auth = "Basic #{Base64.encode64("#{user}:#{password}").strip}"
    query_fn = Proc.new {|cql| query_via_transaction_api(cql, uri, auth)}
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

  def run_query(cql, tries = 3, retry_interval = 20)
    json = nil
    while tries > 0 do
      retr = false
      loser = nil
      tries -= 1
      begin
        json = @query_fn.call(cql)
        raise "Null response ???" if json == nil
        raise Neo4jError.new(json) if Graph.errorful(json)    # could be a timeout
      rescue Faraday::ConnectionFailed => e
        retr = true; loser = e
      rescue Errno::ECONNREFUSED => e
        retr = true; loser = e
      rescue Faraday::TimeoutError => e
        STDERR.puts("Read timeout is #{Graph.read_timeout}")
        retr = true; loser = e
      rescue Net::ReadTimeout
        STDERR.puts("Net:: timeout is default (60s??)")
        retr = true; loser = e
      rescue Retryable => e
        retr = true; loser = e
      # Net::HTTPGatewayTimeout and so on ...
      rescue Neo4jError => e
        loser = e
        STDERR.puts("Neo4j error... #{loser}")
        retr = true if Graph.is_timeout(loser)
      rescue => e
        retr = true if Graph.is_timeout(loser)
        loser = e
      end
      if retr and tries > 0
        STDERR.puts "** #{loser}"
        STDERR.puts "** Will retry after #{retry_interval} seconds, up to #{tries} times"
        sleep(retry_interval)
      elsif loser
        raise loser 
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

  def self.query_via_transaction_api(cql, uri, auth)
    path = uri.path
    blob = nil

    Faraday::Connection.new do |conn|
      conn.use FaradayMiddleware::FollowRedirects
      conn.adapter(:net_http) # NB: Last middleware must be the adapter
      # Make a request
      headers = {}
      headers['Authorization'] = "Basic #{auth}"
      headers['Accept'] = "application/json;charset=UTF-8"
      headers['Content-type'] = "application/json"
      body = JSON.generate({'statements': [{"statement": cql}]})
      response = conn.post(uri, body = body, headers = headers) do |req|
        req.options.timeout = Graph.read_timeout
        req.options.open_timeout = open_timeout
      end
      code = response.status
      message = response.reason_phrase
      maybe_raise_http_error(uri, code, message, response.body)
      # Success
      blob = JSON.parse(response.body)    # can return nil
      if errorful(blob)
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
    uri = URI("#{server}service/cypher")

    blob = nil
    Faraday::Connection.new do |conn|
      conn.use FaradayMiddleware::FollowRedirects
      conn.adapter(:net_http) # NB: Last middleware must be the adapter
      headers = {}
      headers['Authorization'] = "JWT #{token}"
      headers['Accept'] = "application/json"
      # no body
      # Do post or get depending on command???
      response = conn.post(uri, body = "query=#{escaped}", headers = headers) do |req|
        req.options.timeout = Graph.read_timeout
        req.options.open_timeout = open_timeout
      end
      code = response.status
      message = response.reason_phrase
      # do we need to handle redirects??
      maybe_raise_http_error(uri, code, message, response.body)
      # Success
      blob = JSON.parse(response.body)    # can return nil
    end
    blob
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

  # For a neo4j timeout, the error "code" is
  # Neo.DatabaseError.Statement.ExecutionFailed, and the "message"
  # looks like this:
  #   "The transaction has been terminated. Retry your operation in a
  #   new transaction, and you should see a successful result. The
  #   transaction has not completed within the specified timeout
  #   (dbms.transaction.timeout). You may want to retry with a longer
  #   timeout."
  # The only way to distinguish a timeout (which is retryable) from a
  # non-timeout (which isn't) is by string search, as far as I can tell.

  def self.is_timeout(exc)
    return false if exc == nil
    mess = exc.message
    (mess.include?("dbms.transaction.timeout") ||
     mess.include?("dbms.lock.acquisition.timeout"))
  end

  class Neo4jError < RuntimeError
    def initialize(blob)
      if Graph.errorful(blob)
        @blob = blob
      else
        raise "Erroneous error #{blob}"
      end
    end
    def message
      begin
        errors = @blob["errors"]
        reports = errors.map{|x| "#{x["code"]} #{x["message"]}"}
        "Neo4j error(s) - #{reports.join(" | ")}"
      rescue
        "** Unprintable Neo4jError message exception"
      end
    end
  end

  def self.maybe_raise_http_error(uri, code, message, body)
    if code == nil
      raise "HTTP request not made ???"
    elsif code == 200
      nil
    elsif code == 408 || code == 502 || code == 503 || code == 504
      # HTTP 408 Request Timeout
      # HTTP 502 Bad Gateway
      # HTTP 503 Service Unavailable
      # HTTP 504 Gateway Timeout    - e.g. neo4j timeout via EOL proxy
      raise Retryable.new(code, message)
    elsif code >= 300 && code < 400
      raise "HTTP redirect: #{code} #{message}\n  URI: #{uri.to_s}\n  Location: #{response['Location']}"
    else
      # response.body is too much information - subset it somehow ???
      STDERR.puts("--begin http response body--\n#{body}\n--end http response body--")
      raise "HTTP response: #{code} #{message}\n  URI: #{uri.to_s}"
    end
  end

  class Retryable < RuntimeError
    def initialize(code, message)
      @code = code
      @message = message
    end
    def message
      "HTTP error - #{@code} #{@message}"
    end
  end

end
