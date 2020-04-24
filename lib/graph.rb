require 'neography'

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

  def self.via_neography(url)
    connection = Neography::Rest.new(url)
    query_fn = Proc.new {|cql| connection.execute_query(cql)}
    Graph.new(query_fn)
  end

  # Replace query_fn if desired with TraitBank::query, some other
  # call to neography, or to direct access to neo4j

  def initialize(query_fn)
    @query_fn = query_fn
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
