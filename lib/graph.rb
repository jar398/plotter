
class Graph

  def initialize(server, token)
    @query_fn = Proc.new {|cql| query_via_http(server, token, cql)}
  end

  # also stage_scp, stage_web

  def run_query(cql)
    # TraitBank::query(cql)
    json = @query_fn.call(cql)
    if json && json["data"].length > 100
      # Throttle load on server
      sleep(1)
    end
    json
  end

  # Copied from ../painter.rb

  # A particular query method for doing queries using the EOL v3 API over HTTP
  # CODE COPIED FROM traits_dumper.rb - we might want to factor this out...

  def self.query_via_http(server, token, cql)
    # Need to be a web client.
    # "The Ruby Toolbox lists no less than 25 HTTP clients."
    escaped = CGI::escape(cql)
    # TBD: Ought to do GET if query is effectless.
    uri = URI("#{server}service/cypher?query=#{escaped}")
    use_ssl = (uri.scheme == "https")
    Net::HTTP.start(uri.host, uri.port, :use_ssl => use_ssl) do |http|
      request = Net::HTTP::Post.new(uri)
      request['Authorization'] = "JWT #{token}"
      response = http.request(request)
      if response.is_a?(Net::HTTPSuccess)
        JSON.parse(response.body)    # can return nil
      else
        STDERR.puts("** HTTP response: #{response.code} #{response.message}")
        if response.code >= '300' && response.code < '400'
          STDERR.puts("** Location: #{response["Location"]}")
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
