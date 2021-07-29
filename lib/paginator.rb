# TBD: Integrate this with the 'table' class

# Utility for overcoming Cypher query time limits when executing
# long-running Cypher queries.  We replace the original query with a
# number of smaller queries and LIMIT.  The smaller queries yield a
# set of "chunks" that are assembled into a single CSV file of
# results.

# This assumes that each succeeding chunk takes up where the previous
# one ends.  That assumption might be violated if the graph database
# is changing while the query is running, so some caution or
# skepticism should be exercised.

# If the script is interrupted, it can be run again and it will use
# chunk files created on a previous run, if the previous run was in
# the same calendar month.  This is a time saving measure.

# Thanks to Bill Tozier (https://github.com/vaguery) for code review;
# but he is not to be held responsible for anything you see here.

require 'csv'
require 'fileutils'

# The following are required if you want to be an HTTP client:
require 'net/http'
require 'json'
require 'cgi'

class Paginator

  # The query_fn takes a CQL query as input, executes it, and returns
  # a result set.  The result set is returned in the idiosyncratic
  # form delivered by neo4j.  The implementation of the query_fn might
  # use neography, or the EOL web API, or any other method for
  # executing CQL queries.

  def initialize(graph)
    @graph = graph
  end

  # -----

  # supervise_query: generate a set of 'chunks', then put them
  # together into a single .csv file.  Returns [path, n] where path is
  # the path to the .csv file and n is an approximation to the number
  # of rows in the .csv file.  (-1 means error, 0 means no rows, >0 
  # means usually at least 1 row)

  # A chunk (or 'part') is the result set of a single cypher query.
  # The queries are in a single supervise_query call are all the same,
  # except for the value of the SKIP parameter.

  # The reason for this is that the result sets for some queries are
  # too big to capture with a single query, due to timeouts or other
  # problems.

  # Each chunk is placed in its own file.  If a chunk file already
  # exists the query is not repeated - the results from the previous
  # run are used directly without verification.

  # Returns the name of the .csv file (or nil on error), if any, and the number of 
  # rows written (or a larger number, if cached files are used).

  def supervise_query(query, headings, chunksize, csv_path,
                      skipping: true,
                      keep_chunks: false,
                      create_empty: true)
    if File.exist?(csv_path)
      #STDERR.puts "Using previously generated file #{csv_path}"
      [csv_path, 1]
    else
      # Create a directory csv_path.chunks to hold the chunks
      chunks_dir = csv_path + ".chunks"
      if Dir.exist?(chunks_dir) && Dir.entries(chunks_dir).length > 2
        STDERR.puts "There are cached results in #{chunks_dir}"
      end
      begin
        # count = total number of rows
        chunks, count = get_query_chunks(query, headings, chunksize, csv_path, skipping)
        # This always writes a .csv file to csv_path, even if it's empty.
        if assemble_chunks(chunks, headings, csv_path,
                           keep_chunks: true, create_empty: create_empty)
          # Cleanup: Flush the chunks directory if it's empty
          if Dir.exist?(chunks_dir) && Dir.entries(chunks_dir).length <= 2
            FileUtils.rmdir chunks_dir
          end
          [csv_path, count]
        else
          [nil, 0]              # hmph.  flush the 'assemble' parameter???
        end
      rescue => e
        STDERR.puts "** Failed to generate #{csv_path}"
        STDERR.puts "** Exception: #{e}\n#{e.message}"
        STDERR.puts e.backtrace.join("\n")
        [nil, -1]
      end
    end
  end

  # Ensure that all the chunks files for a table exist, using Neo4j to
  # obtain them as needed.
  # Returns a list of paths (file names for the chunks) and a count of
  # the total number of rows (not always accurate).
  # A file will be created for every successful query, but the pathname
  # is only included in the returned list if it contains at least one row.
  # Always returns at least one chunk, which might be empty.

  def get_query_chunks(query, headings, chunksize, csv_path, skipping)
    limit = (chunksize == nil ? 1000000000 : chunksize.to_i)
    chunks = []
    skip = 0
    total = 0
    tick = 0

    # Keep doing queries until no more results are returned, or until
    # something goes wrong
    while true
      # Fetch it one chunk at a time
      basename = (chunksize == nil ? "#{skip}" : "#{skip}_#{chunksize}")
      chunks_dir = csv_path + ".chunks"
      chunk_path = File.join(chunks_dir, "#{basename}.csv")
      if File.exist?(chunk_path)
        chunks.push(chunk_path) if File.size(chunk_path) > 0
        skip += limit
      else
        tick += 1
        STDERR.puts "#{tick} #{chunk_path}" if tick % 25 == 0
        FileUtils.mkdir_p File.dirname(chunk_path)
        #STDERR.puts("Getting #{chunk_path} #{skip} #{limit}")
        whole_query = query
        whole_query = whole_query + " SKIP #{skip}" if skipping
        whole_query = whole_query + " LIMIT #{limit}"
        # This might raise an exception, in which case the whole thing aborts.
        # Retry logic is now embedded in run_query.
        result = @graph.run_query(whole_query)
        sleep(1) # ***** Throttle requests to decrease server load *****

        got = result["data"].length
        # The skip == 0 test is a kludge that fixes a bug where the
        # header row was being omitted in some cases ???
        #STDERR.puts(result) if got == 0
        if got > 0
          emit_csv(result, headings, chunk_path)
          chunks.push(chunk_path)
        end
        #STDERR.puts("Got #{got} limit #{limit} skip #{skip} for #{chunk_path}")
        if got < limit
          #STDERR.puts("Chunk size #{got} < #{limit} for #{chunk_path}")
        end
        total += got
        break if got < limit
        #STDERR.puts("Continuing #{got} limit #{limit} skip #{skip} for #{chunk_path}")
        skip += got
      end
      break if chunksize == nil
    end
    [chunks, total]
  end

  # Combine the chunks files (for a single table) into a single master
  # .csv file which is stored at csv_path.
  # Always returns csv_path to a file (perhaps empty) will be found.
  # chunks is ["foo.csv.chunks/something.csv", ...]

  def assemble_chunks(chunks,
                      headings,
                      csv_path,
                      keep_chunks: false,
                      create_empty: true)
    # Concatenate all the chunks together
    chunks_dir = csv_path + ".chunks"
    if chunks.size == 0
      if create_empty 
        STDERR.puts("Creating empty #{csv_path}")
        raise "Shouldn't happen" if csv_path.include? "predicates"
        CSV.open(csv_path, "wb") do |csv|
          csv << headings if headings != nil
        end
      else
        csv_path = nil
      end
    elsif chunks.size == 1
      # "optimization"
      FileUtils.mv chunks[0], csv_path
      FileUtils.rmdir chunks_dir # should be empty
    else
      temp = csv_path + ".new"
      tails = chunks.drop(1).map { |path| "tail -n +2 #{path}" }
      more = tails.join(' && ')
      command = "(cat #{chunks[0]}; #{more}) >#{temp}"
      system command
      FileUtils.mv temp, csv_path
    end
    if Dir.exist?(chunks_dir) && not(keep_chunks)
      FileUtils.rm_rf chunks_dir
    end
    csv_path
  end

  # Utility - convert native cypher output form to CSV
  def emit_csv(start, headings, path)
    # Sanity check the result
    if start["columns"] == nil or start["data"] == nil
      STDERR.puts "** failed to write #{path}; result = #{start}"
      return nil
    end
    temp = path + ".new"
    FileUtils.mkdir_p File.dirname(temp)
    # Should use the Table class here
    csv = CSV.open(temp, "wb")
    if headings != nil
      csv << headings
    else
      csv << start["columns"]
    end
    count = start["data"].length
    if count > 0
      #STDERR.puts "writing #{count} csv records to #{temp}"
      start["data"].each do |row|
        csv << row
      end
    end
    csv.close
    FileUtils.mv temp, path
    path
  end
end
