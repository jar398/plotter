
desc "Create or refresh config/resources.yml"

task :refresh do

  def get(server_base_url)      # Returns an array
    url = "#{server_base_url}resources.json?per_page=10000"
    puts "GET #{url}"
    blob = JSON.parse(Net::HTTP.get(URI.parse(url)))
    blob["resources"]
  end

  @id = 1

  # `unified` maps normalized name to resource record

  def process_db(db, unified)
    novo = 0
    latest_records = get_serieses(db).sort_by{|r|r["id"].to_i}
    latest_records.each do |record|
      name = record['name']
      if unified.key?(name)
        u_record = unified[name]
      else
        novo += 1
        u_record = {'id' => @id, 'name' => name, 'sources' => []}
        unified[name] = u_record
        @id += 1
      end
      sources = u_record['sources']
      source = sources.detect{|s|s['server']==db}
      unless source
        source = {'server' => db}
        sources.push(source)
      end
      fields = ['id', 'abbr', 'repository_id', 'nodes_count']
      fields.each do |field|
        source[field] = record[field] if record.key?(field)
      end
    end
    puts "# #{novo} new resources"
  end

  # Returns an array of records (so that it can be sorted)

  def get_serieses(db)
    records = get(db)           # Array
    puts "#{records.size} records from #{db}"

    latest_versions = []
    serieses = records.group_by do |record|
      name = record['name']
      raise "No name for record #{record['id']} in #{db}" unless name
      # Sort those with node count of 0 before those with no node
      # count or positive node count
      [name, (record['nodes_count'] != 0)]
    end
    puts "#{serieses.length} serieses"
    longest = 0
    serieses.each do |key, group|
      name, nodey = key
      g = group.sort_by do |record|
        record["id"].to_i
      end    # nodey before nodeful; older first
      longest = [longest, g.length].max
      if g.length > 1
        g.zip(g.drop(1)).each do |rec, succ|
          succ["replaces"] = rec if succ
        end
      end
      latest = g.last
      latest_versions.push(latest)
    end
    puts "longest series length = #{longest}"
    latest_versions
  end

  # Unified is a hash from name to combined resource record

  def process_assembly(pub_url, repo_url, unified)
    process_db(pub_url, unified)
    process_db(repo_url, unified)
    unified.values.each do |record|
      in_pub = record['sources'].detect{|r|r['server'] == pub_url}
      in_repo = record['sources'].detect{|r|r['server'] == repo_url}
      if in_pub and in_repo
        repo_id = in_repo['id']
        putative_id = in_pub['repository_id']
        if not(putative_id)
          puts "** No repository_id for pub id #{in_pub['id']}"
        elsif putative_id != repo_id
          err = "** Repository_id #{putative_id} is not latest version #{repo_id}"
          puts err
          record['note'] = err
        end
      end
    end
    unified
  end

  path = "config/resources.json"
  unified = {}      # maps name to resource record
  if File.exists?(path)
    blob = JSON.parse(File.read(path))
    blob['resources'].each {|r| unified[r['name']] = r}
  else
    puts "Not found: #{path}"
  end

  process_assembly("https://beta.eol.org/",
                   "https://beta-repo.eol.org/",
                  unified)
  process_assembly("https://eol.org/",
                   "https://content.eol.org/",
                   unified) 

  new_path = path + ".new"
  File.write(new_path, JSON.pretty_generate({'resources' => unified.values}) + "\n")
  unified.take(10).each do |name, record|
    puts "#{name}"
    record['sources'].each do |source|
      puts "  <- #{source['server']}resource/#{source['id']}"
    end
  end
  puts "? mv -f #{new_path} #{path}"
end
