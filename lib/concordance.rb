# This code will no longer work due to many many changes in system.rb
# and friends

class Concordance

  def initialize(system)
    @system = system
  end

  def refresh
    path = @system.get_concordance.get_path
    unified = {}      # maps name to resource record
    if File.exist?(path)
      blob = JSON.parse(File.read(path))
      blob['resources'].each {|r| unified[r['name']] = r}
    else
      puts "** Not found: #{path}"
    end

    def s(name)
      @system.get_location(name)
    end

    process_assembly(s("prod_publishing"),
                     s("prod_repository"),
                     unified) 
    process_assembly(s("beta_publishing"),
                     s("beta_repository"),
                    unified)
    process_db("config/config.yml", unified)    # TBD: capture version info

    new_path = path + ".new"
    File.write(new_path, JSON.pretty_generate({'resources' => unified.values}) + "\n")
    unified.take(10).each do |name, record|
      puts "#{name}"
      record['sources'].each do |url, source|
        puts "  <- #{url}resource/#{source["id"]}"
      end
    end
    puts "? mv -f #{new_path} #{path}"
  end

  # Unified is a hash from name to combined resource record

  def process_assembly(pub_url, repo_url, unified)
    process_db(pub_url, unified)
    process_db(repo_url, unified)
    unified.values.each do |record|
      in_pub = record['sources'][pub_url]
      in_repo = record['sources'][repo_url]
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

  # Currently unused, a different approach...

  def absorb(records, loc)
    records.each do |record|
      name = record["name"]
      if @resources.key?(name)
        puts("# Merging #{name}")
        have = @resources[name]
        have.merge!(record) do |key, val1, val2|
          val1
        end
      else
        # ???? what instance ????
        imp = Resource.new(self, loc, record)
        @resources[name] = imp
      end
    end
    @resources_by_id = {}
    @resources.each{|name,record| @resources_by_id[record["id"]] = record}
  end

  # `unified` maps normalized name to resource record

  def process_db(loc, unified)
    records = loc.get_resource_records
    latest_records = get_serieses(records, loc).sort_by{|r|r["id"].to_i}
    novo = 0
    by_id = {}
    unified.each |key, u_record| do
      if u_record.key?("id")
        by_id[u_record["id"]] = u_record
      end
    end
    latest_records.each do |record|
      name = record['name']
      if unified.key?(name)
        u_record = unified[name]
      else
        novo += 1
        u_record = {'name' => name, 'sources' => {}}
        unified[name] = u_record
      end
      unless u_record.key?("id")
        # Mint a fresh id for this resource; avoid collisions
        id = record["id"]
        while by_key.key?(id) do
          puts "# Collision: #{id}, trying #{id+1}" % id
          id += 1
        end
        u_record["id"] = id
      end
      sources = u_record['sources']    # could be {}
      if sources.key?(loc)
        source = sources[loc]
      else
        source = {}
        sources[loc] = source
      end
      fields = ['id', 'abbr', 'repository_id', 'nodes_count', \
               'landing_page']
      fields.each do |field|
        source[field] = record[field] if record.key?(field)
      end
    end
    puts "# #{novo} new resources"
  end

  # Returns an array of records (so that it can be sorted)

  def get_serieses(records, db)
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


end
