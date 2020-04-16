# bin/rails r clobber_resource_ids.rb

# Run this once, after the resources list has been loaded into our
# fresh publishing site from the content repository (`rake sync`), to
# adjust the id of each resource to match what's in some parallel
# publishing site, e.g. eol.org.

# For now you must get the resources.json file manually:
#   wget http://eol.org/resources.json?per_page=1000
# which creates a local file "resources.json?per_page=1000", whose
# name is wired into this script.

# Publishing site
publishing_file = File.read "resources.json?per_page=1000"
data = JSON.parse(publishing_file)
seen = {}

count = 0
nontrivial = 0
wins = 0
dups = 0
missing = 0
data["resources"].each { |resource|
  count += 1
  id = resource["id"]
  name = resource["name"]
  key = name
  STDERR.puts("Processing: #{id} #{key}") if (count % 50).zero?
  next if resource["nodes_count"] == nil
  next if resource["nodes_count"] == 0
  nontrivial += 1
  begin
    # Get record that starts out being a copy of the content resource
    # record...  and turn it into one that matches the other publishing 
    # site
    record = Resource.find_by(name: name)
    if record
      if seen.key?(key)
        r2 = seen[key]
        a = [r2["id"], r2["name"], r2["nodes_count"]]
        b = [id, name, resource["nodes_count"]]
        STDERR.puts("Duplicate key: #{a} #{b}")
        dups += 1
      else
        record.update_attribute(:id, id)
        wins += 1
        seen[key] = resource
      end
    else
      STDERR.puts("Not in db: #{id} #{key}")
      missing += 1
    end
  rescue => e
    STDERR.puts("Exception: #{key} #{e}")
  end
}
STDERR.puts("----")
STDERR.puts("#{count} publishing blobs processed")
STDERR.puts("#{nontrivial} blobs with nodes_count > 0")
STDERR.puts("#{dups} duplicate matched publishing blobs")
STDERR.puts("#{missing} no match in local database")
STDERR.puts("#{wins} local resource records updated content->publishing")
