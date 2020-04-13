
# export URL=
# ID=40 SERVER=https://beta.eol.org/ ruby -r ./lib/reaper/reaper.rb -e Reaper.main

require 'yaml'
require 'json'
require 'net/http'
require 'uri'
require_relative 'resource'

class Reaper
  def self.main

    # Marshall all parameters
    config = YAML.load(File.read("config/secrets.yml"))
    publishing_url = ENV['PUBLISH'] || config["development"]["host"]["url"]
    publishing_url += "/" unless publishing_url.end_with?("/")
    repository_url = ENV['REPOSITORY'] || config["development"]["repository"]["url"]
    repository_url += "/" unless repository_url.end_with?("/")
    resource_id = ENV['ID'].to_i
    repository_id = ENV['REPOSITORY_ID']
    opendata_url = ENV['URL']
    opendata_cache = ENV['OPENDATA_CACHE'] || "/tmp/opendata"

    puts "Publishing site is #{publishing_url}"
    puts "Repository site is #{repository_url}"

    # Get the resource record, if any, from the publisher's resource list
    records = JSON.parse(Net::HTTP.get(URI.parse("#{publishing_url}/resources.json")))
    records_index = {}
    records["resources"].each do |record|
      id = record["id"].to_i
      records_index[id] = record
    end
    record = records_index[resource_id]
    if record
      name = record["name"]
      repository_id ||= record["repository_id"]
    else
      name = "?"
      raise("Unknown resource #{resource_id}; REPOSITORY_ID must be specified") unless repository_id
    end
    puts("Resource #{resource_id}/#{repository_id} name is #{name}")

    resource = Resource.new(resource_id, repository_id.to_i)
    resource.bind_to_opendata(opendata_url, opendata_cache)

    # Heavy lifting
    resource.harvest(repository_url)
    # resource.publish(publishing_url)
  end
end
