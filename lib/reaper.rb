
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
    publishing_url = ENV['PUBLISH']
    repository_url = ENV['REPOSITORY']
    workspace_root = ENV['WORKSPACE_ROOT']

    # This resource
    opendata_url = ENV['URL']
    publishing_id = ENV['ID'].to_i
    repository_id = ENV['REPOSITORY_ID']  # may be nil

    resource = Resource.new(workspace_root)
    resource.bind_to_opendata(opendata_url)
    resource.bind_to_publishing(publishing_url, publishing_id)
    resource.bind_to_repository(repository_url, repository_id)
    resource.harvest(repository_url)
  end

end
