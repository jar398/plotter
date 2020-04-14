require 'resource'

namespace :reap do
  desc 'a b c'
  task :reap do 
    resource = Resource.new(
      workspace_root: ENV['WORKSPACE_ROOT'],
      opendata_url: ENV['URL'],
      publishing_id: ENV['ID'],
      publishing_url: ENV['PUBLISH'],
      repository_id: ENV['REPOSITORY_ID'],
      repository_url: ENV['REPOSITORY'])
    resource.harvest
  end

  task :stage do
    resource = Resource.new(
      workspace_root: ENV['WORKSPACE_ROOT'],
      publishing_id: ENV['ID'],
      stage_scp: ENV['STAGE_SCP'])
    resource.stage
  end

  task :erase do
    resource = Resource.new(
      workspace_root: ENV['WORKSPACE_ROOT'],
      publishing_id: ENV['ID'],
      publishing_url: ENV['PUBLISH'],
      publishing_token: ENV['TOKEN'],
      stage_url: ENV['STAGE_URL'])
    resource.erase
  end

  task :publish do
    resource = Resource.new(
      workspace_root: ENV['WORKSPACE_ROOT'],
      publishing_id: ENV['ID'],
      publishing_url: ENV['PUBLISH'],
      publishing_token: ENV['TOKEN'],
      stage_url: ENV['STAGE_URL'])
    resource.publish
  end
end

