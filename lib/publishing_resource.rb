
require 'resource'

class PublishingResource < Resource

  def get_repository_resource
    @location.assert_publishing
    rid = @config["repository_id"]
    return nil unless rid
    @location.get_repository_location.get_own_resource(rid)
  end

end
