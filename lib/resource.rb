
class Resource

  def initialize(rec, loc)
    @location = loc
    raise "gotta have a name at least" unless rec["name"]
    @config = rec               # Publishing resource record (from JSON/YAML)
  end

  # ---------- Various 'identifiers'...

  def name; @config["name"]; end
  def id; @config["id"]; end
  def location; @location; end

  # ---------- 

  # Path to be combined with workspace root or staging URL
  def relative_path(basename)
    @location.relative_path(File.join("resources",
                                      id.to_s,
                                      basename))
  end

  def workspace_path(relative)
    @location.workspace_path(relative)
  end

  # This is not a function of the resource or location... maybe
  # shouldn't even be a method on this class
  def staging_url(relative)
    # error if resource id not in relative string??
    @location.staging_url(relative)
  end

  # Transitional kludge while adapting repo resource methods for
  # methods on graph resources?
  def get_repository_resource
    self
  end

end
