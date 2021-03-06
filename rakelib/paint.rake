# Typical sequence:
#   prepare
#   publish

require 'resource'
require 'painter'
require 'system'

namespace :paint do

  def testing_resource; 99999; end

  def get_painter
    tag = ENV['CONF'] || raise("Please provide env var CONF")
    id = ENV['ID'] || testing_resource
    id = id.to_i
    assem = Assembly.assembly(tag)
    resource = assem.get_instance.get_resource_by_id(id)
    puts "Resource #{id} on #{tag} is understood as #{resource.name}"
    Painter.new(resource, assem)
  end

  # Ordinary tasks

  desc "Branch-paint a resource"
  task :paint => :prepare do 
    get_painter.publish
  end

  desc "Compute inferred relationships and put them on the staging site"
  task :prepare do 
    get_painter.prepare    # = infer + stage
  end

  desc "Transfer inferred relationships from staging site into the graphdb"
  task :publish do 
    get_painter.publish
  end

  desc "remove a resource's inferences"
  task :erase do 
    get_painter.erase
  end

  # Diagnostic tasks

  desc "Show count of number of inferred relationships"
  task :count do 
    get_painter.count
  end

  desc "Quality control checks"
  task :qc do 
    get_painter.qc
  end

  desc "Set up dummy resource for debugging purposes"
  task :populate do 
    get_painter.populate
  end

  desc "List start and stop directives"
  task :show_directives do
    get_painter.show_directives
  end

  # Subtasks

  desc 'a b c d'
  task :infer do 
    get_painter.infer
  end

  desc 'move inferences to staging site'
  task :stage do 
    get_painter.stage
  end

end
