# Typical sequence:
#   infer
#   stage
#   publish

require 'resource'
require 'painter'
require 'system'

namespace :paint do

  def testing_resource; 99999; end

  def get_painter
    Painter.new(
      Resource.new(
        system: System.system(ENV['CONF']),
        publishing_id: ENV['ID'] || testing_resource))
  end

  # Ordinary tasks

  desc "Compute inferred relationships and put them on the staging site"
  task :paint do 
    get_painter.paint
  end

  desc "Load inferred relationships from staging site into the graphdb"
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
