# Typical sequence:
#   infer
#   stage
#   publish

require 'resource'
require 'painter'
require 'system'

namespace :paint do

  def testing_resource; 99999; end

  def get_resource
    Resource.new(
      system: System.system(ENV['CONF']),
      publishing_id: ENV['ID'] || testing_resource)
  end

  desc 'a b c'
  task :count do 
    Painter.new(get_resource).count
  end

  desc 'a b c'
  task :qc do 
    Painter.new(get_resource).qc
  end

  desc 'a b c d'
  task :infer do 
    Painter.new(get_resource).infer
  end

  desc 'move inferences to staging site'
  task :stage do 
    Painter.new(get_resource).stage
  end

  desc 'store inferences from staging site into the graphdb'
  task :publish do 
    Painter.new(get_resource).publish
  end

  desc 'remove inferences'
  task :clean do 
    Painter.new(get_resource).clean
  end

  desc 'for debugging'
  task :populate do 
    Painter.new(get_resource).populate
  end

end
