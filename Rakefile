require "bundler/gem_tasks"
require "rake/testtask"

# Consider:
#   wget -O config/prod-publishing/resources.json \
#      "https://eol.org/resources.json?per_page=10000"

Rake.add_rakelib 'lib/tasks'

Rake::TestTask.new(:test) do |t|
  t.libs << "test"
  t.libs << "lib"
  t.test_files = FileList["test/**/*_test.rb"]
end

task :default => :test
