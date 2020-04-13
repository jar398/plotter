namespace :paint do
  desc 'a b c'
  task :count do 
    Painter.count(ENV['ID'])
  end

  desc 'a b c'
  task :qc do 
    Painter.qc(ENV['ID'])
  end

end
