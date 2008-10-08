
njob_dir = File.dirname(__FILE__)
for njob in Dir.glob("#{njob_dir}/*.rb")
  next if njob == __FILE__

#  puts "require #{job}"

  require njob
end
