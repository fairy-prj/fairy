
njob_dir = File.dirname(__FILE__)
for njob in Dir.glob("#{njob_dir}/*.rb")

#  puts "require #{job}"

  require njob
end
