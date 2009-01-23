
bjob_dir = File.dirname(__FILE__)
bjob_name = File.basename(bjob_dir)
for bjob in Dir.glob("#{bjob_dir}/*.rb")
  require bjob_name+"/"+File.basename(bjob)
end


