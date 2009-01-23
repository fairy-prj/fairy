
njob_dir = File.dirname(__FILE__)
njob_name = File.basename(njob_dir)
for njob in Dir.glob("#{njob_dir}/*.rb")
  require njob_name+"/"+File.basename(njob)
end

