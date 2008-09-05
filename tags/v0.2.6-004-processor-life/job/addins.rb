
job_dir = File.dirname(__FILE__)
for job in Dir.glob("#{job_dir}/*.rb")

#  puts "require #{job}"

  require job
end
