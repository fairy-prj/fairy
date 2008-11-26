
job_dir = File.basename(File.dirname(__FILE__))
for job in Dir.glob("#{job_dir}/*.rb")
#  next if job == __FILE__

#  puts "require #{job}"

  require job
end

Fairy::post_initialize
