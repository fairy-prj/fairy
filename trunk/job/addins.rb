

job_dir = File.dirname(__FILE__)
job_name = File.basename(job_dir)
for job in Dir.glob("#{job_dir}/*.rb")
  require job_name+"/"+File.basename(job)
end

Fairy::post_initialize
