# encoding: UTF-8

job_dir = File.dirname(__FILE__)
job_name = File.basename(job_dir)
for job in Dir.glob("#{job_dir}/*.rb")
  base = File.basename(job)
  case base
  when /18.rb$/
    next if RUBY_VERSION >= "1.9.0"
  when /19.rb$/
    next unless RUBY_VERSION >= "1.9.0"
  end
  require job_name+"/"+base
end

Fairy::post_initialize
