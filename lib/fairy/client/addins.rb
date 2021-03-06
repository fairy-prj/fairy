# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

job_dir = File.dirname(__FILE__)
subdir = File.basename(File.dirname(job_dir))
job_name = File.basename(job_dir)
for job in Dir.glob("#{job_dir}/*.rb")
  base = File.basename(job)
  case base
  when /18.rb$/
    next if RUBY_VERSION >= "1.9.0"
  when /19.rb$/
    next unless RUBY_VERSION >= "1.9.0"
  end
  require [subdir, job_name, base].join("/")
end

Fairy::post_initialize
