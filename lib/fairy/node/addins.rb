# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

njob_dir = File.dirname(__FILE__)
subdir = File.basename(File.dirname(njob_dir))
njob_name = File.basename(njob_dir)
for njob in Dir.glob("#{njob_dir}/*.rb")
  base = File.basename(njob)
  case base
  when /18.rb$/
    next if RUBY_VERSION >= "1.9.0"
  when /19.rb$/
    next unless RUBY_VERSION >= "1.9.0"
  end
  require [subdir, njob_name, base].join("/")
end

