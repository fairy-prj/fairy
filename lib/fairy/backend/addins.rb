# encoding: UTF-8

bjob_dir = File.dirname(__FILE__)
subdir = File.basename(File.dirname(bjob_dir))
bjob_name = File.basename(bjob_dir)
for bjob in Dir.glob("#{bjob_dir}/*.rb")
  base = File.basename(bjob)
  case base
  when /18.rb$/
    next if RUBY_VERSION >= "1.9.0"
  when /19.rb$/
    next unless RUBY_VERSION >= "1.9.0"
  end
  require [subdir, bjob_name, base].join("/")
end


