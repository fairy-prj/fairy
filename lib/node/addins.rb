# encoding: UTF-8

njob_dir = File.dirname(__FILE__)
njob_name = File.basename(njob_dir)
for njob in Dir.glob("#{njob_dir}/*.rb")
  base = File.basename(njob)
  case base
  when /18.rb$/
    next if RUBY_VERSION >= "1.9.0"
  when /19.rb$/
    next unless RUBY_VERSION >= "1.9.0"
  end
  require njob_name+"/"+base
end

