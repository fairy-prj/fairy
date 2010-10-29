#!/usr/bin/env ruby
# encoding: UTF-8
# 
# Copyright (C) 2007-2010 Rakuten, Inc.
# 

require 'rubygems'
require 'fairy'
require 'optparse'


opt = {:k => 0, :t => /\s+/}

op = OptionParser.new
op.on('-k', '--key=POS', Integer){|v| opt[:k] = v}
op.on('-n', '--numeric-sort'){|v| opt[:n] = v}
op.on('-r', '--reverse', nil, "This must be used with -n."){|v| opt[:r] = v}
op.on('-t', '--separator=SEPARATOR'){|v| opt[:t] = v}
op.parse!(ARGV)


unless ARGV.size == 2
  $stderr.puts op.to_s.sub(/ *\n/, "... INPUT OUTPUT\n")
  exit(1)
end

input_path = ARGV[0];
output_path = ARGV[1];

unless FileTest.exist?(input_path)
  raise "Input doesn't exist."
end

if FileTest.exist?(output_path) && output_path.match(/\.vf\z/)
  raise "Cannot override an existing VFile. Please confirm."
end

if opt[:r] && !opt[:n]
  raise "Cannot specify -r option without -n."
end

if opt[:t].is_a? Regexp
  sep = opt[:t].to_s
else 
  str = eval %{ "#{opt[:t]}" }
  if str.size == 1 && str != "\\"
    sep = (/(?<!\\)#{str}/o).to_s
  else
    sep = str
  end
end

t0 = Time.now

puts "[#{$$}] START: #{t0}"
puts "[#{$$}]   input:  #{input_path}"
puts "[#{$$}]   output: #{output_path}"
puts "[#{$$}]   key: #{opt[:k]}"
puts "[#{$$}]   separator: #{sep}"
puts "[#{$$}]   num-sort: ON" if opt[:n]
puts "[#{$$}]   reverse: ON" if opt[:r]


fairy = Fairy::Fairy.new

fairy.def_pool_variable(:errors, :block => %{Array.new})

input = fairy.input input_path
maped = input.map(%{|ln| 
  @sep_re ||= Regexp.new(#{sep.inspect})
  begin
    sort_key = ln.split(@sep_re)[#{opt[:k]}]
    [sort_key, ln]
  rescue => e
    @Pool.errors.push([e.message, ln])
    Import::TOKEN_NULLVALUE
  end
})

if opt[:n] && opt[:r]
  sorted = maped.sort_by(%{|ary| -ary[0].to_i})
elsif opt[:n]
  sorted = maped.sort_by(%{|ary| ary[0].to_i})
else
  sorted = maped.sort_by(%{|ary| ary[0]})
end

formatted = sorted.map(%{|ary| ary[1]})
formatted.output output_path

unless fairy.pool_variable(:errors).size.zero?
  err = fairy.pool_variable(:errors)
  puts "[#{$$}] WARN: #{err.size} error(s) occurred."
  err.each_with_index{|e,i|
    p [i+1] + e
  }
end

t1 = Time.now
puts "[#{$$}] DONE: #{t1} (#{t1-t0} sec)"


