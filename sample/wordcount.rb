#!/usr/bin/env ruby
# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
# 

require 'rubygems'
require 'fairy'


unless ARGV.size == 2
  $stderr.puts "Usage: #{File.basename($0)} INPUT OUTPUT"
end

input_path = ARGV[0];
output_path = ARGV[1];

unless FileTest.exist?(input_path)
  raise "Input doesn't exist."
  exit(1)
end

if FileTest.exist?(output_path) && output_path.match(/\.vf\z/)
  raise "Cannot override an existing VFile. Please confirm."
end

t0 = Time.now

puts "[#{$$}] START: #{t0}"
puts "[#{$$}]   input:  #{input_path}"
puts "[#{$$}]   output: #{output_path}"

fairy = Fairy::Fairy.new

fairy.def_pool_variable(:errors, :block => %{Array.new})

input = fairy.input input_path
maped = input.mapf(%{|ln| 
  begin
    ln.split
  rescure => e
    @Pool.errors.push([e.message, ln])
    Import::TOKEN_NULLVALUE
  end
})
grouped = maped.group_by(%{|w| w})
counted = grouped.map(%q{|bag| "#{bag.key}\t#{bag.size}"})
counted.output output_path

unless fairy.pool_variable(:errors).size.zero?
  err = fairy.pool_variable(:errors)
  puts "[#{$$}] WARN: #{err.size} error(s) occurred."
  err.each_with_index{|e,i|
    p [i+1] + e
  }
end

t1 = Time.now
puts "[#{$$}] DONE: #{t1} (#{t1-t0} sec)"


