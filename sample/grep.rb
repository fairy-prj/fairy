#!/usr/bin/env ruby
# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

require 'rubygems'
require 'fairy'


unless ARGV.size == 3
  $stderr.puts "Usage: #{File.basename($0)} PATTERN INPUT OUTPUT"
  exit(1)
end

pattern = Regexp.new(eval %{ "#{ARGV[0]}" })

input_path = ARGV[1];
output_path = ARGV[2];

unless FileTest.exist?(input_path)
  raise "Input doesn't exist."
end

if FileTest.exist?(output_path) && output_path.match(/\.vf\z/)
  raise "Cannot override an existing VFile. Please confirm."
end


t0 = Time.now

puts "[#{$$}] START: #{t0}"
puts "[#{$$}]   pattern: #{pattern}"
puts "[#{$$}]   input:  #{input_path}"
puts "[#{$$}]   output: #{output_path}"

fairy = Fairy::Fairy.new

input = fairy.input input_path
greped = input.grep(pattern, :ignore_exception => true)
greped.output output_path

t1 = Time.now
puts "[#{$$}] DONE: #{t1} (#{t1-t0} sec)"


