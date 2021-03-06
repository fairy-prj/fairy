#!/usr/bin/env ruby

require "optparse"

require "deep-connect"

require "processor"

Thread.abort_on_exception=true


controller_port = nil
id = nil
opt = OptionParser.new do |opt|
  opt.on("--controller=VAL"){|val| controller_port = val}
  opt.on("--id=VAL"){|val| id = val}
end
opt.parse!(ARGV)

Fairy::Processor.start(id.to_i, controller_port)

puts "Processor Service Start"

sleep
