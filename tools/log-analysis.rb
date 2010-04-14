#!/usr/bin/env ruby
# encoding: UTF-8

require "time"

module Fairy

  class LogAnalyzer

    def self.analyze(input = ARGF)
      analyzer = self.new(input)
      analyzer.compile
      analyzer.analyze
    end

    def initialize(input)
      @input = input
      @events = {}
    end

    PAT = /([0-9\/]+)\s+([0-9:.]+)\s+([^\s]+)\s+([^\s]+)\s+([^\s]+)\s([^#]+)#([^:]+):\s+(.*)$/

    def compile
      @input.each{|line|
	next unless line =~ /(START|FINISH)\s+(PROCESSING|EXPORT|IMPORT|STORE)/
	line.chomp!
	line =~ PAT
	date, time, host, process, file, object, method, mes = $1, $2, $3, $4, $5, $6, $7, $8
	uuid = [host, process, object].join(" ")

	abs_time = Time.parse(date+" "+time)
	mes =~ /(START|FINISH)\s+(PROCESSING|EXPORT|IMPORT|STORE)/
	@events[uuid] ||= []
	@events[uuid].push [$2, $1, abs_time]
      }
    end

    def analyze
      for k, values in @events
	case values.first[0]
	when "STORE"
	  time = 0
	  values.each_slice(2) do |start, finish|
	    time += finish[2]-start[2]
	  end
	  puts "#{k}, #{values.first[0]}, , , #{time}"

	else
	  st = values.find{|v| v[1] == "START"}
	  fn = values.find{|v| v[1] == "FINISH"}

	  puts "#{k}, #{st[0]}, #{st[2]}, #{fn[2]}, #{fn[2] - st[2]}"
	end
      end
    end
  end
end

Fairy::LogAnalyzer.analyze



