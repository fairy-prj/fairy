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
	line.chomp!
	if line =~ /(START|FINISH)\s+(PROCESSING|EXPORT|IMPORT|STORE|M\.STORE|M\.RESTORE)/
	  compile_stat(line)
	elsif line =~ /PROCESS MONITOR:$/
	  compile_ps(line)
	elsif line =~ /MONITOR: PS:/
	  compile_psdetail(line)
	end
      }
    end

    def compile_stat(line)
      line =~ PAT
      date, time, host, process, file, object, method, mes = $1, $2, $3, $4, $5, $6, $7, $8
      uuid = [host, process, object].join(" ")

      abs_time = Time.parse(date+" "+time)
      mes =~ /(START|FINISH)\s+(PROCESSING|EXPORT|IMPORT|STORE|M\.STORE|M\.RESTORE)/
      @events[[uuid, $2]] ||= []
      @events[[uuid, $2]].push [$2, $1, abs_time]
    end

    def compile_ps(line)
      line =~ PAT
      date, time, host, process, file, object, method, mes = $1, $2, $3, $4, $5, $6, $7, $8
      abs_time = Time.parse(date+" "+time)
      @last_mon_datetime = [[host, process, object].join(" "), abs_time]
    end

    def compile_psdetail(line)
      line =~ /([0-9]+)\s+([0-9]+)\s+([0-9]+)\s+([0-9.]+)\s+([0-9.]+)\s+([0-9]+)\s+([0-9:]+)/
      key = [@last_mon_datetime[0], "PS"]
      @events[key] ||= []
      @events[key].push ["PS", @last_mon_datetime[1], $1, $2, $3, $4, $5, $6, $7]
    end

    def analyze
#p @events
      for k, values in @events
	case values.first[0]
	when "STORE"
	  time = 0
	  values.each_slice(2) do |start, finish|
	    time += finish[2]-start[2]
	  end
	  puts "#{k.first}, #{values.first[0]}, , , #{time}"

	when "M.STORE"
	  time = 0
	  values.each_slice(2) do |start, finish|
	    time += finish[2]-start[2]
	  end
	  puts "#{k.first}, #{values.first[0]}, , , #{time}"

	when "M.RESTORE"
	  time = 0
	  values.each_slice(2) do |start, finish|
	    time += finish[2]-start[2]
	  end
	  puts "#{k.first}, #{values.first[0]}, , , #{time}"

	when "PS"
	  values.each do |ps, datetime, *args|
#p ps, datetime
	    puts "#{k.first}, #{values.first[0]}, #{datetime}, #{args.join(', ')}"
	  end
	else
	  st = values.find{|v| v[1] == "START"}
	  fn = values.find{|v| v[1] == "FINISH"}

	  puts "#{k.first}, #{st[0]}, #{st[2]}, #{fn[2]}, #{fn[2] - st[2]}"
	end
      end
    end
  end
end

Fairy::LogAnalyzer.analyze



