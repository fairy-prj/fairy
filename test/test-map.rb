#!/usr/bin/env ruby
# encoding: UTF-8

case ARGV[0]
when "0"
  io = File.new("/tmp/gg")
  File.open("sample/wc/data/sample_30M.txt").each{|l|
    begin
      l.chomp.split.each{|w| io.print w}
    rescue
      []
    end
  }

when "1"
  require "thread"
  q = Queue.new
  th1 = Thread.start{
    File.open("sample/wc/data/sample_30M.txt").each{|l|
      begin
	l.chomp.split.each{|w| q.push w}
      rescue
	[]
      end
    }
    q.push nil
  }
  while w = q.pop
  end

when "2"
  require "thread"
  q = Queue.new
  th1 = Thread.start{
    File.open("sample/wc/data/sample_30M.txt").each{|l|
      begin
	l.chomp.split.each{|w| q.push w}
      rescue
	[]
      end
    }
    q.push nil
  }
  q2 = Queue.new
  th2 = Thread.start{
    while w = q.pop
      q2.push w
    end
    q2.push nil
  }

  while w = q2.pop
  end

when "4"
  require "thread"

  class Filter
    def initialize
      @q = Queue.new
    end

    def push(l)
      @q.push l
    end

    def each(&block)
      @q.each(&block)
    end

    def start_export
      imp = Queue.new
      e = Filter.new
      Thread.start {
	while l = @q.pop
	  imp.push l
	end
	imp.push nil
      }
      imp
    end

    def Filter.input(file)
      f = Filter.new
      Thread.start{
	File.open(file).each{|l| 
	  f.push l
	}
	f.push nil
      }
      f
    end

    def map(&block)
      imp = start_export
      filter = Filter.new
      Thread.start {
	while l = imp.pop
	  filter.push(block.call(l))
	end
	filter.push nil
      }
      filter
    end

    def mapf(&block)
      imp = start_export
      filter = Filter.new

      Thread.start {
	while l = imp.pop
	  block.call(l).each{|w| filter.push w}
	end
	filter.push nil
      }
      filter
    end

    def output(file)
      imp = start_export

      io = File.open(file, "w")
      while l = imp.pop
	io.print l
      end
    end
  end
  

  Filter.input("sample/wc/data/sample_30M.txt").mapf{|l| l.chomp.split}.output("/tmp/gg")

end

  
