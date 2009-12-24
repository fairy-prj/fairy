#!/usr/bin/env ruby
# encoding: UTF-8

require "thread"

class Filter
  # 入出力にキューあり
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

class Filter2
  # 出力にキューあり
  def initialize
    @q = SizedQueue.new(100)
  end

  def push(l)
    @q.push l
  end

  def each(&block)
    @q.each(&block)
  end

  def pop
    @q.pop
  end

  def Filter2.input(file)
    f = Filter2.new
    Thread.start{
      File.open(file).each{|l| 
	f.push l
      }
      f.push nil
    }
    f
  end

  def map(&block)
    filter = Filter2.new
    Thread.start {
      while l = self.pop
	filter.push(block.call(l))
      end
      filter.push nil
    }
    filter
  end

  def mapf(&block)
    filter = Filter2.new
    Thread.start {
      while l = self.pop
	block.call(l).each{|w| filter.push w}
      end
      filter.push nil
    }
    filter
  end

  def output(file)
    io = File.open(file, "w")
    while l = self.pop
      io.print l
    end
  end
end

class Filter21
  # 出力にキューあり
  def initialize
    @head = []
    @m = Mutex.new
    @c = ConditionVariable.new
    @tail = []
  end

  def push(l)
    if @head.nil?
      @m.synchronize do
	@c.wait(@m)
      end
    end
	
    if @head.size > 100000
      @m.synchronize do
	@tail = @head
	@head = nil
	@c.signal
      end
    end
    @head.push l
  end

#  def each(&block)
#    @q.each(&block)
#  end

  def pop
    if @tail.empty?
      @m.synchronize do
	@c.wait(@m)
      end
    end
      
    val = @q.pop
    if @tail.empty?
      @m.synchronize do
	@head = []
	@c.signal
      end
    end
    val
  end

  def self.input(file)
    f = Filter2.new
    Thread.start{
      File.open(file).each{|l| 
	f.push l
      }
      f.push nil
    }
    f
  end

  def map(&block)
    filter = Filter2.new
    Thread.start {
      while l = self.pop
	filter.push(block.call(l))
      end
      filter.push nil
    }
    filter
  end

  def mapf(&block)
    filter = Filter2.new
    Thread.start {
      while l = self.pop
	block.call(l).each{|w| filter.push w}
      end
      filter.push nil
    }
    filter
  end

  def output(file)
    io = File.open(file, "w")
    while l = self.pop
      io.print l
    end
  end
end

class Filter22
  # 出力にキューあり
  def initialize
    @head = []
    @m = Mutex.new
    @c = ConditionVariable.new
    @tail = []
  end

  def push(l)
    if @head.nil?
      @m.synchronize do
	@c.wait(@m)
      end
    end
	
    if @head.size > 100000
      @m.synchronize do
	@tail = @head
	@head = nil
	@c.signal
      end
    end
    @head.push l
  end

#  def each(&block)
#    @q.each(&block)
#  end

  def pop
    if @tail.empty?
      @m.synchronize do
	@c.wait(@m)
      end
    end
      
    val = @q.pop
    if @tail.empty?
      @m.synchronize do
	@head = []
	@c.signal
      end
    end
    val
  end

  def self.input(file)
    f = Filter2.new
    Thread.start{
      File.open(file).each{|l| 
	f.push l
      }
      f.push nil
    }
    f
  end

  def map(&block)
    filter = Filter2.new
    Thread.start {
      while l = self.pop
	filter.push(block.call(l))
      end
      filter.push nil
    }
    filter
  end

  def mapf(&block)
    filter = Filter2.new
    Thread.start {
      while l = self.pop
	block.call(l).each{|w| filter.push w}
      end
      filter.push nil
    }
    filter
  end

  def output(file)
    io = File.open(file, "w")
    while l = self.pop
      io.print l
    end
  end
end

class Filter3
  def Filter3.input(file)
    Filter3::Input.new(file)
  end

  def map(&block)
    Filter3::Map.new(self, block)
  end


  def mapf(&block)
    Filter3::Mapf.new(self, block)
  end

  def output(file)
    o = Filter3::Output.new(self, file)
    o.output
  end

  class Input<Filter3
    def initialize(file)
      @input = File.open(file)
    end

    def each(&block)
      @input.each &block
    end
  end

  class Map<Filter3
    def initialize(pre, block)
      @pre = pre
      @block = block
    end

    def each(&block)
      @pre.each{|e| block.call(@block.call(e))}
    end
  end

  class Mapf<Filter3
    def initialize(pre, block)
      @pre = pre
      @block = block
    end

    def each(&block)
      @pre.each{|e| @block.call(e).each{|ee| block.call(ee)}}
    end
  end

  class Output<Filter3
    def initialize(pre, file)
      @pre = pre
      @file = file
    end

    def output
      io = File.open(@file, "w")
      @pre.each{|e|
	io.print e
      }
    end
  end
end

class FilterD
  # 入出力にキューあり
  def initialize
    @q = Queue.new
    @ary = []
  end

  def push(l)
    @ary.push l
    if @ary.size > 10000
      @q.push @ary
      @ary = []
    end
  end

  def each_chunk(&block)
    @q.each(&block)
  end

  def each(&block)
    @q.each{|ary| ary.each &block}
  end

  def start_export
    imp = Queue.new
    Thread.start {
      while l = @q.pop
	imp.push l
      end
      imp.push nil
    }
    imp
  end

  def FilterD.input(file)
    f = FilterD.new
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
    filter = FilterD.new
    Thread.start {
      while l = imp.pop
	filter.push(l.each{|e| block.call(e)})
      end
      filter.push nil
    }
    filter
  end

  def mapf(&block)
    imp = start_export
    filter = FilterD.new

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


F = Filter21

case ARGV[0]
when "1"
  f = F.input("sample/wc/data/sample_30M.txt")
  f = f.mapf{|l| begin
	       l.chomp.split
	     rescue
	       []
	     end}
  f.output("/tmp/gg")
  #48.34/fairy(CPQ): 22.67
  #0:19.86(F2)
  #0:05.73(F3)

when "2"
  f = F.input("sample/wc/data/sample_30M.txt")
  f = f.mapf{|l| begin
	       l.chomp.split
	     rescue
	       []
	     end}
  f = f.map{|w| w}
  f.output("/tmp/gg")
  #1:11.72/fairy(CPQ): 1:20.74
  #0:41.84(F2)
  #0:08.25(F3)

when "3"
  f = F.input("sample/wc/data/sample_30M.txt")
  f = f.mapf{|l| begin
	       l.chomp.split
	     rescue
	       []
	     end}
  f = f.map{|w| w}
  f = f.map{|w| w}
  f.output("/tmp/gg")
  #1:32.53/fairy(CPQ): 2:33.66, (Q): 4:06.26
  #0:56.61(F2)
  #0:10.13(F3)

when "4"
  f = F.input("sample/wc/data/sample_30M.txt")
  f = f.mapf{|l| begin
	       l.chomp.split
	     rescue
	       []
	     end}
  f = f.map{|w| w}
  f = f.map{|w| w}
  f = f.map{|w| w}
  f.output("/tmp/gg")
  #2:13.65
  #1:11.08(F2)
  #0:13.32(F3)

when "5"
  f = F.input("sample/wc/data/sample_30M.txt")
  f = f.mapf{|l| begin
	       l.chomp.split
	     rescue
	       []
	     end}
  f = f.map{|w| w}
  f = f.map{|w| w}
  f = f.map{|w| w}
  f = f.map{|w| w}
  f.output("/tmp/gg")
  #2:37.85
  #1:33.25(F2)
  #0:14.73(F3)

end

  
