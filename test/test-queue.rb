require "thread"

class ModalFixQueue
  def initialize
#    @q = []
    @q = Array.new(10000)
    @head = 0
    @last = 0
  end

  def size
    @q.size
  end

  def push(e)
    @q[@head] = e
    @head += 1
  end

  def pop
    return nil if @head <= @last
    e = @q[@last]
    @last += 1
    e
  end

  def each(&block)
    while @last < @head
      block.call @q[@last]
      @last += 1
    end
  end
end

Q = ModalFixQueue

case ARGV[0]
when "0"
  f = File.open("sample/wc/data/sample_30M.txt")
  g = File.open("/tmp/gg", "w")
  f.each do |l|
    begin
      l.chomp.split.each{|w| g.puts w}
    rescue
    end
  end


when "0.1"
  f = File.open("sample/wc/data/sample_30M.txt")
  g = File.open("/tmp/gg", "w")
  f.collect{|l|
    begin
      l.chomp.split
    rescue
    end
  }.flatten.each{|w| g.puts w}

when "1"
  q = Queue.new

  f = File.open("sample/wc/data/sample_30M.txt")
  f.each do |l|
    begin
      l.chomp.split.each{|w| q.push w}
    rescue
    end
  end
  q.push nil

  g = File.open("/tmp/gg", "w")

  while e = q.pop
    g.puts e
  end

when "2"
  q = Queue.new

  Thread.start do
    f = File.open("sample/wc/data/sample_30M.txt")
    f.each do |l|
      begin
	l.chomp.split.each{|w| q.push w}
      rescue
      end
    end
    q.push nil
  end

  g = File.open("/tmp/gg", "w")

  while e = q.pop
    g.puts e
  end

when "3"
  q1 = Queue.new
  q2 = Queue.new

  Thread.start do
    f = File.open("sample/wc/data/sample_30M.txt")
    f.each do |l|
      begin
	l.chomp.split.each{|w| q1.push w}
      rescue
      end
    end
    q1.push nil
  end
  
  Thread.start do
    while e = q1.pop
      q2.push e
    end
    q2.push nil
  end

  g = File.open("/tmp/gg", "w")

  while e = q2.pop
    g.puts e
  end

when "4"
  q = Queue.new

  fib = Fiber.new {
    f = File.open("sample/wc/data/sample_30M.txt")
    f.each do |l|
      begin
	l.chomp.split.each{|w| q.push w; Fiber.yield}
      rescue
      end
    end
    q.push nil
  }

  fib.resume

  g = File.open("/tmp/gg", "w")
  while e = q.pop
    g.puts e
    fib.resume
  end

when "4.0.1"
  q = []

  fib = Fiber.new {
    f = File.open("sample/wc/data/sample_30M.txt")
    f.each do |l|
      begin
	l.chomp.split.each{|w| q.push w; Fiber.yield}
      rescue
      end
    end
  }

  fib.resume

  g = File.open("/tmp/gg", "w")
  while e = q.shift
    g.puts e
    fib.resume
  end

when "4.1"
  q = Queue.new

  fib = Fiber.new {
    f = File.open("sample/wc/data/sample_30M.txt")
    f.each do |l|
      begin
	l.chomp.split.each{|w| q.push w}
      rescue
      end
      Fiber.yield
    end
    q.push nil
  }

  fib.resume

  g = File.open("/tmp/gg", "w")
  while (begin
	   e = q.pop(true)
	 rescue ThreadError
	   fib.resume
	   retry
	 end)
    g.puts e
  end

when "4.1.1"
  q = []

  fib = Fiber.new {
    f = File.open("sample/wc/data/sample_30M.txt")
    f.each do |l|
      begin
	l.chomp.split.each{|w| q.push w}
      rescue
      end
      Fiber.yield
    end
    q.push :EOS
  }

  fib.resume

  g = File.open("/tmp/gg", "w")
  loop do
    while e = q.shift
      exit if e == :EOS
      g.puts e
    end
    fib.resume
  end

when "4.2"
  q = Queue.new

  n = 0
  fib = Fiber.new {
    f = File.open("sample/wc/data/sample_30M.txt")
    f.each do |l|
      begin
	l.chomp.split.each{|w| q.push w}
      rescue
      end
      n += 1
      Fiber.yield if n % 100000 == 0
    end
    q.push nil
  }

  fib.resume

  g = File.open("/tmp/gg", "w")
  while (begin
	   e = q.pop(true)
	 rescue ThreadError
	   fib.resume
	   retry
	 end)
    g.puts e
  end

when "4.2.1"
  q = []

  fib = Fiber.new {
    f = File.open("sample/wc/data/sample_30M.txt")
    f.each do |l|
      begin
	l.chomp.split.each{|w| q.push w}
      rescue
      end
      Fiber.yield if q.size >= 100000
    end
    q.push :EOS
  }

  fib.resume

  g = File.open("/tmp/gg", "w")
  loop do
    while e = q.shift
      exit if e == :EOS
      g.puts e
    end
    fib.resume
  end

when "5"
  q = Queue.new

  Thread.start do
    f = File.open("sample/wc/data/sample_30M.txt")
    f.each do |l|
      begin
	q.push l.chomp.split
      rescue
      end
    end
    q.push nil
  end

  g = File.open("/tmp/gg", "w")

  while qq = q.pop
    qq.each{|e| g.puts e}
  end


when "5.1"
  q = Queue.new

  Thread.start do
    q0 = []
    f = File.open("sample/wc/data/sample_30M.txt")
    f.each do |l|
      begin
	l.chomp.split.each{|e| q0.push e}
      rescue
      end
      if q0.size > 100000
	q.push q0
	q0 = []
      end
    end
    q.push nil
  end

  g = File.open("/tmp/gg", "w")

  while qq = q.pop
    qq.each{|e| g.puts e}
  end

when "6"
  q = Queue.new

  n = 0
  fib = Fiber.new {
    f = File.open("sample/wc/data/sample_30M.txt")
    f.each do |l|
      begin
	q.push l.chomp.split
      rescue
      end
      n += 1
      Fiber.yield if n % 10000 == 0
    end
    q.push nil
  }

  fib.resume

  g = File.open("/tmp/gg", "w")
  while (begin
	   ary = q.pop(true)
	 rescue ThreadError
	   fib.resume
	   retry
	 end)
    ary.each{g.puts e}
  end

when "6.1"
  q = Queue.new

  f = File.open("sample/wc/data/sample_30M.txt")
  f.each do |l|
    begin
      q.push l.chomp.split
    rescue
    end
  end
  q.push nil

  g = File.open("/tmp/gg", "w")

  while ary = q.pop
    ary.each{|e| g.puts e}
  end


when "6.2"
  q = Queue.new

  n = 0
  fib = Fiber.new {
    q0 = []
    f = File.open("sample/wc/data/sample_30M.txt")
    f.each do |l|
      begin
	l.chomp.split.each{|e| q0.push e}
      rescue
      end
      n += 1
      if n % 10000 == 0
	q.push q0
	Fiber.yield 
	q0 = []
      end
    end
    q.push nil
  }

  fib.resume

  g = File.open("/tmp/gg", "w")
  while (begin
	   ary = q.pop(true)
	 rescue ThreadError
	   fib.resume
	   retry
	 end)
    ary.each{g.puts e}
  end

when "6.2.1"
  q = []

  n = 0
  fib = Fiber.new {
    q0 = []
    f = File.open("sample/wc/data/sample_30M.txt")
    f.each do |l|
      begin
	l.chomp.split.each{|e| q0.push e}
      rescue
      end
      n += 1
      if n % 10000 == 0
	q.push q0
	Fiber.yield 
	q0 = []
      end
    end
    q.push :EOS
  }

  fib.resume

  g = File.open("/tmp/gg", "w")
  loop do
    while ary = q.shift
      exit if ary == :EOS
      ary.each{g.puts e}
    end
    fib.resume
  end


when "6.3"
  q1 = Queue.new
  q2 = Queue.new

  Thread.start do
    f = File.open("sample/wc/data/sample_30M.txt")
    f.each do |l|
      begin
	q1.push l.chomp.split
      rescue
      end
    end
    q1.push nil
  end
  
  Thread.start do
    while e = q1.pop
      q2.push e
    end
    q2.push nil
  end

  g = File.open("/tmp/gg", "w")

  while ary = q2.pop
    ary.each{|e| g.puts e}
  end

when "6.3.1"
  q1 = Queue.new
  q2 = Queue.new

  Thread.start do
    ary = ModalFixQueue.new
    f = File.open("sample/wc/data/sample_30M.txt")
    f.each do |l|
      begin
	l.chomp.split.each{|e| ary.push e}
      rescue
      end
      if ary.size >= 10000
	q1.push ary
	ary = ModalFixQueue.new
      end
    end
    q1.push nil
  end
  
  Thread.start do
    while e = q1.pop
      q2.push e
    end
    q2.push nil
  end

  g = File.open("/tmp/gg", "w")

  while ary = q2.pop
    ary.each{|e| g.puts e}
  end

end

  
