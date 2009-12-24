require "thread"

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
    ary = []
    f = File.open("sample/wc/data/sample_30M.txt")
    f.each do |l|
      begin
	l.chomp.split.each{|e| ary.push e}
      rescue
      end
      if ary.size > 100
	q1.push ary
	ary = []
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

when "6.3.2"
  m = Mutex.new
  q1 = Queue.new
  q2 = Queue.new

  Thread.start do
    ary = []
    f = File.open("sample/wc/data/sample_30M.txt")
    f.each do |l|
      begin
	l.chomp.split.each{|e| ary.push e}
      rescue
      end
      if ary.size > 100
	q1.push ary
	ary = []
      end
    end
    q1.push nil
  end
  
  Thread.start do
    while e = m.synchronize{q1.pop}
      q2.push e
    end
    q2.push nil
  end

  g = File.open("/tmp/gg", "w")

  while ary = q2.pop
    ary.each{|e| g.puts e}
  end

end

  
