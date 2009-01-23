
require "fairy"

Thread.abort_on_exception=true

if ARGV[0] == "-njob-monitor"
  require "share/debug"
  ARGV.shift
  $monitor_on = true
  $sleep = 1
end

fairy = Fairy::Fairy.new("localhost", "19999")
if $monitor_on
  Fairy::Debug::njob_status_monitor_on(fairy)
end

case ARGV[0]
when "0", "service get"
  p fairy.controller

when "1", "input"
  p fairy.input(["file://localhost/etc/passwd", "file://localhost/etc/group"])
  sleep $sleep if $sleep 


when "1.5", "input"
  p fairy.input("test/vf")
  sleep $sleep if $sleep 

when "1.7", "input"
  p fairy.input(["file://gentoo/etc/passwd", "file://gentoo/etc/group"])
  sleep $sleep if $sleep 


when "2", "grep"
  p f = fairy.input(["file://localhost/etc/passwd", "file://localhost/etc/group"]).grep(/#{ARGV[1]}/)
  sleep $sleep if $sleep 

when "3", "here"
  here = fairy.input(["/etc/passwd", "/etc/group"]).here
  for l in here
    puts l
  end
  sleep $sleep if $sleep 


when "3.vf", "here"
  here = fairy.input("test/vf").here
  for l in here
    puts l
  end
  sleep $sleep if $sleep 

when "3.0", "here"
  here = fairy.input(["/etc/passwd"]).here
  for l in here
    puts l
  end
  sleep $sleep if $sleep 

when "3.1", "grep.here"
  here = fairy.input(["/etc/passwd", "/etc/group"]).grep(/#{ARGV[1]}/).here
  for l in here
    puts l.inspect
  end
  sleep $sleep if $sleep 

when "3.2", "map.here"
  here = fairy.input(["/etc/passwd", "/etc/group"]).map(%{|l| l.chomp.split(/:/)}).here
  for l in here
    print l.join("-"), "\n"
  end
  sleep $sleep if $sleep 

when "3.3", "smap"
  here = fairy.input(["/etc/passwd", "/etc/group"]).smap(%{|i,o| i.sort.each{|e|o.push e}}).here
  for l in here
    puts l.inspect
  end
  sleep $sleep if $sleep 

when "3.3a", "smap"
  here = fairy.input(["/etc/passwd", "/etc/group"]).smap(%{|i,o| i.sort.each{|e|o.push e}}).here


when "3.3.1"
  10000.times do |i|
    puts "LOOP: #{i}"
    fairy.input(["/etc/passwd", "/etc/group"]).smap(%{|i,o| i.sort.each{|e|o.push e}}).here.to_a
    c = 0
    ObjectSpace.each_object{|obj| c+=1}
    puts "NUMBER OF OBJECT: #{c}"
  end

when "3.3.2"
  1000.times do |i|
    here = fairy.input(["/etc/passwd", "/etc/group"]).smap(%{|i,o| i.sort.each{|e|o.push e}}).map(%{|e| e}).map(%{|e| e}).map(%{|e| e}).here
  end


when "3.3.3"
  here = fairy.input(["/etc/passwd", "/etc/group"]).smap(%{|i,o| i.sort.each{|e|o.push e}}).map(%{|e| e}).map(%{|e| e}).map(%{|e| e}).map(%{|e| e}).map(%{|e| e}).map(%{|e| e}).map(%{|e| e}).map(%{|e| e}).map(%{|e| e}).map(%{|e| e}).here
  for l in here
    puts l
  end

when "3.4", "njob-monitor"
  require "share/debug"
  Fairy::Debug::njob_status_monitor_on(fairy)

  here = fairy.input(["/etc/passwd", "/etc/group"]).smap(%{|i,o| i.sort.each{|e|o.push e}}).here
  for l in here
    puts l
  end

when "3.5"
  puts "nodeの非同期追加のテストはなし"

when "3.6"
  puts "port指定のの非同期追加のテストはなし"

when "4", "group_by"
  here = fairy.input(["test/test-4-data1", "test/test-4-data2"]).group_by(%{|w| w.chomp.split{/\s+/}[0]}).here
  for l in here
    puts l
  end

when "4.0", "group_by"
  here = fairy.input(["test/test-4-data1"]).group_by(%{|w| w.chomp.split{/\s+/}[0]}).here
  for l in here
    puts l
  end

when "4.5", "wc"
  wc = fairy.input(["test/test-4-data1", "test/test-4-data2"]).group_by(%{|w| w.chomp.split(/\s+/)[0]}).smap(%{|i, o| o.push(sprintf("%s=>%d", i.key, i.size))})
  wc.here.each{|w, n| puts "word: #{w}, count: #{n}"}

  sleep $sleep if $sleep 


when "4.5.1", "wc"
  wc = fairy.input(["test/test-4-data1", "test/test-4-data2"]).group_by(%{|w| w.chomp.split(/\s+/)[0]}).smap(%{|i, o| o.push([i.key, i.size])})
  wc.here.each{|w, n| puts "word: #{w}, count: #{n}"}

  sleep $sleep if $sleep 

when "4.5.t", "wc"
  wc = fairy.input(["test/test-4-data1", "test/test-4-data2"]).group_by(%{|w| w.chomp.split(/\s+/)[0]}).smap(%{|i, o| o.push([i.key, i.size])})
  wc.here.each{|r| r = r.dc_dup; w, n = r[0], r[1]; puts "word: #{w}, count: #{n.inspect}"}


when "4.5.x", "wc"
  wc = fairy.input(["test/test-4-data1", "test/test-4-data2"]).group_by(%{|w| w.chomp.split(/\s+/)[0]}).smap(%{|i, o| o.push([i.key, i.size])})
  wc.here.each{|r| w, n = r[0], r[1]; puts "word: #{w}, count: #{n.inspect}"}

when "5", "zip"
  zip = fairy.input("/etc/passwd")
  main = fairy.input("/etc/passwd").zip(zip, :ZIP_BY_SUBSTREAM, %{|e1, e2| e1.chomp+"+"+e2}).here
  for l in main
    puts l
  end


when "5.1", "zip2"
  zip = fairy.input(["/etc/passwd", "/etc/group"])
  main = fairy.input(["/etc/passwd", "/etc/group"]).zip(zip, :ZIP_BY_SUBSTREAM, %{|e1, e2| e1.chomp+"+"+e2}).here
  for l in main
    puts l
  end


when "5.2", "zip3"
  zip1 = fairy.input(["/etc/passwd", "/etc/group"])
  zip2 = fairy.input(["/etc/passwd", "/etc/group"])
  main = fairy.input(["/etc/passwd", "/etc/group"]).zip(zip1, zip2, :ZIP_BY_SUBSTREAM, %{|e1, e2, e3| e1.chomp+"+"+e2.chomp+"-"+e3}).here
  for l in main
    puts l
  end

when "6", "output"

#  DeepConnect::MESSAGE_DISPLAY=true

  fairy.input(["file://localhost/etc/passwd", "file://localhost/etc/group"]).output("test/test-output")
  sleep $sleep if $sleep 

when "6.1"

  here = fairy.input("test/test-output").here
  for l in here
    puts l
  end

when "6.2", "gentoo"
  
  fairy.input("test/test-6.2-input").output("test/test-6.2-output")
  sleep $sleep if $sleep 

when "6.3", "wc"
  wc = fairy.input(["test/test-4-data1", "test/test-4-data2"]).group_by(%{|w| w.chomp.split(/\s+/)[0]}).smap(%{|i, o| o.push(sprintf("%s=>%d", i.key, i.size))})
#  p wc.here.to_a
  wc.output("test/test-6.3-output")

when "6.4", "wc"
  wc = fairy.input("test/test-6.2-input").group_by(%{|w| w.chomp.split(/\s+/)[0]}).smap(%{|i, o| o.push(sprintf("%s=>%d", i.key, i.size))})
#  p wc.here.to_a
  wc.output("test/test-6.4-output")

when "7", "split"
  fairy.input(["file://localhost/etc/passwd"]).split(4).output("test/test-7-output")
  sleep $sleep if $sleep 

when "7.1", "split"
  sp = fairy.input(["file://localhost/etc/passwd"]).split(4).here
  for l in sp
    puts l
  end
  
  sleep $sleep if $sleep 

when "8", "lfile"
  lf = fairy.input("/etc/passwd").here
  for l in lf
    puts l
  end

when "8.1"
  lf = fairy.input("/etc/passwd").output("test/test-8.1-output")

when "8.2"
  lf = fairy.input("/etc/passwd").split(4).output("test/test-8.2-output")

when "9"
  lf = fairy.input("test/test-8.2-output").output("test/test-9.output")

when "9.1"
  lf = fairy.input("test/test-8.2-output").here
  for l in lf
    puts l
  end

when "10"
  lf = fairy.input("/etc/passwd", :split_size=>256).here
  for l in lf
    puts l
  end

when "10.1"
  fairy.input("/etc/passwd", :split_size=>256).output("test/test-10.output.vf")

when "11"
  fairy.def_pool_variable(:ver, "1")
  lf = fairy.input("/etc/passwd").map(%{|e| e.chomp+"+"+@Pool[:ver]}).here
  for l in lf
    puts l
  end

when "11.1"
  fairy.def_pool_variable(:ver, "1")
  lf = fairy.input("/etc/passwd").map(%{|e| e.chomp+"+"+@Pool.ver}).here
  for l in lf
    puts l
  end


when "11.2"
  fairy.def_pool_variable(:ver, "1")
  lf = fairy.input("/etc/passwd").map(%{|e| @Pool.ver = @Pool.ver.succ; e.chomp+"+"+@Pool.ver}).here
  for l in lf
    puts l
  end

when "11.2.1"
  fairy.def_pool_variable(:ver, "1")
  lf = fairy.input("/etc/passwd").map(%{|e| @Pool.ver = @Pool.ver.succ; e.chomp+"+"+@Pool.ver}).map(%{|e| @Pool.ver = @Pool.ver.succ; e.chomp+"-"+@Pool.ver}).here
  for l in lf
    puts l
  end

when "12"

  # いかは NG
  lf = fairy.input("/etc/passwd").map(%{|e| @Pool.ver = @Pool.ver.succ; e.chomp+"+"+@Pool.ver})
  lf.def_job_pool_variable....
  
  
when "X", "sort"
  LOCAL_SORT_SIZE = 10
  input_files = ["/etc/passwd", "/etc/group"]
  
  size = fairy.input(input_files).smap(%{|i, o| o.push i.size}).here.inject(0){|c, n| c += n}

  prob = 10.0/size

end

