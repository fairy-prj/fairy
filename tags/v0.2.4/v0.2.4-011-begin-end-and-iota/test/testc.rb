
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
#  lf = fairy.input("/etc/passwd").map(%{|e| @Pool.ver = @Pool.ver.succ; e.chomp+"+"+@Pool.ver})
#  lf.def_job_pool_variable....

when "13", "shuffle"

  input_files = ["/etc/passwd", "/etc/group"]
  f1 = fairy.input(input_files)
  f2 = f1.shuffle(%{|i, o| i.each{|s| o.push s}})
  for l in f2.here
    puts l
  end

when "13.1"

  input_files = ["/etc/passwd", "/etc/group"]
  f1 = fairy.input(input_files)
  f2 = f1.shuffle(%{|i, o| i.to_a.reverse.each{|s| o.push s}})
  for l in f2.here
    puts l
  end

when "13.2"

  input_files = ["/etc/passwd", "/etc/group"]
  f1 = fairy.input(input_files)
  f2 = f1.shuffle(%{|i, o| 
    begin 
     i.to_a.each{|s| o.push s}
    rescue
     p $!, $@
     raise
    end
    })
  for l in f2.here
    puts l
  end

when "13.3", "reverse"

  input_files = ["/etc/passwd", "/etc/group"]
  f1 = fairy.input(input_files)
  f2 = f1.shuffle(%{|i, o| i.to_a.reverse.each{|s| o.push s}})
  f3 = f2.smap(%{|i, o| i.to_a.reverse.each{|e| o.push e}})
  for l in f3.here
    puts l
  end


when "14", "sort"

  input_files = ["/etc/passwd", "/etc/group"]
  
  f1 = fairy.input(input_files).group_by(%{|e| e[0]})
  f2 = f1.smap(%{|i, o|
	  ary = i.to_a.sort
	  ary.each{|e| o.push e}})
  for l in f2.here
    puts l
  end


when "14.0", "sort"

  input_files = ["/etc/passwd", "/etc/group"]
  
  f1 = fairy.input(input_files).group_by(%{|e| e[0]})
  for l in f1.here
    puts l
  end

when "14.0.1", "sort"

  input_files = ["/etc/passwd", "/etc/group"]
  
  f1 = fairy.input(input_files).split(26)
  for l in f1.here
    puts l
  end


when "14.1", "sort"

  input_files = ["/etc/passwd", "/etc/group"]

  pv = "l"
  fairy.def_pool_variable(:pv, pv)

  f1 = fairy.input(input_files).group_by(%{|e| e > @Pool.pv})
  f2 = f1.smap(%{|i, o|
	  ary = i.to_a.sort
	  ary.each{|e| o.push e}})
  for l in f2.here
    puts l
  end

when "14.2"

  # NG
  puts "これは動きません. デッドロックします"

  input_files = ["/etc/passwd", "/etc/group"]

  pv = "l"
  fairy.def_pool_variable(:pv, pv)

  f1 = fairy.input(input_files).group_by(%{|e| e <=> @Pool.pv})
  f2 = f1.shuffle(%{|i, o| i.sort{|s1, s2| s1.key <=> s2.key}.each{|s| o.push s}})
  f3 = f2.smap(%{|i, o|
	  ary = i.to_a.sort
	  ary.each{|e| o.push e}})
  for l in f3.here
    puts l
  end


when "14.3"

  input_files = ["/etc/passwd", "/etc/group"]

  f1 = fairy.input(input_files).group_by(%{|e| e[0]})
  f2 = f1.smap(%{|i, o|
	  ary = i.to_a.sort
	  ary.each{|e| o.push e}})
  f3 = f2.shuffle(%{|i, o| i.sort{|s1, s2| s1.key <=> s2.key}.each{|s| o.push s}})
  for l in f3.here
    puts l
  end



when "14.4"

  input_files = ["/etc/passwd", "/etc/group"]

  p = "a"
  pv = []
  26.times{pv.push p; p = p.succ}

  fairy.def_pool_variable(:pv, pv)

  f1 = fairy.input(input_files).group_by(%{|e| @Pool.pv.find(proc{"z"}){|p| e < p}})
  f2 = f1.smap(%{|i, o|
	  ary = i.to_a.sort
	  ary.each{|e| o.push e}})
  f3 = f2.shuffle(%{|i, o| i.sort{|s1, s2| s1.key <=> s2.key}.each{|s| o.push s}})
  for l in f3.here
    puts l
  end

when "15.1" , "barrier", "node_arrived"
  
  input_files = ["/etc/passwd", "/etc/group"]

  f1 = fairy.input(input_files).barrier(:mode=>:NODE_CREATION, :cond=>:NODE_ARRIVED, :buffer=>:MEMORY)
  for l in f1.here
    puts l
  end

when "15.1.1"
  
  input_files = ["/etc/passwd", "/etc/group"]

  pv = "l"
  fairy.def_pool_variable(:pv, pv)

  f1 = fairy.input(input_files).group_by(%{|e| e <=> @Pool.pv})
  f2 = f1.barrier(:mode=>:NODE_CREATION, :cond=>:NODE_ARRIVED, :buffer=>:MEMORY)
  f3 = f2.shuffle(%{|i, o| i.sort{|s1, s2| s1.key <=> s2.key}.each{|s| o.push s}})
  f4 = f3.smap(%{|i, o|
	  ary = i.to_a.sort
	  ary.each{|e| o.push e}})
  for l in f4.here
    puts l
  end

when "15.1.2"

  # NODE の生成のされ方が気になっている

  input_files = ["/etc/passwd", "/etc/group"]
  f1 = fairy.input(input_files).smap(%{|i,o| puts "SLEEPIN"; sleep 5; puts "WAKEUP"; i.each{|e| o.push e}})
  f2 = f1.barrier(:mode=>:NODE_CREATION, :cond=>:NODE_ARRIVED, :buffer=>:MEMORY)
  for l in f2.here
    puts l
  end

when "15.1.2.1"

  # NODE の生成のされ方が気になっている 根本はこちらにあるらしい

  input_files = ["/etc/passwd", "/etc/group"]
  f1 = fairy.input(input_files).smap(%{|i,o| puts "SLEEPIN"; sleep 5; puts "WAKEUP"; i.each{|e| o.push e}})
  for l in f1.here
    puts l
  end


when "15.2", "data_arrived"

  input_files = ["/etc/passwd", "/etc/group"]
  f1 = fairy.input(input_files).barrier(:mode=>:NODE_CREATION, :cond=>:DATA_ARRIVED, :buffer=>:MEMORY)
  for l in f1.here
    puts l
  end

when "15.2.1"

  input_files = ["/etc/passwd", "/etc/group"]

  pv = "l"
  fairy.def_pool_variable(:pv, pv)

  f1 = fairy.input(input_files).group_by(%{|e| e <=> @Pool.pv})
  f2 = f1.barrier(:mode=>:NODE_CREATION, :cond=>:DATA_ARRIVED, :buffer=>:MEMORY)
  f3 = f2.shuffle(%{|i, o| i.sort{|s1, s2| s1.key <=> s2.key}.each{|s| o.push s}})
  f4 = f3.smap(%{|i, o|
	  ary = i.to_a.sort
	  ary.each{|e| o.push e}})
  for l in f4.here
    puts l
  end

when "15.2.2"

  input_files = ["/etc/passwd", "/etc/group"]
  f1 = fairy.input(input_files).smap(%{|i,o| puts "SLEEPIN"; sleep 5; puts "WAKEUP"; i.each{|e| o.push e}})
  f2 = f1.barrier(:mode=>:NODE_CREATION, :cond=>:DATA_ARRIVED, :buffer=>:MEMORY)
  for l in f2.here
    puts l
  end

when "15.3", "all_data_imported"

  input_files = ["/etc/passwd", "/etc/group"]
  f1 = fairy.input(input_files).barrier(:mode=>:NODE_CREATION, :cond=>:ALL_DATA, :buffer=>:MEMORY)
  for l in f1.here
    puts l
  end

when "15.3.1"

  input_files = ["/etc/passwd", "/etc/group"]

  pv = "l"
  fairy.def_pool_variable(:pv, pv)

  f1 = fairy.input(input_files).group_by(%{|e| e <=> @Pool.pv})
  f2 = f1.barrier(:mode=>:NODE_CREATION, :cond=>:ALL_DATA, :buffer=>:MEMORY)
  f3 = f2.shuffle(%{|i, o| i.sort{|s1, s2| s1.key <=> s2.key}.each{|s| o.push s}})
  f4 = f3.smap(%{|i, o|
	  ary = i.to_a.sort
	  ary.each{|e| o.push e}})
  for l in f4.here
    puts l
  end

when "15.3.2"

  input_files = ["/etc/passwd", "/etc/group"]
  f1 = fairy.input(input_files).smap(%{|i,o| i.each{|e| o.push e; sleep 1}})
  f2 = f1.barrier(:mode=>:NODE_CREATION, :cond=>:ALL_DATA, :buffer=>:MEMORY)
  for l in f2.here
    puts l
  end


when "15.3.2.1"

  input_files = ["/etc/passwd", "/etc/group"]
  f1 = fairy.input(input_files).map(%{|e| sleep 1; e})
  f2 = f1.barrier(:mode=>:NODE_CREATION, :cond=>:ALL_DATA, :buffer=>:MEMORY)
  for l in f2.here
    puts l
  end

when "15.3.2.2"

  input_files = ["/etc/passwd", "/etc/group"]
  f1 = fairy.input(input_files).map(%{|e| sleep 1; e})
  for l in f1.here
    puts l
  end

when "15.4", "block_cond"

  input_files = ["/etc/passwd", "/etc/group"]

  fairy.def_pool_variable(:mutex, Mutex.new)

  f0 = fairy.input(input_files).smap(%{|i,o| @Pool.mutex.synchronize{puts "LOCK"; sleep 5; puts "LOCK OUT"}})

  sleep 2

  f1 = fairy.input(input_files).barrier(:mode=>:NODE_CREATION, :cond=>%{puts "COND:"; @Pool.mutex.lock}, :buffer=>:MEMORY)
  for l in f1.here
    puts l
  end

when "15.5", "stream"

  input_files = ["/etc/passwd", "/etc/group"]
  f1 = fairy.input(input_files).barrier(:mode=>:STREAM, :cond=>:DATA_ARRIVED, :buffer=>:MEMORY)
  for l in f1.here
    puts l
  end

when "15.5.1"

  input_files = ["/etc/passwd", "/etc/group"]
  f1 = fairy.input(input_files).smap(%{|i,o| puts "SLEEPIN"; sleep 5; puts "WAKEUP"; i.each{|e| o.push e}})
  f2 = f1.barrier(:mode=>:STREAM, :cond=>:DATA_ARRIVED, :buffer=>:MEMORY)
  for l in f2.here
    puts l
  end

when "16", "begin", "end"

  here = fairy.input(["/etc/passwd", "/etc/group"]).map(%{|l| l.chomp.split(/:/)}, :BEGIN=>%{puts "BEGIN"}, :END=>%{puts "END"}).here
  for l in here
    print l.join("-"), "\n"
  end
  sleep $sleep if $sleep 


when "17", "iota"

  iota = fairy.input(Fairy::Iota, 1000)
  for l in iota.here
    puts l
  end
  sleep $sleep if $sleep 

when "17.1"

  f0 = fairy.input(Fairy::Iota, 1000)
  f1 = f0.map(%{|e| @sum += e}, :BEGIN=>%{@sum = 0})
  for l in f1.here
    puts l
  end
  sleep $sleep if $sleep 

when "17.2"

  f0 = fairy.input(Fairy::Iota, 1000)
  f1 = fairy.input(Fairy::Iota, 1000)
  f2 = f0.zip(f1, :ZIP_BY_SUBSTREAM, %{|e1, e2| e1+e2})
  for l in f2.here
    puts l
  end
  sleep $sleep if $sleep 


end
