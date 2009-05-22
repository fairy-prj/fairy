#!/usr/bin/env ruby
# encoding: UTF-8

require "fairy"

Thread.abort_on_exception = Fairy::CONF.DEBUG_THREAD_ABORT_ON_EXCEPTION

if ARGV[0] == "-njob-monitor"
  require "fairy/share/debug"
  ARGV.shift
  $monitor_on = true
  $sleep = 1
end

#fairy = Fairy::Fairy.new("localhost", "19999")
fairy = Fairy::Fairy.new

if $monitor_on
  Fairy::Debug::njob_status_monitor_on(fairy)
end

case ARGV[0]
when "0", "service get"
  p fairy.controller

when "1", "input"
  p fairy.input(["file://localhost/etc/passwd", "file://localhost/etc/group"])
  sleep $sleep if $sleep 


when "1.5"
  p fairy.input("test/vf")
  sleep $sleep if $sleep 

when "1.7"
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


when "3.vf"
  here = fairy.input("test/vf").here
  for l in here
    puts l
  end
  sleep $sleep if $sleep 

when "3.0"
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

when "3.1.1"
  here = fairy.input(["/etc/passwd", "/etc/group"]).grep(/#{ARGV[1]}/).here
  here.each{|l|
    puts l.inspect
  }
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

when "3.3a"
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
    puts "LOOP: #{i}"
    here = fairy.input(["/etc/passwd", "/etc/group"]).smap(%{|i,o| i.sort.each{|e|o.push e}}).map(%{|e| e}).map(%{|e| e}).map(%{|e| e}).here
    here.to_a
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
o
when "3.5"
  puts "nodeの非同期追加のテストはなし"

when "3.6"
  puts "port指定のの非同期追加のテストはなし"

when "4", "group_by"
  here = fairy.input(["test/test-4-data1", "test/test-4-data2"]).group_by(%{|w| w.chomp.split{/\s+/}[0]}).here
  for l in here
    puts l
  end

when "4.0"
  here = fairy.input(["test/test-4-data1"]).group_by(%{|w| w.chomp.split{/\s+/}[0]}).here
  for l in here
    puts l
  end

when "4.5"
  wc = fairy.input(["test/test-4-data1", "test/test-4-data2"]).group_by(%{|w| w.chomp.split(/\s+/)[0]}).smap(%{|i, o| o.push(sprintf("%s=>%d", i.key, i.size))})
  wc.here.each{|w| puts "word=>count: #{w}"}

  sleep $sleep if $sleep 


when "4.5.1"
  wc = fairy.input(["test/test-4-data1", "test/test-4-data2"]).group_by(%{|w| w.chomp.split(/\s+/)[0]}).smap(%{|i, o| o.push([i.key, i.size])})
  wc.here.each{|w, n| puts "word: #{w}, count: #{n}"}

  sleep $sleep if $sleep 

when "4.5.t"
  wc = fairy.input(["test/test-4-data1", "test/test-4-data2"]).group_by(%{|w| w.chomp.split(/\s+/)[0]}).smap(%{|i, o| o.push([i.key, i.size])})
  wc.here.each{|r| r = r.dc_dup; w, n = r[0], r[1]; puts "word: #{w}, count: #{n.inspect}"}


when "4.5.x"
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
  
  for l in fairy.input("test/test-6.3-output").here
    puts l
  end

when "6.4"
  wc = fairy.input("test/test-6.2-input").group_by(%{|w| w.chomp.split(/\s+/)[0]}).smap(%{|i, o| o.push(sprintf("%s=>%d", i.key, i.size))})
#  p wc.here.to_a
  wc.output("test/test-6.4-output")

  for l in fairy.input("test/test-6.4-output").here
    puts l
  end


when "6.5"
  wc = fairy.input("test/test-6.2-input").group_by(%{|w| w.chomp.split(/\s+/)[0]}).smap(%{|i, o| o.push(sprintf("%s=>%d", i.key, i.size))})
#  p wc.here.to_a
  wc.output("test/test-6.5-output.vf", :one_file_by_process => true)

  for l in fairy.input("test/test-6.5-output.vf").here
    puts l
  end


when "7", "split"
  fairy.input(["file://localhost/etc/passwd"]).split (4).output("test/test-7-output")
  sleep $sleep if $sleep 

when "7.1"
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
  lf = fairy.input("/etc/passwd").map(%{|e| p @Pool; e.chomp+"+"+@Pool[:ver]}).here
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

when "11.3"
  fairy.def_pool_variable(:var, :block => %{Mutex.new})
  p fairy.pool_variable(:var).__deep_connect_reference?
  puts fairy.pool_variable(:var).inspect

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


when "14.0"

  input_files = ["/etc/passwd", "/etc/group"]
  
  f1 = fairy.input(input_files).group_by(%{|e| e[0]})
  for l in f1.here
    puts l
  end

when "14.0.1"

  input_files = ["/etc/passwd", "/etc/group"]
  
  f1 = fairy.input(input_files).split(26)
  for l in f1.here
    puts l
  end


when "14.1"

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

  puts "これは, 時間がかかります. デッドロックしているわけではありません"
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

  f1 = fairy.input(input_files).barrier(:mode=>:NODE_CREATION, :cond=>%{puts "COND:"; @Pool.mutex.lock}, :buffer=>:MEMORY, :BEGIN=>%{puts "AAAAAAAAAAAA"})
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

when "17.3"

  iota = fairy.times(100, :SPLIT_NO=>10)
  for l in iota.here
    puts l
  end


when "18", "emap"

  # ref 14.4

  input_files = ["/etc/passwd", "/etc/group"]

  p = "a"
  pv = []
  26.times{pv.push p; p = p.succ}

  fairy.def_pool_variable(:pv, pv)

  f1 = fairy.input(input_files).group_by(%{|e| @Pool.pv.find(proc{"z"}){|p| e < p}})
  f2 = f1.emap(%{|i| i.to_a.sort})
  f3 = f2.eshuffle(%{|i| i.sort{|s1, s2| s1.key <=> s2.key}})
  for l in f3.here
    puts l.inspect
  end

when "19", "there"

  f1 = 100.times.collect{|e| e}.there(fairy).split(2).split(4).map(%{|i| i})
  for l in f1.here
    puts l
  end

when "19.1"

  f1 = fairy.there(100.times).split(2).split(4).map(%{|i| i})
  for l in f1.here
    puts l
  end

when "20", "break"

  # これはどうさしない

  iota = fairy.input(Fairy::Iota, 1000)
  f = iota.map(%{|i| 
    if i == 50
       break 1000
    end
    i
  })
  for l in f.here
    puts l
  end
  sleep $sleep if $sleep 

when "21", "exception"

  iota = fairy.input(Fairy::Iota, 1000)
  f = iota.map(%{|i| 
    if i == 50
       fugegeu
    end
    i
  })
  for l in f.here
    puts l
  end

when "21.1"
  fairy.def_pool_variable(:foo, 1)
  fairy.def_pool_variable(:foo, 2)

when "22", "output varray"

  output_varray = fairy.input(Fairy::Iota, 1000).output(Fairy::VArray)
puts "X"
  puts output_varray.varray.peer_inspect
  for e in output_varray.varray
    p e
  end


when "22.1"

  input_files = ["/etc/passwd", "/etc/group"]
  output_varray = fairy.input(input_files).output(Fairy::VArray)
puts "X"
  puts output_varray.varray.peer_inspect
  for e in output_varray.varray
    p e
  end

when "23", "input varray"

  va = fairy.input(Fairy::Iota, 1000).to_va

  for l in fairy.input(va).here
    puts l
  end

when "23.1"

  va = fairy.input(Fairy::Iota, 1000).to_va
  10.times do |i|
    puts "itr#{i}"
    va = fairy.input(va).map(%{|i| i*2}).to_va
  end
  for l in fairy.input(va).here
    puts l
  end

when "23.2"

  va = fairy.input(Fairy::Iota, 100).to_va
  puts "va[10]: "
  p va[10]  
  puts "va[20]=500 "
  va[20]= 500
  p va[20]
  puts "EACH:"
  for l in va
    puts l
  end

when "24", "k-means"

  puts "これは動作しません"

  require "matrix"

  NoKluster = 2
  Threshold = 0.1

  Data = [[0, 0], [0, 0.5], [1, 1], [1, 0.5]]

  initial_centers = fairy.def_pool_variable(:NoKluster, NoKluster)

  fairy.def_pool_variable(:centers, 
			  :block=>%{require "matrix"
                                    @Pool.NoKluster.times.collect{Vector[rand, rand]}})

  measure = 100000

  va = Data.there(fairy).split(2).map(%{|data| Vector[*data]}, 
				      :BEGIN=>%{require "matrix"}).to_va

  while measure > Threshold
    cvpair = fairy.input(va).map(%{|v|
      [@Pool.centers.min_by{|c| (v - c).r}, v]})
    gpair = cvpair.group_by(%{|cv| cv[0]})
    cpair = gpair.emap(%{|i|
      n = 0
      [i.inject(0){|nc, c, v| n += 1; nc += v}/n, i.key]}).here
    measure = cpair.inject(0){|m, n, o| m += (n - i).r}
    fairy.pool_variable(:centers, cpair.map{|n, o| n})
  end

when "24.1"

  require "matrix"

  NoKluster = 2
  Threshold = 0.1

  Data = [[0.0, 0.0], [0.0, 0.5], [1.0, 1.0], [1.0, 0.5]]

  initial_centers = fairy.def_pool_variable(:NoKluster, NoKluster)

  fairy.def_pool_variable(:centers, 
			  :block=>%{require "matrix"
                                    @Pool.NoKluster.times.collect{Vector[rand, rand]}})

  p fairy.pool_variable(:centers)

  measure = 100000

  va = Data.there(fairy).split(2).map(%{|data| data = data.dc_deep_copy;Vector[*data]}, 
	 			      :BEGIN=>%{require "matrix"}).to_va

  va.each{|e| puts e.inspect}

  loop = 0
  while measure > Threshold
    puts "ITR: START LOOP: #{loop += 1}"

    cvpair = fairy.input(va).map(%{|v|
      v = v.dc_deep_copy
      [@Pool.centers.min_by{|c| c = c.dc_deep_copy; (v - c).r}, v]})

    puts "ITR: ph#1"
    gpair = cvpair.group_by(%{|c| c[0]})

    puts "ITR: ph#2"
    cpair = gpair.smap(%{|i, o|
      n = 0
      o.push [i.inject(Vector[0.0,0.0]){|nc, cv| 
                       c = cv[0].dc_deep_copy
                       v = cv[1].dc_deep_copy
                       n += 1
                       nc += v} * (1.0/n), i.key]}, 
		       :BEGIN=>%{require "matrix"}).here.to_a

    puts "ITR: ph#3"
    measure = cpair.inject(0){|m, no| 
      n = no[0].dc_deep_copy
      o = no[1].dc_deep_copy
      m += (n - o).r}
    puts "ITR: ph#4"
    fairy.pool_variable(:centers, cpair.map{|no| no[0].dc_deep_copy})

    puts "FINISH:"
    fairy.pool_variable(:centers).each{|e| puts e.inspect}
  end

when "24.2", "k-means-02"

  require "matrix"

  NoKluster = 2
  Threshold = 0.1

  Data = [[0, 0], [0, 0.5], [1, 1], [1, 0.5]]

  initial_centers = fairy.def_pool_variable(:NoKluster, NoKluster)

  fairy.def_pool_variable(:centers, 
			  :block=>%{require "matrix"
                                    @Pool.NoKluster.times.collect{Vector[rand, rand]}})

  puts "Init Centers:"
  fairy.pool_variable(:centers).each{|e| puts e.inspect}

  measure = 100000

  va = Data.there(fairy).split(2).map(%{|data| Vector[*data]}, 
				      :BEGIN=>%{require "matrix"}).to_va

  loop = 0
  while measure > Threshold
    puts "ITR: START LOOP: #{loop += 1}"

    cvpair = fairy.input(va).map(%{|v|
      [@Pool.centers.min_by{|c| (v - c).r}, v]})
    gpair = cvpair.group_by(%{|cv| cv[0]})
    cpair = gpair.smap(%{|i, o|
      n = 0
      o.push [i.inject(Vector[0.0,0.0]){|nc, cv| n += 1; nc += cv[1]}*(1.0/n), i.key.dc_dup]},
		       :BEGIN=>%{require "matrix"}).here.to_a
    
#    p cpair
    cpair.map{|n, o| n}.each{|e| p e}

    measure = cpair.inject(0){|m, no| m += (no[0] - no[1]).r}
    fairy.pool_variable(:centers, cpair.map{|no| no[0]})

    puts "ITR FINISH:"
    fairy.pool_variable(:centers).each{|e| puts e.inspect}
 end

when "24.3", "k-means-03"

  require "matrix"

  NoKluster = 2
  Threshold = 0.1

  Data = [[0, 0], [0, 0.5], [1, 1], [1, 0.5]]

  initial_centers = fairy.def_pool_variable(:NoKluster, NoKluster)

  fairy.def_pool_variable(:centers, 
			  :block=>%{require "matrix"
                                    @Pool.NoKluster.times.collect{Vector[rand, rand]}})

  puts "Init Centers:"
  fairy.pool_variable(:centers).each{|e| puts e.inspect}

  measure = 100000

  va = Data.there(fairy).split(2).map(%{|data| Vector[*data]}, 
				      :BEGIN=>%{require "matrix"}).to_va

  loop = 0
  while measure > Threshold
    puts "ITR: START LOOP: #{loop += 1}"

    cvpair = fairy.input(va).map(%{|v|
      [@Pool.centers.min_by{|c| (v - c).r}, v]})
    gpair = cvpair.group_by(%{|cv| cv[0]})
    cpair = gpair.emap(%{|i|
      n = 0
      new_c = i.inject(Vector[0.0,0.0]){|nc, cv| n += 1; nc += cv[1]}*(1.0/n)
      [[new_c, i.key]]},
		       :BEGIN=>%{require "matrix"}).here.to_a
    
#    p cpair

    cpair.map{|n, o| n}.each{|e| p e}
    measure = cpair.inject(0){|m, no| m += (no[0] - no[1]).r}
    fairy.pool_variable(:centers, cpair.map{|no| no[0]})

    puts "ITR FINISH:"
    fairy.pool_variable(:centers).each{|e| puts e.inspect}
 end

when "24.4", "k-means-04"
  require "matrix"

  NoKluster = 2
  Threshold = 0.1

  Data = [[0, 0], [0, 0.5], [1, 1], [1, 0.5]]

  initial_centers = fairy.def_pool_variable(:NoKluster, NoKluster)

  fairy.def_pool_variable(:centers, 
			  :block=>%{require "matrix"
                                    @Pool.NoKluster.times.collect{Vector[rand, rand]}})

  puts "Init Centers:"
  fairy.pool_variable(:centers).each{|e| puts e.inspect}

  measure = 100000

  va = Data.there(fairy).split(2).map(%{|data| Vector[*data]}, 
				      :BEGIN=>%{require "matrix"}).to_va

  loop = 0
  while measure > Threshold
    puts "ITR: START LOOP: #{loop += 1}"

    cvpair = fairy.input(va).map(%{|v|
      [@Pool.centers.min_by{|c| (v - c).r}, v]})
    gpair = cvpair.group_by(%{|c, v| c})
    cpair = gpair.emap(%{|i|
      n = 0
      new_c = i.inject(Vector[0.0,0.0]){|nc, (c, v)| n += 1; nc += v}*(1.0/n)
      [[new_c, i.key]]},
		       :BEGIN=>%{require "matrix"}).here.to_a
    
    measure = cpair.inject(0){|m, (n, o)| m += (n - o).r}

    fairy.pool_variable(:centers, cpair.map{|(n, o)| n})

    puts "ITR FINISH:"
    fairy.pool_variable(:centers).each{|e| puts e.inspect}
 end

when "24.5", "k-means-05"
  require "matrix"

  NoKluster = 2
  Threshold = 0.1

  Data = [[0, 0], [0, 0.5], [1, 1], [1, 0.5]]

  initial_centers = fairy.def_pool_variable(:NoKluster, NoKluster)

  fairy.def_pool_variable(:centers, 
			  :block=>%{require "matrix"
                                    @Pool.NoKluster.times.collect{Vector[rand, rand]}})

  puts "Init Centers:"
  fairy.pool_variable(:centers).each{|e| puts e.inspect}

  measure = 100000

  va = Data.there(fairy).split(2).map(%{|data| Vector[*data]}, 
				      :BEGIN=>%{require "matrix"}).to_va
  loop = 0
  while measure > Threshold
    puts "ITR: START LOOP: #{loop += 1}"

    cvpair = fairy.input(va).map(%{|v| [@Pool.centers.min_by{|c| (v - c).r}, v]})
    gpair = cvpair.group_by(%{|c, v| c})
    cpair = gpair.emap(%{|i|
      n = 0
      new_c = i.inject(Vector[0.0,0.0]){|nc, (c, v)| n += 1; nc += v}*(1.0/n)
      [[new_c, i.key]]},
		       :BEGIN=>%{require "matrix"}).here.to_a
    
    measure = cpair.inject(0){|m, (n, o)| m += (n - o).r}

    fairy.pool_variable(:centers, cpair.map{|n, o| n})

    puts "ITR FINISH:"
    fairy.pool_variable(:centers).each{|e| puts e.inspect}
 end

  sleep 100

when "25.1", "block"
  
  Data = [[0, 0], [0, 0.5], [1, 1], [1, 0.5]]
  
  here = Data.there(fairy).map(%{|e1, e2| e1}).here
  for l in here
    p l
  end

when "25.2"
  
  Data = [[0, 0], [0, 0.5], [1, 1], [1, 0.5]]
  
  here = Data.there(fairy).map(%{|e1| e1}).here
  for l1, l2 in here
    p l1, l2
  end

when "26", "inject"

  iota = fairy.input(Fairy::Iota, 101, :SPLIT_NO=>10)
  inject = iota.inject(%{|sum, value| sum + value})
  p inject.value

when "26.1"

  iota = fairy.input(Fairy::Iota, 1001, :SPLIT_NO=>10)
  inject = iota.inject(%{|sum, value| sum + value})
  p inject.value

when "26.2", "min"

  iota = fairy.input(Fairy::Iota, 1001, :SPLIT_NO=>10, :offset=>10)
  min = iota.min(%{|x, y| -(x<=>y)})
  p min.value

when "26.3", "max"

  iota = fairy.input(Fairy::Iota, 1001, :SPLIT_NO=>10, :offset=>10)
  max = iota.max
  p max.value

when "26.4", "min_by"

  iota = fairy.input(Fairy::Iota, 1001, :SPLIT_NO=>10, :offset=>10)
  minby = iota.min_by(%{|x| -x})
  p minby.value

when "26.5", "max_by"

  iota = fairy.input(Fairy::Iota, 1001, :SPLIT_NO=>10, :offset=>10)
  maxby = iota.max_by(%{|x| x})
  p maxby.value

when "26.6"

  iota = fairy.input(Fairy::Iota, 1001, :SPLIT_NO=>10)
  inject = iota.inject(%{|sum, value| sum + value})
  for l in inject.here
    p l
  end

when "27", "terminate"

  iota = fairy.input(Fairy::Iota, 1001, :SPLIT_NO=>10, :offset=>10)
  maxby = iota.max_by(%{|x| x})
  p maxby.value
  # 途中で^C


when "27.1"

  iota = fairy.input(Fairy::Iota, 1001, :SPLIT_NO=>10, :offset=>10)
  maxby = iota.max_by(%{|x| x})
  p maxby.value
  sleep 100

when "28", "mgroup_by"
  
  iota = fairy.input(Fairy::Iota, 101, :SPLIT_NO=>10, :offset=>10)
  f3 = iota.mgroup_by(%{|e| [e-1, e, e+1]}).emap(%{|i| [i.to_a]})
  for l in f3.here
    puts "#[#{l.inspect}]"
  end

when "29", "lifegame"
  require "matrix"

  Offsets =  [
    [-1, -1], [-1, 0], [-1, 1], 
    [0, -1],  [0, 0],  [0, 1], 
    [1, -1],  [1, 0],  [1, 1]
  ]
  InitialPositions = [
             [-1, 0], [-1, 1],
    [0, -1], [0, 0],
             [1, 0],
  ]

puts "X:1"
  va = InitialPositions.there(fairy).split(2).map(%{|p| Vector[*p.to_a]},
						  :BEGIN=>%{require "matrix"}).to_va

puts "X:2"

  fairy.def_pool_variable(:offsets, Offsets.map{|p| Vector[*p.to_a]})
puts "X:3"

  loop = 0
  loop do
    puts "ITR: #{loop+=1}"
    
    f1 = fairy.input(va).mgroup_by(%{|v| @Pool.offsets.collect{|o| v + o}},
		      :BEGIN=>%{require "matrix"})
    va = f1.smap(%{|i, o| 
      lives = i.to_a
      if lives.include?(i.key) && (lives.size == 3 or lives.size == 4)
        o.push i.key
      elsif lives.size == 3
        o.push i.key
      end
    }, :BEGIN=>%{require "matrix"}).to_va
    
    puts va.to_a.each{|v| puts v}
  end

when "30", "handle_exeption"
  puts "例外なし"
  here = fairy.input(["/etc/passwd", "/etc/group"]).map(%{|l| l.chomp.split(/:/)}).here
  for l in here
    print l.join("-"), "\n"
  end
  sleep $sleep if $sleep 

when "30.1"
  puts "例外あり"

#   module Forwardable
#     @debug = true
#   end

  here = fairy.input(["/etc/passwd", "/etc/group"]).map(%{|l| 
     l.chombo.split(/:/)
  }).here
  for l in here
    print l.join("-"), "\n"
  end
  sleep $sleep if $sleep 

when "31", "stdout"

  iota = fairy.input(Fairy::Iota, 1001, :SPLIT_NO=>10, :offset=>10)
  minby = iota.min_by(%{|x| puts x; -x})
  p minby.value


when "31.1"

  iota = fairy.input(Fairy::Iota, 1001, :SPLIT_NO=>10, :offset=>10)
  minby = iota.min_by(%{|x| p x; -x})
  p minby.value

when "32", "find"

  iota = fairy.input(Fairy::Iota, 1001, :SPLIT_NO=>10, :offset=>10)
  find = iota.find(%{|x| x == 10})
  p find.value

when "32.1"

  iota = fairy.input(Fairy::Iota, 1001, :SPLIT_NO=>10, :offset=>10)
  find = iota.find(%{|x| x == 500})
  p find.value

when "33", "gbreak"
  
  iota = fairy.input(Fairy::Iota, 1001, :SPLIT_NO=>10, :offset=>10)
  here = iota.map(%{|x| if x == 500; gbreak; else x; end}).here
  for l in here
    puts l
  end

  sleep 2


when "33.1"
  here = fairy.input(["/etc/passwd", "/etc/group"]).map(%{|l| 
    l0 = l.chomp.split(/:/)
    if l0[0] == "keiju"
      gbreak
    else
      l0
    end}).here
  for l in here
    print l.join("-"), "\n"
  end
  sleep 2

when "33.2"
  
  iota = fairy.input(Fairy::Iota, 1001, :SPLIT_NO=>10, :offset=>10)
  here = iota.map(%{|x| if x == 500; break; else x; end}).here
  for l in here
    puts l
  end

  sleep 2

when "33.3"
  here = fairy.input(["/etc/passwd", "/etc/group"]).map(%{|l| 
    l0 = l.chomp.split(/:/)
    if l0[0] == "keiju"
      break
    else
      l0
    end}).here
  for l in here
    print l.join("-"), "\n"
  end
  sleep 2

when "34", "serialize msort"

  va = fairy.input(["/etc/passwd", "/etc/group"]).emap(%{|i| i.to_a.sort}).to_va

  sampling = fairy.input(va).select(%{|e| (i += 1) % 10 == 0}, :BEGIN=>%{i = 0}).here.sort
  
  puts "SAMPLING:"
  p sampling
  
  puts "PIVOTS:" 
  pvs = sampling.values_at(sampling.size.div(3), (sampling.size*2).div(3), -1)
  fairy.def_pool_variable(:pvs, pvs)
  p pvs

  div = fairy.input(va).group_by(%{|e| 
   key = @Pool.pvs.find{|pv| e <= pv}
   key ? key : @Pool.pvs.last})

  msort = div.emap(%{|i| 
    buf = []
    i.each{|e|
       idx = buf.rindex{|b| b < e}
       if idx 
         buf.insert(idx+1, e)
       else
         buf.unshift e
       end}
    buf})
  shuffle = msort.eshuffle(%{|i| i.sort{|s1, s2| s1.key <=> s2.key}})
  puts "RESULT:"
  for l in shuffle.here
    puts l
  end

when "34.1"

  SAMPLING_RATIO_1_TO = 10
  PVN = 4

  va = fairy.input(["/etc/passwd", "/etc/group"]).emap(%{|i| i.to_a.sort}).to_va

  puts "SAMPLING: RATIO: 1/#{SAMPLING_RATIO_1_TO}"
  sample = fairy.input(va).select(%{|e| (i += 1) % #{SAMPLING_RATIO_1_TO} == 0},
				    :BEGIN=>%{i = 0}).here.sort
  p sample
  
  puts "PIVOTS:" 
  idxes = (1...PVN).collect{|i| (sample.size*i).div(PVN)}
  idxes.push -1
  pvs = sample.values_at(*idxes)
  fairy.def_pool_variable(:pvs, pvs)
  p pvs

  div = fairy.input(va).group_by(%{|e| 
   key = @Pool.pvs.find{|pv| e <= pv}
   key ? key : @Pool.pvs.last})

  msort = div.emap(%{|i| 
    buf = []
    i.each{|e|
       idx = buf.rindex{|b| b < e}
       if idx 
         buf.insert(idx+1, e)
       else
         buf.unshift e
       end}
    buf})
  shuffle = msort.eshuffle(%{|i| i.sort{|s1, s2| s1.key <=> s2.key}})
  puts "RESULT:"
  for l in shuffle.here
    puts l
  end

when "35.0"
  finput = fairy.input("sample/wc/data/fairy.cat")
  fmap = finput.smap(%{|i,o|
    i.each{|ln|
      ln.chomp.split.each{|w| o.push(w)}
    }
  })
  for w in fmap.here
    puts w
  end

when "35.1"
  finput = fairy.input("sample/wc/data/fairy.cat")
  fmap = finput.smap(%{|i,o|
    i.each{|ln|
      ln.chomp.split.each{|w| o.push(w)}
    }
  })
  fshuffle = fmap.group_by(%{|w| w.hash % 5})
  for w in fshuffle.here
    puts w
  end

when "35.2"
  finput = fairy.input("sample/wc/data/fairy.cat")
#  finput = fairy.input("sample/wc/data/wc.vf")
  fmap = finput.smap(%{|i,o|
    i.each{|ln|
      begin
         ln.chomp.split.each{|w| o.push(w)}
      rescue
      end
    }
  })
  fshuffle = fmap.group_by(%{|w| w.hash % 5})
  freduce = fshuffle.smap(%q{|i,o| o.push("#{i.key}\t#{i.size}")})
  for w in freduce.here
    puts w
  end

when "35.3"
  finput = fairy.input("sample/wc/data/fairy.cat")
#  finput = fairy.input("sample/wc/data/wc.vf")
  fmap = finput.smap(%{|i,o|
    i.each{|ln|
      ln.chomp.split.each{|w| o.push(w)}
    }
  })
  fshuffle = fmap.group_by(%{|w| w.hash % 500})
  freduce = fshuffle.smap(%q{|i,o| o.push("#{i.key}\t#{i.size}")})
  for w in freduce.here
    puts w
  end

when "35.4"
  finput = fairy.input("sample/wc/data/fairy.cat")
#  finput = fairy.input("sample/wc/data/wc.vf")
  fmap = finput.smap(%{|i,o|
    i.each{|ln|
      ln.chomp.split.each{|w| o.push(w)}
    }
  })
  fshuffle = fmap.group_by(%{|w| w})
  freduce = fshuffle.smap(%q{|i,o| o.push("#{i.key}\t#{i.size}")})
  for w in freduce.here
    puts w
  end

when "35.5"
  finput = fairy.input("sample/wc/data/fairy.cat")
#  finput = fairy.input("sample/wc/data/wc.vf")
  fmap = finput.smap(%{|i,o|
    i.each{|ln|
      ln.chomp.split.each{|w| o.push(w)}
    }
  })
  fshuffle = fmap.group_by(%{|w| w.hash % 20})
  freduce = fshuffle.smap(%q{|i,o| 
    words = {}
    i.each{|w|
      if words.key?(w)
        words[w] += 1
      else
        words[w] = 1
      end
    }
    for w, size in words
       o.push("#{w}\t#{size}")
    end
  })
  for w in freduce.here
    puts w
  end

when "35.5.1"
  finput = fairy.input("sample/wc/data/fairy.cat")
#  finput = fairy.input("sample/wc/data/wc.vf")
  fmap = finput.smap(%{|i,o|
    i.each{|ln|
      ln.chomp.split.each{|w| o.push(w)}
    }
  })
  fshuffle = fmap.group_by(%{|w| w.hash % 20})
  freduce = fshuffle.smap(%q{|i,o| 
    words = i.group_by{|w| w}
    for w, ww in words
       o.push("#{w}\t#{ww.size}")
    end
  })
  for w in freduce.here
    puts w
  end

when "35.6"
  finput = fairy.input("sample/wc/data/fairy.cat")
#  finput = fairy.input("sample/wc/data/wc.vf")
  fmap = finput.smap(%{|i,o|
    i.each{|ln|
      ln.chomp.split.each{|w| o.push(w)}
    }
  })
  fshuffle = fmap.group_by(%{|w| w.hash % 20})
  freduce = fshuffle.smap(%q{|i,o| 
    s = i.to_a.sort_by{|w| w}
    key = nil
    count = 0
    s.each{|w|
      if key == w
        count+=1
      else
         o.push("#{key}\t#{count}")
         key = w
         count = 1
      end
    }
    o.push("#{key}\t#{count}")
  })
  for w in freduce.here
    puts w
  end

when "36.0", "mod_group_by"
  finput = fairy.input("sample/wc/data/fairy.cat")
  fmap = finput.smap(%{|i,o|
    i.each{|ln|
      ln.chomp.split.each{|w| o.push(w)}
    }
  })
  fshuffle = fmap.mod_group_by(%{|w| w})
  for w in fshuffle.here
    puts w
  end
  sleep 1

when "36.1"
  finput = fairy.input("sample/wc/data/fairy.cat")
  fmap = finput.smap(%{|i,o|
    i.each{|ln|
      ln.chomp.split.each{|w| o.push(w)}
    }
  })
  fshuffle = fmap.mod_group_by(%{|w| w})
  freduce = fshuffle.map(%q{|key, values| "#{key}\t#{values.size}"})
  for w in freduce.here
    puts w
  end

  sleep 2

when "36.1.1"
  finput = fairy.input("sample/wc/data/fairy.cat")
  fmap = finput.smap(%{|i,o|
    i.each{|ln|
      ln.chomp.split.each{|w| o.push(w)}
    }
  })
  fshuffle = fmap.mod_group_by(%{|w| w})
  freduce = fshuffle.map(%q{|key, values| [key, values.size]})
  freduce.here.sort_by{|w| w[1]}.each{|w| puts "key: #{w[0]} count: #{w[1]}"}

  sleep 2

when "37", "merge_group_by"

  SAMPLING_RATIO_1_TO = 10
  PVN = 4

  va = fairy.input(["/etc/passwd", "/etc/group"]).emap(%{|i| i.to_a.sort}).to_va

  puts "SAMPLING: RATIO: 1/#{SAMPLING_RATIO_1_TO}"
  sample = fairy.input(va).select(%{|e| (i += 1) % #{SAMPLING_RATIO_1_TO} == 0},
				    :BEGIN=>%{i = 0}).here.sort
  p sample
  
  puts "PIVOTS:" 
  idxes = (1...PVN).collect{|i| (sample.size*i).div(PVN)}
  idxes.push -1
  pvs = sample.values_at(*idxes)
  fairy.def_pool_variable(:pvs, pvs)
  p pvs

  puts "MergeGroupBy:" 
  div = fairy.input(va).merge_group_by(%{|e| 
    key = @Pool.pvs.find{|pv| e <= pv}
    key ? key : @Pool.pvs.last})

  puts "SMAP:" 
  msort = div.smap(%{|i, o|
    buf = i.map{|st| [st, st.pop]}.select{|st, v|!v.nil?}.sort_by{|st, v| v}
    while st_min = buf.shift
      st, min = st_min
      o.push min
      next unless v = st.pop
      idx = buf.rindex{|st, vv| vv < v}
      idx ? buf.insert(idx+1, [st, v]) : buf.unshift([st, v])
    end})
  puts "SHUFFLE:" 
  shuffle = msort.eshuffle(%{|i| i.sort{|s1, s2| s1.key <=> s2.key}})
  puts "RESULT:"
  for l in shuffle.here
    puts l.inspect
  end

#  sleep 5

when "37.0"

  SAMPLING_RATIO_1_TO = 10
  PVN = 4

  va = fairy.input(["/etc/passwd", "/etc/group"]).emap(%{|i| i.to_a.sort}).to_va

  puts "SAMPLING: RATIO: 1/#{SAMPLING_RATIO_1_TO}"
  sample = fairy.input(va).select(%{|e| (i += 1) % #{SAMPLING_RATIO_1_TO} == 0},
				    :BEGIN=>%{i = 0}).here.sort
  p sample
  
  puts "PIVOTS:" 
  idxes = (1...PVN).collect{|i| (sample.size*i).div(PVN)}
  idxes.push -1
  pvs = sample.values_at(*idxes)
  fairy.def_pool_variable(:pvs, pvs)
  p pvs

  div = fairy.input(va).merge_group_by(%{|e| 
    key = @Pool.pvs.find{|pv| e <= pv}
    key ? key : @Pool.pvs.last})

  f1 = div.smap(%{|i, o|
puts "START"
    i.map{|st| puts st; st.each{|v| puts v; o.push v}}})
  puts "RESULT:"
  for l in f1.here
    puts l
  end

when "38"

#  f1 = fairy.input(["/etc/passwd", "/etc/group", "/etc/group"])
#  f2 = fairy.input(["/etc/group", "/etc/group"])
  f1 = fairy.input(["/etc/passwd"])
  f2 = fairy.input(["/etc/group"])
  f3 = f1.product(f2, %{|e1, e2| e1.chomp+"+"+e2})
  for l in f3.here
    puts l
  end


when "38.1"

  f1 = fairy.input(Fairy::Iota, 10, :SPLIT_NO=>1)
  f2 = fairy.input(Fairy::Iota, 20, :SPLIT_NO=>1, :offset=>10)
  f3 = f1.product(f2, %{|e1, e2| e1.to_s+"+"+e2.to_s})
  for l in f3.here
    puts l
  end

when "38.1.1"

  f1 = fairy.input(Fairy::Iota, 10, :SPLIT_NO=>2)
  f2 = fairy.input(Fairy::Iota, 10, :SPLIT_NO=>2)
  f3 = f1.product(f2, %{|e1, e2| e1.to_s+"+"+e2.to_s})
  for l in f3.here
    puts l
  end

when "38.1.2"
  f1 = fairy.input(Fairy::Iota, 10, :SPLIT_NO=>4)
  f2 = fairy.input(Fairy::Iota, 10, :SPLIT_NO=>4)
  f3 = f1.product(f2, %{|e1, e2| e1.to_s+"+"+e2.to_s})
  for l in f3.here.sort
    puts l
  end

when "38.2"
  f1 = fairy.input(Fairy::Iota, 10, :SPLIT_NO=>4)
  f2 = fairy.input(Fairy::Iota, 10, :SPLIT_NO=>4)
  f3 = fairy.input(Fairy::Iota, 10, :SPLIT_NO=>4)
  f4 = f1.product(f2, f3, %{|e1, e2, e3| e1.to_s+"+"+e2.to_s+"+"+e3.to_s})
  for l in f4.here.sort
    puts l
  end

when "39", "sub"

  f0 = fairy.input(["/etc/passwd", "/etc/group"])
  f1 = f0.sub{|subfairy, input|
    SAMPLING_RATIO_1_TO = 10
    PVN = 4

    va = input.emap(%{|i| i.to_a.sort}).to_va

    puts "SAMPLING: RATIO: 1/#{SAMPLING_RATIO_1_TO}"
    sample = subfairy.input(va).select(%{|e| (i += 1) % #{SAMPLING_RATIO_1_TO} == 0},
				    :BEGIN=>%{i = 0}).here.sort
    p sample
  
    puts "PIVOTS:" 
    idxes = (1...PVN).collect{|i| (sample.size*i).div(PVN)}
    idxes.push -1
    pvs = sample.values_at(*idxes)
    subfairy.def_pool_variable(:pvs, pvs)
    p pvs

    puts "MergeGroupBy:" 
    div = subfairy.input(va).merge_group_by(%{|e| 
       key = @Pool.pvs.find{|pv| e <= pv}
       key ? key : @Pool.pvs.last})

    puts "SMAP:" 
    msort = div.smap(%{|i, o|
    buf = i.map{|st| [st, st.pop]}.select{|st, v|!v.nil?}.sort_by{|st, v| v}
    while st_min = buf.shift
      st, min = st_min
      o.push min
      next unless v = st.pop
      idx = buf.rindex{|st, vv| vv < v}
      idx ? buf.insert(idx+1, [st, v]) : buf.unshift([st, v])
    end})
    puts "SHUFFLE:" 
    shuffle = msort.eshuffle(%{|i| i.sort{|s1, s2| s1.key <=> s2.key}})
  }

  puts "RESULT:"
  for l in f1.here
    puts l.inspect
  end

when "39.0"

  f0 = fairy.input(["/etc/passwd", "/etc/group"])
  f1 = f0.sub{|subfairy, input| input.grep(/keiju/)}
  for l in f1.here
    puts l.inspect
  end


when "40", "def_filter"
  Fairy.def_filter(:grep_keiju) do |fairy, input|
    input.select(%{|e| /keiju/ =~ e})
  end

  f0 = fairy.input(["/etc/passwd", "/etc/group"])
  f1 = f0.grep_keiju
  for l in f1.here
    puts l.inspect
  end

when "40.1"
  Fairy.def_filter(:grep_keiju, :sub=>true) do |fairy, input|
    input.select(%{|e| /keiju/ =~ e})
  end

  f0 = fairy.input(["/etc/passwd", "/etc/group"])
  f1 = f0.grep_keiju
  for l in f1.here
    puts l.inspect
  end

when "41", "join"
  join = fairy.input("/etc/passwd", "/etc/group")
  main = fairy.input("/etc/passwd", "/etc/group").join(join, %{|in0, in2, out| 
    in0.to_a.zip(in2.to_a).each{|e1, e2| out.push e1.chomp+"+"+e2.chomp}}).here

  for l in main
    puts l
  end
  sleep 3

when "42", "equijoin"
  # これは, 正常に動作しない. -> 42.1
  MOD = 5
  puts "P#1"
#  main = fairy.input("/etc/passwd").map(%{|e| e.chomp.split(/:/)}).group_by(%{|*e| e[0].hash % #{MOD}})
  main = fairy.input("/etc/passwd").map(%{|e| e.chomp.split(/:/)}).group_by(%{|e| e[0]})
  puts "P#2"
#  other = fairy.input("/etc/group").map(%{|e| e.chomp.split(/:/)}).group_by(%{|*e| e[0].hash % #{MOD}})
  puts "P#2"
#  other = fairy.input("/etc/group").map(%{|e| e.chomp.split(/:/)}).group_by(%{|*e| e[0]})
#  other = fairy.input("/etc/passwd").map(%{|e| e.chomp.split(/:/)}).group_by(%{|e| e[0]})
  other = fairy.input("/etc/passwd").map(%{|e| e.chomp.split(/:/)}).group_by(%{|e| e[0]}).barrier(:mode=>:NODE_CREATION, :cond=>:NODE_ARRIVED, :buffer=>:MEMORY)
  puts "P#3"
  j = main.join(other, %{|in0, in1, out|

    next unless in0 && in1    

    ary_m = in0.group_by{|e| e[0]}
    ary_o = in1.group_by{|e| e[0]}

    ary_m.each{|key, values|
#      puts "KEY: \#{key}"
#      puts "VALUE: \#{values}"

      o_values = ary_o[key]
      next unless o_values
      values.each{|value|
#        p value
        o_values.each{|o_value|
          ary = [*value].push *o_value
          out.push ary
       }
      }
    }
  }, :by => :key)
  puts "P#4"
  for *l in j.here
    puts l.inspect
  end

  sleep 5

when "42.1"
  
  main = fairy.input("/etc/group").map(%{|e| e.chomp.split(/:/)})
#  main = fairy.input("/etc/passwd").map(%{|e| e.chomp.split(/:/)})
  other = fairy.input("/etc/passwd").map(%{|e| e.chomp.split(/:/)})
  count = 0
  for l in main.equijoin(other, 0).here
    count += 1
    puts l.inspect
  end
  puts "COUNT: #{count}"

when "42.2.1"
  main = fairy.input(["/etc/passwd"]).map(%{|e| e.chomp.split(/:/)}).group_by(%{|e| e[0]})
  for *l in main.here
    puts l.inspect
  end

when "42.2.2"
  main = fairy.input(["/etc/passwd"]).map(%{|e| e}).group_by(%{|e| e.split(/:/)[0]})
  for *l in main.here
    puts l.inspect
  end

when "42.2.3"
  main = fairy.input(["/etc/passwd"]).map(%{|e| e.chomp.split(/:/)[0]}).group_by(%{|e| e})
  for *l in main.here
    puts l.inspect
  end

when "43", "cat"
  other = fairy.input(["/etc/group"])
  main = fairy.input(["/etc/passwd"]).cat(other)
  for l in main.here
    puts l.inspect
  end

when "43.2", "equijoin2"
  main = fairy.input("/etc/group").map(%{|e| e.chomp.split(/:/)})
#  main = fairy.input("/etc/passwd").map(%{|e| e.chomp.split(/:/)})
  other = fairy.input("/etc/passwd").map(%{|e| e.chomp.split(/:/)})
  count = 0
  for *l in main.equijoin2(other, 0).here
    count += 1
    puts l.inspect
  end
  puts "COUNT: #{count}"

when "44", "flatten"
  main = fairy.input("/etc/passwd").mapf(%{|e| e.chomp.split(/:/)})
  for l in main.here
    puts l.inspect
  end
  puts "COUNT: #{count}"

when "45", "simple file by key buffer"
  finput = fairy.input(["/etc/passwd"])
  fmap = finput.smap(%{|i,o|
    i.each{|ln|
      ln.chomp.split(/:/).each{|w| o.push(w)}
    }
  })
  fshuffle = fmap.mod_group_by(%{|w| w}, :buffering_policy => {:buffering_class => :SimpleFileByKeyBuffer})
#  fshuffle = fmap.mod_group_by(%{|w| w})
  freduce = fshuffle.map(%q{|key, values| "#{key}\t#{values.size}"})
  for w in freduce.here
    puts w
  end

  sleep 2

when "45.0"
  finput = fairy.input("sample/wc/data/fairy.cat")
#  finput = fairy.input(["/etc/passwd"])
  fmap = finput.smap(%{|i,o|
    i.each{|ln|
      ln.chomp.split.each{|w| o.push(w)}
#      ln.chomp.split(":").each{|w| o.push(w)}
    }
  })
  fshuffle = fmap.mod_group_by(%{|w| w}, :buffering_policy => {:buffering_class => :OnMemoryBuffer})
#  fshuffle = fmap.mod_group_by(%{|w| w})
  freduce = fshuffle.map(%q{|key, values| "#{key}\t#{values.size}"})
  for w in freduce.here
    puts w
  end

  sleep 2


when "45.1"
  finput = fairy.input("sample/wc/data/fairy.cat")
#  finput = fairy.input(["/etc/passwd"])
  fmap = finput.smap(%{|i,o|
    i.each{|ln|

      ln.chomp.split.each{|w| o.push(w)}
#      ln.chomp.split(":").each{|w| o.push(w)}
    }
  })
  fshuffle = fmap.mod_group_by(%{|w| w}, :buffering_policy => {:buffering_class => :SimpleCommandSortBuffer})
#  fshuffle = fmap.mod_group_by(%{|w| w})
  freduce = fshuffle.map(%q{|key, values| "#{key}\t#{values.size}"})
  for w in freduce.here
    puts w
  end

  sleep 2

when "45.2", "Command Merge Sort Buffer"
  finput = fairy.input("sample/wc/data/fairy.cat")
#  finput = fairy.input(["/etc/passwd"])
  fmap = finput.smap(%{|i,o|
    i.each{|ln|
      ln.chomp.split.each{|w| o.push(w)}
#      ln.chomp.split(":").each{|w| o.push(w)}
    }
  })
  fshuffle = fmap.mod_group_by(%{|w| w}, :buffering_policy => {:buffering_class => :CommandMergeSortBuffer})
#  fshuffle = fmap.mod_group_by(%{|w| w}, :buffering_policy => {:buffering_class => :CommandMergeSortBuffer, :threshold => 100})
#  fshuffle = fmap.mod_group_by(%{|w| w})
  freduce = fshuffle.map(%q{|key, values| "#{key}\t#{values.size}"})
  for w in freduce.here
    puts w
  end

  sleep 2


when "45.3", "Merge Sort Buffer"
  finput = fairy.input("sample/wc/data/fairy.cat")
#  finput = fairy.input(["/etc/passwd"])
  fmap = finput.smap(%{|i,o|
    i.each{|ln|
      ln.chomp.split.each{|w| o.push(w)}
#      ln.chomp.split(":").each{|w| o.push(w)}
    }
  })
#  fshuffle = fmap.mod_group_by(%{|w| w}, :buffering_policy => {:buffering_class => :CommandMergeSortBuffer})
  fshuffle = fmap.mod_group_by(%{|w| w}, :buffering_policy => {:buffering_class => :MergeSortBuffer, :threshold => 100})
#  fshuffle = fmap.mod_group_by(%{|w| w})
  freduce = fshuffle.map(%q{|key, values| "#{key}\t#{values.size}"})
  for w in freduce.here
    puts w
  end

  sleep 2

when "46"
#  f = fairy.input("sample/wc/data/fairy.cat").sort_by(%{|w| w})
  f = fairy.input(["/etc/passwd", "/etc/group"]).sort_by(%{|w| w})
  for w in f.here
    puts w
  end

when "47.1"
  Fairy.def_filter(:test_sort_by) do |fairy, input, block_source, *opts|
    
    sampling_ratio_1_to = opts[:sampling_ratio]
    sampling_ratio_1_to ||= Fairy::CONF.SORT_SAMPLING_RATIO_1_TO
    pvn = opts[:pvn]
    pvn ||= Fairy::CONF.SORT_N_GROUP_BY
    
    va = input.emap(%{|i| 
    sort_proc = proc{#{block_source}}
    i.to_a.collect{|e| [sort_proc.call(e), e]}.sort_by{|e| e.first}}).to_va

    if va.size/sampling_ratio_1_to < Fairy::CONF.SORT_SAMPLING_MIN
      sampling_ratio_1_to = Fairy::CONF.SORT_SAMPLING_MIN.div(va.size)
    end
    if va.size/sampling_ratio_1_to > Fairy::CONF.SORT_SAMPLING_MAX
      sampling_ratio_1_to = Fairy::CONF.SORT_SAMPLING_MAX.div(va.size)
    end

    Fairy::Log::debug(self, "SAMPLING: RATIO: 1/#{sampling_ratio_1_to}")
    sample = fairy.input(va).select(%{|e| (i += 1) % #{sampling_ratio_1_to} == 0},
				    :BEGIN=>%{i = 0}).here.sort_by{|e| e.first}.map{|e| e.first}

    idxes = (1...pvn).collect{|i| (sample.size*i).div(pvn)}
    idxes.push -1
    pvs = sample.values_at(*idxes)
    Fairy::Log::debug(self, "PVS: #{pvs.inspect}")
    fairy.def_pool_variable(:pvs, pvs)

    div = fairy.input(va).merge_group_by(%{|e| 
    key = @Pool.pvs.find{|pv| e.first <= pv}
    key ? key : @Pool.pvs.last})

    msort = div.smap(%{|i, o|

    raise "foo"

    buf = i.map{|st| [st, st.pop.dc_deep_copy]}.select{|st, v|!v.nil?}.sort_by{|st, v| v.first}
    while st_min = buf.shift
      st, min = st_min
      o.push min.last
      next unless v = st.pop.dc_deep_copy # 取りあえずの対応
      idx = buf.rindex{|st0, v0| v0.first <= v.first}
      idx ? buf.insert(idx+1, [st, v]) : buf.unshift([st, v])
    end})
    
    shuffle = msort.eshuffle(%{|i| i.sort{|s1, s2| s1.key <=> s2.key}})
    #  shuffle = msort.eshuffle(%{|i| i.sort_by{|s1| Log::debug(self, s1.key.inspect); s1.key}})
  end

  f = fairy.input(["/etc/passwd", "/etc/group"]).test_sort_by(%{|w| w})
  for w in f.here
    puts w
  end

when "47.2"
  input = fairy.input(["/etc/passwd", "/etc/group"])

  sampling_ratio_1_to = Fairy::CONF.SORT_SAMPLING_RATIO_1_TO
  pvn = Fairy::CONF.SORT_N_GROUP_BY
    
  va = input.emap(%{|i| 
    sort_proc = proc{|w| w}
    i.to_a.collect{|e| [sort_proc.call(e), e]}.sort_by{|e| e.first}}).to_va

  if va.size/sampling_ratio_1_to < Fairy::CONF.SORT_SAMPLING_MIN
    sampling_ratio_1_to = Fairy::CONF.SORT_SAMPLING_MIN.div(va.size)
  end
  if va.size/sampling_ratio_1_to > Fairy::CONF.SORT_SAMPLING_MAX
    sampling_ratio_1_to = Fairy::CONF.SORT_SAMPLING_MAX.div(va.size)
  end

  Fairy::Log::debug(self, "SAMPLING: RATIO: 1/#{sampling_ratio_1_to}")
  sample = fairy.input(va).select(%{|e| (i += 1) % #{sampling_ratio_1_to} == 0},
				    :BEGIN=>%{i = 0}).here.sort_by{|e| e.first}.map{|e| e.first}

  idxes = (1...pvn).collect{|i| (sample.size*i).div(pvn)}
  idxes.push -1
  pvs = sample.values_at(*idxes)
  Fairy::Log::debug(self, "PVS: #{pvs.inspect}")
  fairy.def_pool_variable(:pvs, pvs)

  div = fairy.input(va).merge_group_by(%{|e| 
    key = @Pool.pvs.find{|pv| e.first <= pv}
    key ? key : @Pool.pvs.last})

  msort = div.smap(%{|i, o|

    raise "foo"

    buf = i.map{|st| [st, st.pop.dc_deep_copy]}.select{|st, v|!v.nil?}.sort_by{|st, v| v.first}
    while st_min = buf.shift
      st, min = st_min
      o.push min.last
      next unless v = st.pop.dc_deep_copy # 取りあえずの対応
      idx = buf.rindex{|st0, v0| v0.first <= v.first}
      idx ? buf.insert(idx+1, [st, v]) : buf.unshift([st, v])
    end})
    
  shuffle = msort.eshuffle(%{|i| i.sort{|s1, s2| s1.key <=> s2.key}})
    #  shuffle = msort.eshuffle(%{|i| i.sort_by{|s1| Log::debug(self, s1.key.inspect); s1.key}})

  for w in shuffle.here
    puts w
  end

when "48"

  iota = fairy.input(Fairy::Iota, 1000)
  f = iota.map(%{|i| 
    if i == 50
       fugegeu
    end
    i
  })
  begin
  for l in f.here
    puts l
  end
  rescue
    puts "HOGE"
    p $!
  end


when "48.1"

  iota = fairy.input(Fairy::Iota, 1000)
  begin
  f = iota.map(%{|i| 
    if i == 50
       fugegeu
    end
    i
  })
  rescue
    puts "HOGEGE"
    p $!
  end

  sleep 10

  begin
  for l in f.here
    puts l
  end
  rescue
    puts "HOGE"
    p $!
  end

when "49", "file buffering queue"
  
  iota = fairy.input(Fairy::Iota, 1000)
  f = iota.smap(%{|i, o| i.each{|e| o.push e}}, 
		 :prequeuing_policy => {
		   :queuing_class => :FileBufferdQueue, 
		   :threshold => 10})
  for l in f.here
    puts l
  end



when "49.1"
  
  iota = fairy.input(Fairy::Iota, 1000)
  f = iota.smap(%{|i, o| i.each{|e| o.push e}}, 
		 :prequeuing_policy => {
		   :queuing_class => :OnMemoryQueue, 
		   :threshold => 100})
  for l in f.here
    puts l
  end

when "49.2"
  
  iota = fairy.input(Fairy::Iota, 1000)
  f = iota.smap(%{|i, o| i.each{|e| 
    unless e.kind_of?(Integer)
      p e
    end
    o.push e}}, 
		 :prequeuing_policy => {
#		   :queuing_class => :OnMemoryQueue, 
		   :queuing_class => :FileBufferdQueue, 
		   :threshold => 10})
  f.output("test/test-49.2-output.vf")

$stdin.gets

  for l in fairy.input("test/test-49.2-output.vf").here
    puts l
  end

when "50", "exec"
  f = fairy.exec(["file://localhost/foo/bar", "file://localhost/bar/baz"])
  for l in f.here
    puts l
  end

when "50.1"
  system("ruby-dev", "bin/fairy-cp", "--split", "100", "/etc/passwd", "test/test-50.vf")
#  sleep 10
  f = fairy.exec("test/test-50.vf")
  for l in f.here
    puts l
  end

when "51"
  vf = "test/test-51.vf"
  system("ruby-dev", "bin/fairy-cp", "--split", "100", "/etc/passwd", vf)
  system("ruby-dev", "bin/fairy-rm", vf)


when "51.1"
  vf = "test/test-51.1.vf"
#  system("ruby-dev", "bin/fairy-cp", "--split", "100", "/etc/passwd", vf)
  system("ruby-dev", "bin/fairy-rm", vf)


when "51.2"
  vf = "test/test-51.vf"
  system("echo '#!fairy vfile' > #{vf}")
  system("echo 'file://[::ffff:127.0.0.1]/home/keiju/public/a.research/fairy/git/fairy/test/Repos/emperor2/test/test-51-000' >> #{vf}")
  system("echo 'file://[::ffff:127.0.0.1]/home/keiju/public/a.research/fairy/git/fairy/test/Repos/emperor2/test/test-51-001' >> #{vf}")

#  system("ruby-dev", "bin/fairy-cp", "--split", "100", "/etc/passwd", vf)
  system("ruby-dev", "bin/fairy-rm", vf)


when "52.init", "Bug:#49"
  system("ruby-dev bin/fairy-cp /etc/passwd test/test-52.vf")

when "52.init2"
  system("ruby-dev bin/fairy-cp sample/wc/data/fairy.cat test/test-52.vf")

when "52.1"
#  f = Fairy::Fairy.new
  f = fairy.input("test/test-52.vf")
  f = f.mapf(%{|ln|
    begin
      ln.chomp.split
    rescue
      []
    end})
  f = f.mod_group_by(%{|w| w})
  f = f.map(%q{|key,values| ret=0; values.each{|v| ret+=1}; "#{key}\t#{ret}"})
  f.output("test/test-52-out.vf")
#  sleep 200

when "52.out"
  fairy.input("test/test-52-out.vf").here.each{|l| puts l}

when "52.2"
#  fairy.input("/etc/passwd").output("test/test-52.vf")
  fairy.input("sample/wc/data/fairy.cat").output("test/test-52.vf")
#  sleep 10
  
#  fairy.input("test/test-52.vf").here.each{|l| puts l}

when "52.3"
  fairy.input("sample/wc/data/fairy.cat").here.each{|l| puts l}

when "52.4"
#  fairy.input("/etc/passwd").output("test/test-52.vf")
  fairy.input("sample/wc/data/fairy.cat").output("test/test-52.4")

when "53", "Bug#74"
  f = fairy.input("sample/wc/data/fairy.cat")
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.mod_group_by(%{|w| w})
  f = f.map(%{|key, values| [key, values.size]})
  #  f.here.each{|e| puts e}
  f.output("test/test-53.out.vf")

when "53.init"
  system("bin/fairy", "--home", ".",  
	 "cp", "--split", "72856", "sample/wc/data/fairy.cat", "test/test-53.vf")

when "53.init2"
  system("bin/fairy", "--home", ".",  
	 "cp", "--split", "873813", "sample/wc/data/sample_10M.txt", "test/test-53.vf")

when "53.1"
  f = fairy.input("test/test-53.vf")
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.mod_group_by(%{|w| w})
  f = f.map(%{|key, values| [key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-53.out.vf")

when "53.out"
  fairy.input("test/test-53.out.vf").here.each{|l| puts l}

when "53.ruby"
  h = {}
  File.open("sample/wc/data/sample_10M.txt").each do |line|
    line.chomp.split.each{|w| h[w] = (h[w] || 0) + 1}
  end
  for k, v in h
    print k, v, "\n"
  end

when "54.init"
  fairy.input("sample/wc/data/fairy.cat").output("test/test-54.vf")

when "54.ruby"
  File.open("sample/wc/data/fairy.cat") do |i|
    File.open("/tmp/test", "w") do |o|
      i.each{|s| o.puts s}
    end
  end

when "54.1"
  fairy.input("test/test-54.vf").output("test/test-54-out.vf")

when "54.1.1"
  fairy.input("test/test-54.vf").map(%{|s| s}).output("test/test-54-out.vf")

when "54.1.2"
  fairy.input("test/test-54.vf").map(%{|s| s}).map(%{|s| s}).output("test/test-54-out.vf")

when "54.2"

  matome = 1000

  f = fairy.input("sample/wc/data/fairy.cat")
  f = f.smap(%{|i, o|
    buf = []
    i.each do |s|
      buf.push s
      if buf.size >= #{matome}
	o.push buf
	buf = []
      end
    end
    if buf.size > 0
      o.push buf
    end
  })
  f.output("test/test-54-out.vf")

when "55", "SR#66"

when "55.init"
  system("bin/fairy", "--home", ".",  
	 "cp", "--split", "72857", "sample/wc/data/fairy.cat", "test/test-55.vf")

when "55.init2"
  system("bin/fairy", "--home", ".",  
	 "cp", "--split", "873814", "sample/wc/data/sample_10M.txt", "test/test-55.vf")

when "55.1"
#  f = fairy.input("test/test-55.vf")
#  f = fairy.input(["sample/wc/data/sample_10M.txt"])
  f = fairy.input(["sample/wc/data/sample_30M.txt"])
#  f = fairy.input(["sample/wc/data/sample_30M.txt", 
#		    "sample/wc/data/sample_30M.txt"])

#  f = fairy.input(["sample/wc/data/sample_30M.txt", 
#		    "sample/wc/data/sample_30M.txt", 
#		    "sample/wc/data/sample_30M.txt", 
#		    "sample/wc/data/sample_30M.txt"])
#  f = fairy.input(["sample/wc/data/sample_30M.txt", 
#		    "file://giant/home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"])
#   f = fairy.input(["sample/wc/data/sample_30M.txt", 
#  		    "file://giant/home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt",
#  		    "sample/wc/data/sample_30M.txt",
#  		    "file://giant/home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt",
# "sample/wc/data/sample_30M.txt", 
#  		    "file://giant/home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt",
#  		    "sample/wc/data/sample_30M.txt",
#  		    "file://giant/home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"])
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.mod_group_by(%{|w| w})
  f = f.map(%{|key, values| [key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-55.out.vf")

  puts "FINISH/in SLEEP"
#  sleep 

when "55.1.1"
#  f = fairy.input("test/test-55.vf")
#  f = fairy.input(["sample/wc/data/sample_10M.txt"])
#  f = fairy.input(["sample/wc/data/sample_30M.txt"])
#  f = fairy.input(["sample/wc/data/sample_30M.txt", 
#		    "sample/wc/data/sample_30M.txt"])

#  f = fairy.input(["sample/wc/data/sample_30M.txt", 
#		    "sample/wc/data/sample_30M.txt", 
#		    "sample/wc/data/sample_30M.txt", 
#		    "sample/wc/data/sample_30M.txt"])
#  f = fairy.input(["sample/wc/data/sample_30M.txt", 
#		    "file://giant/home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"])
   f = fairy.input(["sample/wc/data/sample_30M.txt", 
  		    "file://giant/home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt",
  		    "sample/wc/data/sample_30M.txt",
  		    "file://giant/home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt",
 "sample/wc/data/sample_30M.txt", 
  		    "file://giant/home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt",
  		    "sample/wc/data/sample_30M.txt",
  		    "file://giant/home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"])
#   f = f.mapf(%{|ln| begin
#                       ln.chomp.split
# 		    rescue
# 		      []
# 		    end
#   })
  f = f.map(%{|ln| ln.chomp})
#  f = f.map(%{|ln| ln})

#  f = f.mod_group_by(%{|w| w})
#  f = f.map(%{|key, values| [key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-55.1.out.vf")

  puts "FINISH/in SLEEP"
#  sleep 

when "55.1.2"
#  f = fairy.input("test/test-55.vf")
#  f = fairy.input(["sample/wc/data/sample_10M.txt"])
  f = fairy.input(["sample/wc/data/sample_30M.txt"])
#  f = fairy.input(["sample/wc/data/sample_30M.txt", 
#		    "sample/wc/data/sample_30M.txt"])

#  f = fairy.input(["sample/wc/data/sample_30M.txt", 
#		    "sample/wc/data/sample_30M.txt", 
#		    "sample/wc/data/sample_30M.txt", 
#		    "sample/wc/data/sample_30M.txt"])
#  f = fairy.input(["sample/wc/data/sample_30M.txt", 
#		    "file://giant/home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"])
#   f = fairy.input(["sample/wc/data/sample_30M.txt", 
#  		    "file://giant/home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt",
#  		    "sample/wc/data/sample_30M.txt",
#  		    "file://giant/home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt",
# "sample/wc/data/sample_30M.txt", 
#  		    "file://giant/home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt",
#  		    "sample/wc/data/sample_30M.txt",
#  		    "file://giant/home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"])
  f = f.split(1).output("test/test-55.1.2.out.vf")
#  sleep 
  

when "55.1.3"
#  f = fairy.input("test/test-55.vf")
#  f = fairy.input(["sample/wc/data/sample_10M.txt"])
  f = fairy.input(["sample/wc/data/sample_30M.txt"])
#  f = fairy.input(["sample/wc/data/sample_30M.txt", 
#		    "sample/wc/data/sample_30M.txt"])

#  f = fairy.input(["sample/wc/data/sample_30M.txt", 
#		    "sample/wc/data/sample_30M.txt", 
#		    "sample/wc/data/sample_30M.txt", 
#		    "sample/wc/data/sample_30M.txt"])
#  f = fairy.input(["sample/wc/data/sample_30M.txt", 
#		    "file://giant/home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"])
#   f = fairy.input(["sample/wc/data/sample_30M.txt", 
#  		    "file://giant/home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt",
#  		    "sample/wc/data/sample_30M.txt",
#  		    "file://giant/home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt",
# "sample/wc/data/sample_30M.txt", 
#  		    "file://giant/home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt",
#  		    "sample/wc/data/sample_30M.txt",
#  		    "file://giant/home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"])
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.split(1).output("test/test-55.1.3.out.vf")
#  sleep 
  

when "55.1.4"
#  f = fairy.input("test/test-55.vf")
#  f = fairy.input(["sample/wc/data/sample_10M.txt"])
  f = fairy.input(["sample/wc/data/sample_30M.txt"])
#  f = fairy.input(["sample/wc/data/sample_30M.txt", 
#		    "sample/wc/data/sample_30M.txt"])

#  f = fairy.input(["sample/wc/data/sample_30M.txt", 
#		    "sample/wc/data/sample_30M.txt", 
#		    "sample/wc/data/sample_30M.txt", 
#		    "sample/wc/data/sample_30M.txt"])
#  f = fairy.input(["sample/wc/data/sample_30M.txt", 
#		    "file://giant/home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"])
#   f = fairy.input(["sample/wc/data/sample_30M.txt", 
#  		    "file://giant/home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt",
#  		    "sample/wc/data/sample_30M.txt",
#  		    "file://giant/home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt",
# "sample/wc/data/sample_30M.txt", 
#  		    "file://giant/home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt",
#  		    "sample/wc/data/sample_30M.txt",
#  		    "file://giant/home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"])
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.output("test/test-55.1.3.out.vf")
#  sleep 

when "55.1.5"
#  f = fairy.input("test/test-55.vf")
#  f = fairy.input(["sample/wc/data/sample_10M.txt"])
  f = fairy.input(["sample/wc/data/sample_30M.txt"])
#  f = fairy.input(["sample/wc/data/sample_30M.txt", 
#		    "sample/wc/data/sample_30M.txt"])

#  f = fairy.input(["sample/wc/data/sample_30M.txt", 
#		    "sample/wc/data/sample_30M.txt", 
#		    "sample/wc/data/sample_30M.txt", 
#		    "sample/wc/data/sample_30M.txt"])
#  f = fairy.input(["sample/wc/data/sample_30M.txt", 
#		    "file://giant/home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"])
#   f = fairy.input(["sample/wc/data/sample_30M.txt", 
#  		    "file://giant/home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt",
#  		    "sample/wc/data/sample_30M.txt",
#  		    "file://giant/home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt",
# "sample/wc/data/sample_30M.txt", 
#  		    "file://giant/home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt",
#  		    "sample/wc/data/sample_30M.txt",
#  		    "file://giant/home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"])
  f = f.map(%{|ln| begin
                      $L.push ln.chomp.split
                      $L.push ln.chomp.split
                      $L.push ln.chomp.split
                      $L.push ln.chomp.split
                      ln+ln
		    rescue
		      ""
		    end},
	     :BEGIN => %{$L = []}
  )
  f = f.split(1).output("test/test-55.1.3.out.vf")
#  sleep 

  

when "55.2"
  f = fairy.exec("test/test-55.vf")
  f = f.map(%{|uri| File.open(URI(uri).path)}).map(%{|e| e.path})
  for e in f.here
    p e
  end

when "55.3"
  f = fairy.exec("test/test-55.vf")
  f = f.mapf(%{|uri| 
     file =  File.open(URI(uri).path)
     [file, 0, 1, 3]}).map(%{|e| e.class})
  for e in f.here
    p e
  end

when "55.4"
  input_files = ["/etc/passwd", "/etc/group"]
#  input_files = ["/etc/passwd"]

  f1 = fairy.input(input_files).barrier(:mode=>:NODE_CREATION, :cond=>:NODE_ARRIVED, :buffer=>:MEMORY)
  for l in f1.here
    puts l
  end

when "55.4.1"
  input_files = ["/etc/passwd", "/etc/group"]

  f1 = fairy.input(input_files).map(%{|e| e})
  for l in f1.here
    puts l
  end

when "55.5"
  f = fairy.input("test/test-55.vf")
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.mod_group_by(%{|w| w}, :buffering_policy => {:buffering_class => :SimpleCommandSortBuffer})
  f = f.map(%{|key, values| [key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-55.out.vf")

when "55.6"
    require "matrix"

  NoKluster = 2
  Threshold = 0.1

  Data = [[0, 0], [0, 0.5], [1, 1], [1, 0.5]]

  initial_centers = fairy.def_pool_variable(:NoKluster, NoKluster)

  fairy.def_pool_variable(:centers, 
			  :block=>%{require "matrix"
                                    @Pool.NoKluster.times.collect{Vector[rand, rand]}})

  puts "Init Centers:"
  fairy.pool_variable(:centers).each{|e| puts e.inspect}

  measure = 100000

  va = Data.there(fairy).split(2).map(%{|data| Vector[*data]}, 
				      :BEGIN=>%{require "matrix"}).to_va
  
  for l in fairy.input(va).here
    puts l
  end

  
  
#   loop = 0
#   while measure > Threshold
#     puts "ITR: START LOOP: #{loop += 1}"

#     cvpair = fairy.input(va).map(%{|v| [@Pool.centers.min_by{|c| (v - c).r}, v]})
#     gpair = cvpair.group_by(%{|c, v| c})
#     cpair = gpair.emap(%{|i|
#       n = 0
#       new_c = i.inject(Vector[0.0,0.0]){|nc, (c, v)| n += 1; nc += v}*(1.0/n)
#       [[new_c, i.key]]},
# 		       :BEGIN=>%{require "matrix"}).here.to_a
    
#     measure = cpair.inject(0){|m, (n, o)| m += (n - o).r}

#     fairy.pool_variable(:centers, cpair.map{|n, o| n})

#     puts "ITR FINISH:"
#     fairy.pool_variable(:centers).each{|e| puts e.inspect}
#  end

#  sleep 100

when "55.6.1"
    require "matrix"

  NoKluster = 2
  Threshold = 0.1

  Data = [[0, 0], [0, 0.5], [1, 1], [1, 0.5]]

  initial_centers = fairy.def_pool_variable(:NoKluster, NoKluster)

  fairy.def_pool_variable(:centers, 
			  :block=>%{require "matrix"
                                    @Pool.NoKluster.times.collect{Vector[rand, rand]}})

  puts "Init Centers:"
  fairy.pool_variable(:centers).each{|e| puts e.inspect}

  measure = 100000

#  f0 = Data.there(fairy).split(2).map(%{|data| Vector[*data]}, 
#				      :BEGIN=>%{require "matrix"}).here
  f0 = Data.there(fairy).here
  
  for l in f0
    puts l
  end

when "55.6.2"
#  f1 = 100.times.collect{|e| [e, e]}.there(fairy).split(2).split(4).map(%{|i| i})
  f1 = 100.times.collect{|e| [e, e]}.there(fairy)
  for l in f1.here
    puts l
  end

when "55.6.3"
  f1 = 100.times.collect{|e| e}.there(fairy).split(2).split(4).map(%{|i| [i, i]})
  for l in f1.here
    puts l
  end

when "55.7"
  f = fairy.input("test/test-55.vf")
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  }, :prequeuing_policy => {
#		   :queuing_class => :OnMemoryQueue, 
		   :queuing_class => :FileBufferdQueue, 
	       :threshold => 10000})
  f = f.mod_group_by(%{|w| w}, :buffering_policy => {:buffering_class => :SimpleCommandSortBuffer})
  f = f.map(%{|key, values| [key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-55.out.vf")


end

