
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

when "3.1.1", "grep.here"
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
  wc.here.each{|w| puts "word=>count: #{w}"}

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
  
  for l in fairy.input("test/test-6.3-output").here
    puts l
  end

when "6.4", "wc"
  wc = fairy.input("test/test-6.2-input").group_by(%{|w| w.chomp.split(/\s+/)[0]}).smap(%{|i, o| o.push(sprintf("%s=>%d", i.key, i.size))})
#  p wc.here.to_a
  wc.output("test/test-6.4-output")

  for l in fairy.input("test/test-6.4-output").here
    puts l
  end


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

when "19.1", "there"

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


when "22.1", "output varray"

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

when "23.1", "input varray"

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

when "24.1", "k-means"

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

when "25.2", "block"
  
  Data = [[0, 0], [0, 0.5], [1, 1], [1, 0.5]]
  
  here = Data.there(fairy).map(%{|e1| e1}).here
  for l1, l2 in here
    p l1, l2
  end

when "26", "inject"

  iota = fairy.input(Fairy::Iota, 101, :SPLIT_NO=>10)
  inject = iota.inject(%{|sum, value| sum + value})
  p inject.value

when "26.1", "inject"

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

when "26.6", "inject"

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


when "27.1", "terminate"

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

when "30.1", "handle_exeption"
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


when "31.1", "stdout"

  iota = fairy.input(Fairy::Iota, 1001, :SPLIT_NO=>10, :offset=>10)
  minby = iota.min_by(%{|x| p x; -x})
  p minby.value

when "32", "find"

  iota = fairy.input(Fairy::Iota, 1001, :SPLIT_NO=>10, :offset=>10)
  find = iota.find(%{|x| x == 10})
  p find.value

when "32.1", "find"

  iota = fairy.input(Fairy::Iota, 1001, :SPLIT_NO=>10, :offset=>10)
  find = iota.find(%{|x| x == 500})
  p find.value

when "33", "gbreak"
  
  iota = fairy.input(Fairy::Iota, 1001, :SPLIT_NO=>10, :offset=>10)
  here = iota.map(%{|x| if x == 500; gbreak; else x; end}).here
  for l in here
    puts l
  end

  sleep 10

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
  sleep 10
  sleep $sleep if $sleep 


end

