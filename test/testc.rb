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

when "3.3", "seg_map"
  here = fairy.input(["/etc/passwd", "/etc/group"]).seg_map(%{|i,block| i.sort.each{|e|block.call e}}).here
  for l in here
    puts l.inspect
  end
  sleep $sleep if $sleep 

when "3.3a"
  here = fairy.input(["/etc/passwd", "/etc/group"]).seg_map(%{|i,block| i.sort.each{|e|block.call e}}).here

  for l in here
    puts l.inspect
  end

when "3.3.1"
  10000.times do |i|
    puts "LOOP: #{i}"
    fairy.input(["/etc/passwd", "/etc/group"]).seg_map(%{|i,block| i.sort.each{|e|block.call e}}).here.to_a
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
  here = fairy.input(["/etc/passwd", "/etc/group"]).seg_map(%{|i,b| i.sort.each{|e|b.call e}}).map(%{|e| e}).map(%{|e| e}).map(%{|e| e}).map(%{|e| e}).map(%{|e| e}).map(%{|e| e}).map(%{|e| e}).map(%{|e| e}).map(%{|e| e}).map(%{|e| e}).here
  for l in here
    puts l
  end

when "3.4", "njob-monitor"
  require "fairy/share/debug"
  Fairy::Debug::njob_status_monitor_on(fairy)

  here = fairy.input(["/etc/passwd", "/etc/group"]).seg_map(%{|i,b| i.sort.each{|e|b.call e}}).here
  for l in here
    puts l
  end
o
when "3.5"
  puts "nodeの非同期追加のテストはなし"

when "3.6"
  puts "port指定のの非同期追加のテストはなし"

when "4", "basic_group_by"
  here = fairy.input(["sample/wc/data/ruby.txt", "sample/wc/data/ruby.txt"]).basic_group_by(%{|w| w.chomp.split{/\s+/}[0]}).here
  for l in here
    puts l
  end

when "4.0"
  here = fairy.input(["test/test-4-data1"]).basic_group_by(%{|w| w.chomp.split{/\s+/}[0]}).here
  for l in here
    puts l
  end

when "4.1"
  fairy.input(["test/test-4-data1"]).basic_group_by(%{|w| w.chomp.split{/\s+/}[0]}).output("test/test-4-output.vf")


when "4.5"
  wc = fairy.input(["test/test-4-data1", "test/test-4-data2"]).basic_group_by(%{|w| w.chomp.split(/\s+/)[0]}).seg_map(%{|i, block| block.call(sprintf("%s=>%d", i.key, i.size))})
  wc.here.each{|w| puts "word=>count: #{w}"}

  sleep $sleep if $sleep 


when "4.5.1"
  wc = fairy.input(["test/test-4-data1", "test/test-4-data2"]).basic_group_by(%{|w| w.chomp.split(/\s+/)[0]}).seg_map(%{|i, block| block.call([i.key, i.size])})
  wc.here.each{|w, n| puts "word: #{w}, count: #{n}"}

  sleep $sleep if $sleep 

when "4.5.t"
  wc = fairy.input(["test/test-4-data1", "test/test-4-data2"]).basic_group_by(%{|w| w.chomp.split(/\s+/)[0]}).smap(%{|i, o| o.push([i.key, i.size])})
  wc.here.each{|r| r = r.dc_dup; w, n = r[0], r[1]; puts "word: #{w}, count: #{n.inspect}"}


when "4.5.x"
  wc = fairy.input(["test/test-4-data1", "test/test-4-data2"]).basic_group_by(%{|w| w.chomp.split(/\s+/)[0]}).smap(%{|i, o| o.push([i.key, i.size])})
  wc.here.each{|r| w, n = r[0], r[1]; puts "word: #{w}, count: #{n.inspect}"}

when "5", "seg_zip"
  seg_zip = fairy.input(["/etc/passwd"])
  main = fairy.input(["/etc/passwd"]).seg_zip(seg_zip, :ZIP_BY_SEGMENT, %{|e1, e2| e1.chomp+"+"+e2}).here
  for l in main
    puts l
  end

when "5.1", "zip2"
  zip = fairy.input(["/etc/passwd", "/etc/group"])
  main = fairy.input(["/etc/passwd", "/etc/group"]).seg_zip(zip, :ZIP_BY_SEGMENT, %{|e1, e2| e1.chomp+"+"+e2}).here
  for l in main
    puts l
  end


when "5.2", "zip3"
  zip1 = fairy.input(["/etc/passwd", "/etc/group"])
  zip2 = fairy.input(["/etc/passwd", "/etc/group"])
  main = fairy.input(["/etc/passwd", "/etc/group"]).seg_zip(zip1, zip2, :ZIP_BY_SEGMENT, %{|e1, e2, e3| e1.chomp+"+"+e2.chomp+"-"+e3}).here
  for l in main
    puts l
  end

when "6", "output"

#  DeepConnect::MESSAGE_DISPLAY=true

  fairy.input(["file://localhost/etc/passwd", "file://localhost/etc/group"]).output("test/test-output")
  sleep $sleep if $sleep 

when "6.0.1"
  fairy.input(["sample/wc/data/sample_30M.txt"]).output("test/test-output")

when "6.0.2"
  fairy.input(["sample/wc/data/sample_30M.txt"]).map(%{|e| e.chomp.split}).output("test/test-output")

when "6.0.3"
  fairy.input(["sample/wc/data/sample_30M.txt"]).map(%{|e| e.chomp.split}).map(%{|e| e}).output("test/test-output")

when "6.0.4"
  fairy.input(["sample/wc/data/sample_30M.txt"]).map(%{|e| e.chomp.split}).map(%{|e| e}).map(%{|e| e}).output("test/test-output")

when "6.0.5"
  fairy.input(["sample/wc/data/sample_30M.txt"]).map(%{|e| e.chomp.split}).map(%{|e| e}).map(%{|e| e}).map(%{|e| e}).output("test/test-output")

when "6.0.6"
  fairy.input(["sample/wc/data/sample_30M.txt"]).map(%{|e| e.chomp.split}).map(%{|e| e}).map(%{|e| e}).map(%{|e| e}).map(%{|e| e}).output("test/test-output")

when "6.1"

  here = fairy.input("test/test-output").here
  for l in here
    puts l
  end

when "6.2", "gentoo"
  
  fairy.input("test/test-6.2-input").output("test/test-6.2-output")
  sleep $sleep if $sleep 

when "6.3", "wc"
  wc = fairy.input(["test/test-4-data1", "test/test-4-data2"]).basic_group_by(%{|w| w.chomp.split(/\s+/)[0]}).seg_map(%{|i, b| b.call(sprintf("%s=>%d", i.key, i.size))})
#  p wc.here.to_a
  wc.output("test/test-6.3-output")
  
  for l in fairy.input("test/test-6.3-output").here
    puts l
  end

when "6.4"
  wc = fairy.input("test/test-6.2-input").basic_group_by(%{|w| w.chomp.split(/\s+/)[0]}).smap(%{|i, o| o.push(sprintf("%s=>%d", i.key, i.size))})
#  p wc.here.to_a
  wc.output("test/test-6.4-output")

  for l in fairy.input("test/test-6.4-output").here
    puts l
  end


when "6.5"
  wc = fairy.input("test/test-6.2-input").basic_group_by(%{|w| w.chomp.split(/\s+/)[0]}).smap(%{|i, o| o.push(sprintf("%s=>%d", i.key, i.size))})
#  p wc.here.to_a
  wc.output("test/test-6.5-output.vf", :one_file_by_process => true)

  for l in fairy.input("test/test-6.5-output.vf").here
    puts l
  end


when "7", "seg-split"
  fairy.input(["file://localhost/etc/passwd"]).seg_split(4).output("test/test-output.vf")
  sleep 5

when "7.1"
  sp = fairy.input(["file://localhost/etc/passwd"]).seg_split(4).here
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
  lf = fairy.input("/etc/passwd").seg_split(4).output("test/test-8.2-output")

when "9"
  lf = fairy.input("test/test-8.2-output").output("test/test-9.output")

when "9.1"
  lf = fairy.input("test/test-8.2-output").here
  for l in lf
    puts l
  end

when "10"
  lf = fairy.input("/etc/passwd", :split_size=>256).here
#  lf = fairy.input("sample/wc/data/sample_30M.txt", :split_size=>3*1024*1024).here
  for l in lf
    puts l
  end

when "10.1"
  fairy.input("/etc/passwd", :split_size=>256).output("test/test-10.output.vf")

when "10.2"
  lf = fairy.input("/etc/passwd").here
  for l in lf
    puts l
  end


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

when "13", "seg_shuffle"

  input_files = ["/etc/passwd", "/etc/group"]
  f1 = fairy.input(input_files)
  f2 = f1.seg_shuffle(%{|i, o| i.each{|s| o.push s}})
  for l in f2.here
    puts l
  end

when "13.1"

  input_files = ["/etc/passwd", "/etc/group"]
  f1 = fairy.input(input_files)
  f2 = f1.seg_shuffle(%{|i, o| i.to_a.reverse.each{|s| o.push s}})
  for l in f2.here
    puts l
  end

when "13.2"

  input_files = ["/etc/passwd", "/etc/group"]
  f1 = fairy.input(input_files)
  f2 = f1.seg_shuffle(%{|i, o| 
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
  f2 = f1.seg_shuffle(%{|i, o| i.to_a.reverse.each{|s| o.push s}})
  f3 = f2.seg_map(%{|i, block| i.to_a.reverse.each{|e| block.call e}})
  for l in f3.here
    puts l
  end


when "14", "sort"

  input_files = ["/etc/passwd", "/etc/group"]
  
  f1 = fairy.input(input_files).basic_group_by(%{|e| e[0]})
  f2 = f1.seg_map(%{|i, block|
	  ary = i.to_a.sort
	  ary.each{|e| block.call e}})
  for l in f2.here
    puts l
  end


when "14.0"

  input_files = ["/etc/passwd", "/etc/group"]
  
  f1 = fairy.input(input_files).basic_group_by(%{|e| e[0]})
  for l in f1.here
    puts l
  end

when "14.0.1"

  input_files = ["/etc/passwd", "/etc/group"]
  
  f1 = fairy.input(input_files).seg_split(26)
  for l in f1.here
    puts l
  end


when "14.1"

  input_files = ["/etc/passwd", "/etc/group"]

  pv = "l"
  fairy.def_pool_variable(:pv, pv)

  f1 = fairy.input(input_files).basic_group_by(%{|e| e > @Pool.pv})
  f2 = f1.seg_map(%{|i, block|
	  ary = i.to_a.sort
	  ary.each{|e| block.call e}})
  for l in f2.here
    puts l
  end

when "14.2"

  input_files = ["/etc/passwd", "/etc/group"]

  pv = "l"
  fairy.def_pool_variable(:pv, pv)

  f1 = fairy.input(input_files).basic_group_by(%{|e| e <=> @Pool.pv})
  f2 = f1.seg_shuffle(%{|i, o| i.sort{|s1, s2| s1.key <=> s2.key}.each{|s| o.push s}})
  f3 = f2.seg_map(%{|i, block|
	  ary = i.to_a.sort
	  ary.each{|e| block.call e}})
  for l in f3.here
    puts l
  end


when "14.3"

  # NG
  puts "これは動きません. デッドロックします"

  input_files = ["/etc/passwd", "/etc/group"]

  f1 = fairy.input(input_files).basic_group_by(%{|e| e[0]})
  f2 = f1.seg_map(%{|i, block|
	  ary = i.to_a.sort
	  ary.each{|e| block.call e}})
  f3 = f2.seg_shuffle(%{|i, o| i.sort{|s1, s2| s1.key <=> s2.key}.each{|s| o.push s}})
  for l in f3.here
    puts l
  end

when "14.3.1"

  input_files = ["/etc/passwd", "/etc/group"]

  f1 = fairy.input(input_files).basic_group_by(%{|e| e[0]})
  f2 = f1.seg_map(%{|i, block|
	  ary = i.to_a.sort
	  ary.each{|e| block.call e}})
  for l in f2.here
    puts l
  end


when "14.4"

  input_files = ["/etc/passwd", "/etc/group"]

  p = "a"
  pv = []
  26.times{pv.push p; p = p.succ}

  fairy.def_pool_variable(:pv, pv)

  f1 = fairy.input(input_files).basic_group_by(%{|e| @Pool.pv.find(proc{"z"}){|p| e < p}})
  f2 = f1.seg_map(%{|i, block|
	  ary = i.to_a.sort
	  ary.each{|e| block.call e}})
  f3 = f2.seg_shuffle(%{|i, o| i.sort{|s1, s2| s1.key <=> s2.key}.each{|s| o.push s}})
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

  f1 = fairy.input(input_files).basic_group_by(%{|e| e <=> @Pool.pv})
  f2 = f1.barrier(:mode=>:NODE_CREATION, :cond=>:NODE_ARRIVED, :buffer=>:MEMORY)
  f3 = f2.seg_shuffle(%{|i, o| i.sort{|s1, s2| s1.key <=> s2.key}.each{|s| o.push s}})
  f4 = f3.seg_map(%{|i, block|
	  ary = i.to_a.sort
	  ary.each{|e| block.call e}})
  for l in f4.here
    puts l
  end

when "15.1.2"

  # NODE の生成のされ方が気になっている

  input_files = ["/etc/passwd", "/etc/group"]
  f1 = fairy.input(input_files).seg_map(%{|i,block| puts "SLEEPIN"; sleep 5; puts "WAKEUP"; i.each{|e| block.call e}})
  f2 = f1.barrier(:mode=>:NODE_CREATION, :cond=>:NODE_ARRIVED, :buffer=>:MEMORY)
  for l in f2.here
    puts l
  end

when "15.1.2.1"

  # NODE の生成のされ方が気になっている 根本はこちらにあるらしい

  input_files = ["/etc/passwd", "/etc/group"]
  f1 = fairy.input(input_files).seg_map(%{|i,block| puts "SLEEPIN"; sleep 5; puts "WAKEUP"; i.each{|e| block.call e}})
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

  f1 = fairy.input(input_files).basic_group_by(%{|e| e <=> @Pool.pv})
  f2 = f1.barrier(:mode=>:NODE_CREATION, :cond=>:DATA_ARRIVED, :buffer=>:MEMORY)
  f3 = f2.seg_shuffle(%{|i, o| i.sort{|s1, s2| s1.key <=> s2.key}.each{|s| o.push s}})
  f4 = f3.seg_map(%{|i, block|
	  ary = i.to_a.sort
	  ary.each{|e| block.call e}})
  for l in f4.here
    puts l
  end

when "15.2.2"

  input_files = ["/etc/passwd", "/etc/group"]
  f1 = fairy.input(input_files).seg_map(%{|i,block| puts "SLEEPIN"; sleep 5; puts "WAKEUP"; i.each{|e| block.call e}})
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

  f1 = fairy.input(input_files).basic_group_by(%{|e| e <=> @Pool.pv})
  f2 = f1.barrier(:mode=>:NODE_CREATION, :cond=>:ALL_DATA, :buffer=>:MEMORY)
  f3 = f2.seg_shuffle(%{|i, o| i.sort{|s1, s2| s1.key <=> s2.key}.each{|s| o.push s}})
  f4 = f3.seg_map(%{|i, block|
	  ary = i.to_a.sort
	  ary.each{|e|  block.call e}})
  for l in f4.here
    puts l
  end

when "15.3.2"

  puts "これは, 時間がかかります. デッドロックしているわけではありません"
  input_files = ["/etc/passwd", "/etc/group"]
  f1 = fairy.input(input_files).seg_map(%{|i,b| i.each{|e| b.call e; sleep 1}})
  f2 = f1.barrier(:mode=>:NODE_CREATION, :cond=>:ALL_DATA, :buffer=>:MEMORY)
  for l in f2.here
    puts l
  end


when "15.3.2.1"

  puts "これは, 時間がかかります. デッドロックしているわけではありません"
  input_files = ["/etc/passwd", "/etc/group"]
  f1 = fairy.input(input_files).map(%{|e| sleep 1; e})
  f2 = f1.barrier(:mode=>:NODE_CREATION, :cond=>:ALL_DATA, :buffer=>:MEMORY)
  for l in f2.here
    puts l
  end

when "15.3.2.2"

  puts "これは, 時間がかかります. デッドロックしているわけではありません"
  input_files = ["/etc/passwd", "/etc/group"]
  f1 = fairy.input(input_files).map(%{|e| sleep 1; e})
  for l in f1.here
    puts l
  end

when "15.4", "block_cond"

  input_files = ["/etc/passwd", "/etc/group"]

  fairy.def_pool_variable(:mutex, :block=>%{Mutex.new})

  f0 = fairy.input(input_files).seg_map(%{|i,b| @Pool.mutex.synchronize{Log.debug(self, "LOCK"); sleep 5; Log.debug(self, "LOCK OUT")}; b.call 1}).here

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
  f1 = fairy.input(input_files).seg_map(%{|i,b| puts "SLEEPIN"; sleep 5; puts "WAKEUP"; i.each{|e| b.call e}})
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

  iota = fairy.input(Fairy::InputIota, 1000)
  for l in iota.here
    puts l
  end
  sleep $sleep if $sleep 

when "17.1"

  f0 = fairy.input(Fairy::InputIota, 1000)
  f1 = f0.map(%{|e| @sum += e}, :BEGIN=>%{@sum = 0})
  for l in f1.here
    puts l
  end
  sleep $sleep if $sleep 

when "17.2"

  f0 = fairy.input(Fairy::InputIota, 1000)
  f1 = fairy.input(Fairy::InputIota, 1000)
  f2 = f0.seg_zip(f1, :ZIP_BY_SEGMENT, %{|e1, e2| e1+e2})
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

  f1 = fairy.input(input_files).basic_group_by(%{|e| @Pool.pv.find(proc{"z"}){|p| e < p}})
  f2 = f1.emap(%{|i| i.to_a.sort})
  f3 = f2.seg_eshuffle(%{|i| i.sort{|s1, s2| s1.key <=> s2.key}})
  for l in f3.here
    puts l.inspect
  end

when "18.0"
  f = fairy.input(["/etc/passwd", "/etc/group"])
  f = f.emap(%{|i| i.to_a.sort})
  for l in f.here
    puts l.inspect
  end
  
  

when "19", "there"

  f1 = 100.times.collect{|e| e}.there(fairy).seg_split(2).seg_split(4).map(%{|i| i})
  for l in f1.here
    puts l
  end

when "19.1"

  f1 = fairy.there(100.times).seg_split(2).seg_split(4).map(%{|i| i})
  for l in f1.here
    puts l
  end

when "20", "break"

  iota = fairy.input(Fairy::InputIota, 1000)
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

  iota = fairy.input(Fairy::InputIota, 1000)
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

  output_varray = fairy.input(Fairy::InputIota, 1000).output(Fairy::VArray)
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

  va = fairy.input(Fairy::InputIota, 1000).to_va

  for l in fairy.input(va).here
    puts l
  end

when "23.1"

  va = fairy.input(Fairy::InputIota, 1000).to_va
  10.times do |i|
    puts "itr#{i}"
    va = fairy.input(va).map(%{|i| i*2}).to_va
  end
  for l in fairy.input(va).here
    puts l
  end

when "23.2"

  va = fairy.input(Fairy::InputIota, 100).to_va
  puts "va[10]: "
  p va[10]  
  puts "va[20]: "
  p va[20]  

  puts "va[20]=500 "
  va[20]= 500
  p va[20]
  puts "EACH:"
  for l in va
    puts l
  end

when "23.3"

  input_files = ["/etc/passwd", "/etc/group"]
  va = fairy.input(input_files).to_va

  for e in fairy.input(va).here
    p e
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

  va = Data.there(fairy).seg_split(2).map(%{|data| Vector[*data]}, 
				      :BEGIN=>%{require "matrix"}).to_va

  while measure > Threshold
    cvpair = fairy.input(va).map(%{|v|
      [@Pool.centers.min_by{|c| (v - c).r}, v]})
    gpair = cvpair.basic_group_by(%{|cv| cv[0]})
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

  va = Data.there(fairy).seg_split(2).map(%{|data| data = data.dc_deep_copy;Vector[*data]}, 
	 			      :BEGIN=>%{require "matrix"}).to_va

  va.each{|e| puts e.inspect}

  loop = 0
  while measure > Threshold
    puts "ITR: START LOOP: #{loop += 1}"

    cvpair = fairy.input(va).map(%{|v|
      v = v.dc_deep_copy
      [@Pool.centers.min_by{|c| c = c.dc_deep_copy; (v - c).r}, v]})

    puts "ITR: ph#1"
    gpair = cvpair.basic_group_by(%{|c| c[0]})

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

  va = Data.there(fairy).seg_split(2).map(%{|data| Vector[*data]}, 
				      :BEGIN=>%{require "matrix"}).to_va

  loop = 0
  while measure > Threshold
    puts "ITR: START LOOP: #{loop += 1}"

    cvpair = fairy.input(va).map(%{|v|
      [@Pool.centers.min_by{|c| (v - c).r}, v]})
    gpair = cvpair.basic_group_by(%{|cv| cv[0]})
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

  va = Data.there(fairy).seg_split(2).map(%{|data| Vector[*data]}, 
				      :BEGIN=>%{require "matrix"}).to_va

  loop = 0
  while measure > Threshold
    puts "ITR: START LOOP: #{loop += 1}"

    cvpair = fairy.input(va).map(%{|v|
      [@Pool.centers.min_by{|c| (v - c).r}, v]})
    gpair = cvpair.basic_group_by(%{|cv| cv[0]})
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

  va = Data.there(fairy).seg_split(2).map(%{|data| Vector[*data]}, 
				      :BEGIN=>%{require "matrix"}).to_va

  loop = 0
  while measure > Threshold
    puts "ITR: START LOOP: #{loop += 1}"

    cvpair = fairy.input(va).map(%{|v|
      [@Pool.centers.min_by{|c| (v - c).r}, v]})
    gpair = cvpair.basic_group_by(%{|c, v| c})
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

  va = Data.there(fairy).seg_split(2).map(%{|data| Vector[*data]}, 
				      :BEGIN=>%{require "matrix"}).to_va
  loop = 0
  while measure > Threshold
    puts "ITR: START LOOP: #{loop += 1}"

    cvpair = fairy.input(va).map(%{|v| [@Pool.centers.min_by{|c| (v - c).r}, v]})
    gpair = cvpair.basic_group_by(%{|c, v| c})
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

  iota = fairy.input(Fairy::InputIota, 101, :SPLIT_NO=>10)
  inject = iota.inject(%{|sum, value| sum + value})
  p inject.value

when "26.0"

  iota = fairy.input(Fairy::InputIota, 101, :SPLIT_NO=>10)
  inject = iota.inject(%{|sum, value| sum + value})
  p inject.here.to_a

when "26.1"

  iota = fairy.input(Fairy::InputIota, 1001, :SPLIT_NO=>10)
  inject = iota.inject(%{|sum, value| sum + value})
  p inject.value

when "26.2", "min"

  iota = fairy.input(Fairy::InputIota, 1001, :SPLIT_NO=>10, :offset=>10)
  min = iota.min(%{|x, y| -(x<=>y)})
  p min.value

when "26.3", "max"

  iota = fairy.input(Fairy::InputIota, 1001, :SPLIT_NO=>10, :offset=>10)
  max = iota.max
  p max.value

when "26.4", "min_by"

  iota = fairy.input(Fairy::InputIota, 1001, :SPLIT_NO=>10, :offset=>10)
  minby = iota.min_by(%{|x| -x})
  p minby.value

when "26.5", "max_by"

  iota = fairy.input(Fairy::InputIota, 1001, :SPLIT_NO=>10, :offset=>10)
  maxby = iota.max_by(%{|x| x})
  p maxby.value

when "26.6"

  iota = fairy.input(Fairy::InputIota, 1001, :SPLIT_NO=>10)
  inject = iota.inject(%{|sum, value| sum + value})
  for l in inject.here
    p l
  end

when "26.7"

  iota = fairy.input(Fairy::InputIota, 1001, :SPLIT_NO=>10)
  inject = iota.map(%{|e| [e]}).inject(%{|sum, value| sum.concat value})
  for l in inject.here
    p l
  end

when "26.8"

  iota = fairy.input(Fairy::InputIota, 101, :SPLIT_NO=>10)
  inject = iota.inject(%{|sum, value| sum + value}, :init_value => 0)
  p inject.value


when "27", "terminate"

  iota = fairy.input(Fairy::InputIota, 1001, :SPLIT_NO=>10, :offset=>10)
  maxby = iota.max_by(%{|x| x})
  p maxby.value
  # 途中で^C


when "27.1"

  iota = fairy.input(Fairy::InputIota, 1001, :SPLIT_NO=>10, :offset=>10)
  maxby = iota.max_by(%{|x| x})
  p maxby.value
  sleep 100

when "28", "basic_mgroup_by"
  
  iota = fairy.input(Fairy::InputIota, 101, :SPLIT_NO=>10, :offset=>10)
  f3 = iota.basic_mgroup_by(%{|e| [e-1, e, e+1]}).emap(%{|i| [i.to_a]})
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
  va = InitialPositions.there(fairy).seg_split(2).map(%{|p| Vector[*p.to_a]},
						  :BEGIN=>%{require "matrix"}).to_va

puts "X:2"

  fairy.def_pool_variable(:offsets, Offsets.map{|p| Vector[*p.to_a]})
puts "X:3"

  loop = 0
  loop do
    puts "ITR: #{loop+=1}"
    
    f1 = fairy.input(va).basic_mgroup_by(%{|v| @Pool.offsets.collect{|o| v + o}},
		      :BEGIN=>%{require "matrix"})
    va = f1.seg_map(%{|i, b| 
      lives = i.to_a
      if lives.include?(i.key) && (lives.size == 3 or lives.size == 4)
        b.call i.key
      elsif lives.size == 3
        b.call i.key
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

  iota = fairy.input(Fairy::InputIota, 1001, :SPLIT_NO=>10, :offset=>10)
  minby = iota.min_by(%{|x| puts x; -x})
  p minby.value


when "31.1"

  iota = fairy.input(Fairy::InputIota, 1001, :SPLIT_NO=>10, :offset=>10)
  minby = iota.min_by(%{|x| p x; -x})
  p minby.value

when "32", "find"

  iota = fairy.input(Fairy::InputIota, 1001, :SPLIT_NO=>10, :offset=>10)
  find = iota.find(%{|x| x == 10})
  p find.value

when "32.1"

  iota = fairy.input(Fairy::InputIota, 1001, :SPLIT_NO=>10, :offset=>10)
  find = iota.find(%{|x| x == 500})
  p find.value

when "33", "gbreak"
  
  iota = fairy.input(Fairy::InputIota, 10001, :SPLIT_NO=>10, :offset=>10)
  here = iota.map(%{|x| if x == 530; gbreak; else x; end}).here
  for l in here
    puts l
  end

  puts "IN SLEEP"
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
  sleep 2

when "33.2"
  
  iota = fairy.input(Fairy::InputIota, 1001, :SPLIT_NO=>10, :offset=>10)
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

  div = fairy.input(va).basic_group_by(%{|e| 
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
  shuffle = msort.seg_eshuffle(%{|i| i.sort{|s1, s2| s1.key <=> s2.key}})
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

  div = fairy.input(va).basic_group_by(%{|e| 
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
  shuffle = msort.seg_eshuffle(%{|i| i.sort{|s1, s2| s1.key <=> s2.key}})
  puts "RESULT:"
  for l in shuffle.here
    puts l
  end

when "35.0"
  finput = fairy.input("sample/wc/data/fairy.cat")
  fmap = finput.seg_map(%{|i,b|
    i.each{|ln|
      ln.chomp.split.each{|w| b.call(w)}
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
  fshuffle = fmap.basic_group_by(%{|w| w.hash % 5})
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
  fshuffle = fmap.basic_group_by(%{|w| w.hash % 5})
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
  fshuffle = fmap.basic_group_by(%{|w| w.hash % 500})
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
  fshuffle = fmap.basic_group_by(%{|w| w})
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
  fshuffle = fmap.basic_group_by(%{|w| w.hash % 20})
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
  fshuffle = fmap.basic_group_by(%{|w| w.hash % 20})
  freduce = fshuffle.smap(%q{|i,o| 
    words = i.basic_group_by{|w| w}
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
  fshuffle = fmap.basic_group_by(%{|w| w.hash % 20})
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

when "36.0", "group_by"
  finput = fairy.input("sample/wc/data/fairy.cat")
  fmap = finput.seg_map(%{|i,b|
    i.each{|ln|
      ln.chomp.split.each{|w| b.call(w)}
    }
  })
  fshuffle = fmap.group_by(%{|w| w})
  for w in fshuffle.here
    puts w
  end
  sleep 1

when "36.1"
  finput = fairy.input("sample/wc/data/fairy.cat")
  fmap = finput.seg_map(%{|i,o|
    i.each{|ln|
      ln.chomp.split.each{|w| o.call(w)}
    }
  })
  fshuffle = fmap.group_by(%{|w| w})
  freduce = fshuffle.map(%q{|values| "#{values.key}\t#{values.size}"})
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
  fshuffle = fmap.group_by(%{|w| w})
  freduce = fshuffle.map(%q{|values| [values.key, values.size]})
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
  msort = div.seg_map(%{|i, block|
    buf = i.map{|st| [st, st.pop]}.select{|st, v|!v.nil?}.sort_by{|st, v| v}
    while st_min = buf.shift
      st, min = st_min
      block.call min
      next unless v = st.pop
      idx = buf.rindex{|st, vv| vv < v}
      idx ? buf.insert(idx+1, [st, v]) : buf.unshift([st, v])
    end})
  puts "SHUFFLE:" 
  shuffle = msort.seg_eshuffle(%{|i| i.sort{|s1, s2| s1.key <=> s2.key}})
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

  f1 = fairy.input(Fairy::InputIota, 10, :SPLIT_NO=>1)
  f2 = fairy.input(Fairy::InputIota, 20, :SPLIT_NO=>1, :offset=>10)
  f3 = f1.product(f2, %{|e1, e2| e1.to_s+"+"+e2.to_s})
  for l in f3.here
    puts l
  end

when "38.1.1"

  f1 = fairy.input(Fairy::InputIota, 10, :SPLIT_NO=>2)
  f2 = fairy.input(Fairy::InputIota, 10, :SPLIT_NO=>2)
  f3 = f1.product(f2, %{|e1, e2| e1.to_s+"+"+e2.to_s})
  for l in f3.here
    puts l
  end

when "38.1.2"
  f1 = fairy.input(Fairy::InputIota, 10, :SPLIT_NO=>4)
  f2 = fairy.input(Fairy::InputIota, 10, :SPLIT_NO=>4)
  f3 = f1.product(f2, %{|e1, e2| e1.to_s+"+"+e2.to_s})
  for l in f3.here.sort
    puts l
  end

when "38.2"
  f1 = fairy.input(Fairy::InputIota, 10, :SPLIT_NO=>4)
  f2 = fairy.input(Fairy::InputIota, 10, :SPLIT_NO=>4)
  f3 = fairy.input(Fairy::InputIota, 10, :SPLIT_NO=>4)
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
    msort = div.seg_map(%{|i, b|
    buf = i.map{|st| [st, st.pop]}.select{|st, v|!v.nil?}.sort_by{|st, v| v}
    while st_min = buf.shift
      st, min = st_min
      b.call min
      next unless v = st.pop
      idx = buf.rindex{|st, vv| vv < v}
      idx ? buf.insert(idx+1, [st, v]) : buf.unshift([st, v])
    end})
    puts "SHUFFLE:" 
    shuffle = msort.seg_eshuffle(%{|i| i.sort{|s1, s2| s1.key <=> s2.key}})
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

when "40.2"
#  Fairy.def_filter(:grep_keiju) do |fairy, input|
#    input.select(%{|e| /keiju/ =~ e})
#  end

  f0 = fairy.input(["/etc/passwd", "/etc/group"])
  f1 = f0.grep_keiju
  for l in f1.here
    puts l.inspect
  end


when "41", "join"
  join = fairy.input(["/etc/passwd", "/etc/group"])
  main = fairy.input(["/etc/passwd", "/etc/group"]).seg_join(join, %{|in0, in2, out_block| 
    Log::debug(self, "AAAAAAAAAAAAAAAA")
    in0.to_a.zip(in2.to_a).each{|e1, e2| Log::debug(self, "AAAAAAAAAAAAAAAB");out_block.call e1.chomp+"+"+e2.chomp}}).here

  for l in main
    puts l
  end
  sleep 3

# when "41.1"
#   join = fairy.input(["/etc/passwd", "/etc/group"])
#   main = fairy.input(["/etc/passwd", "/etc/group"]).seg_join(join, %{|in0, in2, out_block| 
#     Log::debug(self, "AAAAAAAAAAAAAAAA")
#     ary0 = in0.to_a
#     Log::debug(self, "AAAAAAAAAAAAAAAB")
#     in2.each{|e|
#        Log::debug(self, "AAAAAAAAAAAAAAAB: \#{e.inspect}")
#     }
#    Log::debug(self, "AAAAAAAAAAAAAAABE")
#     ary2 = in2.to_a
#     Log::debug(self, "AAAAAAAAAAAAAAAC")

#     ary0.zip(ary2).each{|e1, e2| Log::debug(self, "AAAAAAAAAAAAAAAD");out_block.call e1.chomp+"+"+e2.chomp}
#     Log::debug(self, "AAAAAAAAAAAAAAAE")
#     }).here

#   for l in main
#     puts l
#   end
  sleep 3

when "42", "equijoin"
when "42.1"
  
#  main = fairy.input(["/etc/group"]).map(%{|e| e.chomp.split(/:/)})
  main = fairy.input(["/etc/passwd"]).map(%{|e| e.chomp.split(/:/)})
  other = fairy.input(["/etc/passwd"]).map(%{|e| e.chomp.split(/:/)})
  count = 0
  for l in main.equijoin(other, 0).here
    count += 1
    puts l.inspect
  end
  puts "COUNT: #{count}"

when "42.2.1"
  main = fairy.input(["/etc/passwd"]).map(%{|e| e.chomp.split(/:/)}).basic_group_by(%{|e| e[0]})
  for *l in main.here
    puts l.inspect
  end

when "42.2.2"
  main = fairy.input(["/etc/passwd"]).map(%{|e| e}).basic_group_by(%{|e| e.split(/:/)[0]})
  for *l in main.here
    puts l.inspect
  end

when "42.2.3"
  main = fairy.input(["/etc/passwd"]).map(%{|e| e.chomp.split(/:/)[0]}).basic_group_by(%{|e| e})
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
  main = fairy.input(["/etc/group"]).map(%{|e| e.chomp.split(/:/)})
#  main = fairy.input("/etc/passwd").map(%{|e| e.chomp.split(/:/)})
  other = fairy.input(["/etc/passwd"]).map(%{|e| e.chomp.split(/:/)})
  count = 0
  for *l in main.equijoin2(other, 0).here
    count += 1
    puts l.inspect
  end
  puts "COUNT: #{count}"

when "43.3"
  Fairy.def_filter(:equijoin3) do |fairy, input, other, *no|
    puts no1 = no2 = no[0]
    puts no2 = no[1] if no[1]

    main = input.map(%{|e| [e[#{no1}], 0, e]})
    other = other.map(%{|e| [e[#{no2}], 1, e]})
  
    main.cat(other).group_by(%{|e| e[0]}).map(%{|values| values})
  end

  main = fairy.input("/etc/passwd").map(%{|e| e.chomp.split(/:/)})
  other = fairy.input("/etc/passwd").map(%{|e| e.chomp.split(/:/)})
  count = 0
  for l in main.equijoin3(other, 0).here
    count += 1
    puts l.inspect
  end
  puts "COUNT: #{count}"

when "43.3.1"
  main = fairy.input("/etc/passwd").map(%{|e| e.chomp.split(/:/)}).group_by(%{|e| e[0]})
  for values in main.here
    puts "key=#{values.key} values=#{values.inspect}"
  end

when "43.3.1.1"
  main = fairy.input("/etc/passwd").mapf(%{|e| e.chomp.split(/:/)}).group_by(%{|e| e})
  for values in main.here
    puts "key=#{values.key} values=#{values.inspect}"
  end

when "43.3.1.2"
  main = fairy.input("/etc/passwd").map(%{|e| e.chomp.split(/:/)}).group_by(%{|e| e[0]}).map(%{|values| [values.key, values.to_a]})
  for key, values in main.here
    puts "key=#{key} values=#{values.inspect}"
  end

when "43.3.1.3"
  main = fairy.input("/etc/passwd").mapf(%{|e| e.chomp.split(/:/)}).group_by(%{|e| e}).map(%{|values| [values.key, values.class]})
  for key, values in main.here
    puts "key=#{key} values=#{values.inspect}"
  end


when "43.3.2"
  main = fairy.input("/etc/passwd").map(%{|e| e.chomp.split(/:/)})
  for e in main.here
    puts e.inspect
  end

when "44", "flatten"
  main = fairy.input("/etc/passwd").mapf(%{|e| e.chomp.split(/:/)})
  for l in main.here
    puts l.inspect
  end
  puts "COUNT: #{count}"

when "44.1"
  main = fairy.input("/etc/passwd").mapf(%{|e| [e.chomp.split(/:/)]}, :N=>2)
  for l in main.here
    puts l.inspect
  end
  puts "COUNT: #{count}"


when "44.2"
  main = fairy.input("/etc/passwd").mapf(%{|e| [e.chomp.split(/:/)]}, :N=>3)
  for l in main.here
    puts l.inspect
  end
  puts "COUNT: #{count}"

when "45", "simple file by key buffer"
  finput = fairy.input(["/etc/passwd"])
  fmap = finput.seg_map(%{|i,b|
    i.each{|ln|
      ln.chomp.split(/:/).each{|w| b.call(w)}
    }
  })
  fshuffle = fmap.group_by(%{|w| w}, :buffering_policy => {:buffering_class => :SimpleFileByKeyBuffer})
#  fshuffle = fmap.group_by(%{|w| w})
  freduce = fshuffle.map(%q{|values| "#{values.key}\t#{values.size}"})
  for w in freduce.here
    puts w
  end

  sleep 2

when "45.0"
  finput = fairy.input("sample/wc/data/fairy.cat")
#  finput = fairy.input(["/etc/passwd"])
  fmap = finput.seg_map(%{|i,b|
    i.each{|ln|
      ln.chomp.split.each{|w| b.call(w)}
#      ln.chomp.split(":").each{|w| o.push(w)}
    }
  })
  fshuffle = fmap.group_by(%{|w| w}, :buffering_policy => {:buffering_class => :OnMemoryBuffer})
#  fshuffle = fmap.group_by(%{|w| w})
  freduce = fshuffle.map(%q{|kvs| "#{kvs.key}\t#{kvs.size}"})
  for w in freduce.here
    puts w
  end

  sleep 2


when "45.1"
  finput = fairy.input("sample/wc/data/fairy.cat")
#  finput = fairy.input(["/etc/passwd"])
  fmap = finput.seg_map(%{|i,b|
    i.each{|ln|

      ln.chomp.split.each{|w| b.call(w)}
#      ln.chomp.split(":").each{|w| o.push(w)}
    }
  })
  fshuffle = fmap.group_by(%{|w| w}, :buffering_policy => {:buffering_class => :SimpleCommandSortBuffer})
#  fshuffle = fmap.group_by(%{|w| w})
  freduce = fshuffle.map(%q{|values| "#{values.key}\t#{values.size}"})
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
  fshuffle = fmap.group_by(%{|w| w}, :buffering_policy => {:buffering_class => :CommandMergeSortBuffer})
#  fshuffle = fmap.group_by(%{|w| w}, :buffering_policy => {:buffering_class => :CommandMergeSortBuffer, :threshold => 100})
#  fshuffle = fmap.group_by(%{|w| w})
  freduce = fshuffle.map(%q{|values| "#{values.key}\t#{values.size}"})
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
#  fshuffle = fmap.group_by(%{|w| w}, :buffering_policy => {:buffering_class => :CommandMergeSortBuffer})
  fshuffle = fmap.group_by(%{|w| w}, :buffering_policy => {:buffering_class => :MergeSortBuffer, :threshold => 100})
#  fshuffle = fmap.group_by(%{|w| w})
  freduce = fshuffle.map(%q{|values| "#{values.key}\t#{values.size}"})
  for w in freduce.here
    puts w
  end

  sleep 2


when "45.4", "Ext Merge Sort Buffer"
  finput = fairy.input("sample/wc/data/fairy.cat")
#  finput = fairy.input(["/etc/passwd"])
  fmap = finput.seg_map(%{|i,b|
    i.each{|ln|
      ln.chomp.split.each{|w| b.call(w)}
#      ln.chomp.split(":").each{|w| b.call(w)}
    }
  })
#  fshuffle = fmap.group_by(%{|w| w}, :buffering_policy => {:buffering_class => :CommandMergeSortBuffer})
  fshuffle = fmap.group_by(%{|w| w}, 
			       :buffering_policy => {
				 :buffering_class => :ExtMergeSortBuffer, 
				 :threshold => 1000})
#  fshuffle = fmap.group_by(%{|w| w})
  freduce = fshuffle.map(%q{|values| "#{values.key}\t#{values.size}"})
  freduce.output("test/test-45.vf")
#   for w in freduce.here
#     puts w
#   end

  sleep 2

when "46", "sort_by"
#  f = fairy.input(["sample/wc/data/fairy.cat"]).sort_by(%{|w| w})
  f = fairy.input(["/etc/passwd", "/etc/group"]).sort_by(%{|w| w})
  for w in f.here
    puts w
  end

when "47.1"
  Fairy.def_filter(:test_sort_by) do |fairy, input, block_source, *opts|
    
    sampling_ratio_1_to = opts[:sampling_ratio]
    sampling_ratio_1_to ||= Fairy::CONF.SORT_SAMPLING_RATIO_1_TO
    pvn = opts[:pvn]
    pvn ||= Fairy::CONF.SORT_NO_SEGMENT
    
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
    
    shuffle = msort.seg_eshuffle(%{|i| i.sort{|s1, s2| s1.key <=> s2.key}})
    #  shuffle = msort.seg_eshuffle(%{|i| i.sort_by{|s1| Log::debug(self, s1.key.inspect); s1.key}})
  end

  f = fairy.input(["/etc/passwd", "/etc/group"]).test_sort_by(%{|w| w})
  for w in f.here
    puts w
  end

when "47.2"
  input = fairy.input(["/etc/passwd", "/etc/group"])

  sampling_ratio_1_to = Fairy::CONF.SORT_SAMPLING_RATIO_1_TO
  pvn = Fairy::CONF.SORT_NO_SEGMENT
    
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
    
  shuffle = msort.seg_eshuffle(%{|i| i.sort{|s1, s2| s1.key <=> s2.key}})
    #  shuffle = msort.seg_eshuffle(%{|i| i.sort_by{|s1| Log::debug(self, s1.key.inspect); s1.key}})

  for w in shuffle.here
    puts w
  end

when "48"

  iota = fairy.input(Fairy::InputIota, 1000)
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

  iota = fairy.input(Fairy::InputIota, 1000)
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
  
  iota = fairy.input(Fairy::InputIota, 1000)
  f = iota.smap(%{|i, o| i.each{|e| o.push e}}, 
		 :prequeuing_policy => {
		   :queuing_class => :FileBufferdQueue, 
		   :threshold => 10})
  for l in f.here
    puts l
  end



when "49.1"
  
  iota = fairy.input(Fairy::InputIota, 1000)
  f = iota.smap(%{|i, o| i.each{|e| o.push e}}, 
		 :prequeuing_policy => {
		   :queuing_class => :OnMemoryQueue, 
		   :threshold => 100})
  for l in f.here
    puts l
  end

when "49.2"
  
  iota = fairy.input(Fairy::InputIota, 1000)
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
  f = fairy.exec(["file://localhost/etc/passwd", "file://localhost/etc/group"])
  for l in f.here
    puts l
  end

when "50.1"
  system("ruby-dev", "bin/fairy", "--home", ".", "cp", "--split", "100", "/etc/passwd", "test/test-50.vf")
  puts "SLEEP IN"
  sleep 10
  puts "WAKE UP"
  f = fairy.exec("test/test-50.vf")
  for l in f.here
    puts l
  end

when "51"
  vf = "test/test-51.vf"
#  system("ruby-dev", "bin/fairy-cp", "--split", "100", "/etc/passwd", vf)
  system("ruby-dev", "bin/fairy-rm", vf)


when "51.1"
  vf = "test/test-51.1.vf"
  system("ruby-dev", "bin/fairy-cp", "--split", "100", "/etc/passwd", vf)
  system("ruby-dev", "bin/fairy-rm", vf)


when "51.2"
  vf = "test/test-51.vf"
  system("echo '#!fairy vfile' > #{vf}")
  system("echo 'file://127.0.0.1/home/keiju/public/a.research/fairy/git/fairy/test/Repos/emperor2/test/test-51-000' >> #{vf}")
  system("echo 'file://127.0.0.1/home/keiju/public/a.research/fairy/git/fairy/test/Repos/emperor2/test/test-51-001' >> #{vf}")

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
  f = f.group_by(%{|w| w})
  f = f.map(%q{|values| ret=0; values.each{|v| ret+=1}; "#{values.key}\t#{ret}"})
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
  f = f.group_by(%{|w| w})
  f = f.map(%{|values| [values.key, values.size]})
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
  f = f.group_by(%{|w| w})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
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
  f = f.group_by(%{|w| w})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-55.out.vf")

  puts "FINISH/in SLEEP"
  sleep 

when "55.1.1"
#  f = fairy.input("test/test-55.vf")
#  f = fairy.input(["sample/wc/data/sample_10M.txt"])
#  f = fairy.input("sample/wc/data/sample_30M.txt")
  f = fairy.input(["sample/wc/data/sample_30M.txt"])
#  f = fairy.input(["sample/wc/data/sample_30M.txt", 
#		    "sample/wc/data/sample_30M.txt"])

#  f = fairy.input(["sample/wc/data/sample_30M.txt", 
#		    "sample/wc/data/sample_30M.txt", 
#		    "sample/wc/data/sample_30M.txt", 
#		    "sample/wc/data/sample_30M.txt"])
#  f = fairy.input(["sample/wc/data/sample_30M.txt", 
#		    "file://giant/home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"])
#    f = fairy.input(["sample/wc/data/sample_30M.txt", 
#   		    "file://giant/home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt",
#   		    "sample/wc/data/sample_30M.txt",
#   		    "file://giant/home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt",
#  "sample/wc/data/sample_30M.txt", 
#   		    "file://giant/home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt",
#   		    "sample/wc/data/sample_30M.txt",
#   		    "file://giant/home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"])
#   f = f.mapf(%{|ln| begin
#                       ln.chomp.split
# 		    rescue
# 		      []
# 		    end
#   })
  f = f.map(%{|ln| ln.chomp})
#  f = f.map(%{|ln| ln})

#  f = f.group_by(%{|w| w})
#  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-55.1.out.vf")

#  puts "FINISH/in SLEEP"
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
  f = f.seg_split(1).output("test/test-55.1.2.out.vf")
  sleep 
  

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
  f = f.seg_split(1).output("test/test-55.1.3.out.vf")
  sleep 
  

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
  f = f.map(%{|w| w}).map(%{|w| w}).output("test/test-55.1.3.out.vf")

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
  f = f.seg_split(1).output("test/test-55.1.3.out.vf")
#  sleep 

  

when "55.2"
  f = fairy.exec(["/etc/passwd", "/etc/group"])
  f = f.map(%{|uri| File.open(URI(uri).path)}).map(%{|e| e.path})
  for e in f.here
    p e
  end

when "55.3"
  f = fairy.exec(["/etc/passwd", "/etc/group"])
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
  f = f.group_by(%{|w| w}, :buffering_policy => {:buffering_class => :SimpleCommandSortBuffer})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
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

  va = Data.there(fairy).seg_split(2).map(%{|data| Vector[*data]}, 
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

#  f0 = Data.there(fairy).seg_split(2).map(%{|data| Vector[*data]}, 
#				      :BEGIN=>%{require "matrix"}).here
  f0 = Data.there(fairy).here
  
  for l in f0
    puts l
  end

when "55.6.2"
#  f1 = 100.times.collect{|e| [e, e]}.there(fairy).seg_split(2).seg_split(4).map(%{|i| i})
  f1 = 100.times.collect{|e| [e, e]}.there(fairy)
  for l in f1.here
    puts l
  end

when "55.6.3"
  f1 = 100.times.collect{|e| e}.there(fairy).seg_split(2).seg_split(4).map(%{|i| [i, i]})
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
  f = f.group_by(%{|w| w}, :buffering_policy => {:buffering_class => :SimpleCommandSortBuffer})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-55.out.vf")

when "56", "cat"

  system("bin/fairy", "--home", ".",
	 "cat", "test/test-56.vf")


when "56.init"
  system("bin/fairy", "--home", ".",  
	 "cp", "--split", "72856", "sample/wc/data/fairy.cat", "test/test-56.vf")

when "56.1"
  system("bin/fairy", "--home", ".",
	 "cp", "test/test-56.vf", "/tmp/zz")

when "57", "SR80"  
  system("bin/fairy", "--home", ".",  
	 "cp", "--split", "72856", "sample/wc/data/fairy.cat", "test/test-56.vf")
  
when "57.1"
  system("bin/fairy", "--home", ".",
	 "cp", "test/test-56.vf", "/tmp/zz")

when "58"
  f = fairy.input(["test/Repos/emperor2/test/test-58-euc-jp"])
  h = f.map(%{|ln| ln.chomp.split}).here
  for ln in h
    $stdout.puts ln
  end

when "58.1"
  f = fairy.input(["test/Repos/emperor2/test/test-58-euc-jp"])
  h = f.map(%{|ln| ln.chomp.split}).output("test/test-58-out.vf")

when "59"
  fairy.input(["sample/wc/data/sample_30M.txt"]).output("test/test-output")

when "59.0"
  fairy.input(["sample/wc/data/sample_30M.txt", "sample/wc/data/sample_30M.txt"]).output("test/test-output")

when "59.1.1"
  fairy.input(["sample/wc/data/sample_30M.txt"]).map(%{|e| e.chomp.split}).output("test/test-output")

when "59.1.1.0"
  fairy.input(["sample/wc/data/sample_30M.txt", "sample/wc/data/sample_30M.txt"]).map(%{|e| e.chomp.split}).output("test/test-output")

when "59.1.2"
  fairy.input(["sample/wc/data/sample_30M.txt"]).map(%{|e| e.chomp.split}).map(%{|e| e}).output("test/test-output")

when "59.1.3"
  fairy.input(["sample/wc/data/sample_30M.txt"]).map(%{|e| e.chomp.split}).map(%{|e| e}).map(%{|e| e}).output("test/test-output")

when "59.1.4"
  fairy.input(["sample/wc/data/sample_30M.txt"]).map(%{|e| e.chomp.split}).map(%{|e| e}).map(%{|e| e}).map(%{|e| e}).output("test/test-output")

when "59.1.5"
  fairy.input(["sample/wc/data/sample_30M.txt"]).map(%{|e| e.chomp.split}).map(%{|e| e}).map(%{|e| e}).map(%{|e| e}).map(%{|e| e}).output("test/test-output")


when "59.2"
  fairy.input(["sample/wc/data/sample_30M.txt"],
	      :postmapping_policy => :MPNewProcessor).output("test/test-output")

when "59.2.0.0"
  fairy.input(["sample/wc/data/sample_30M.txt", "sample/wc/data/sample_30M.txt"],
	      :postmapping_policy => :MPNewProcessor).output("test/test-output")

when "59.2.0"
  fairy.input(["sample/wc/data/sample_30M.txt"]).map(%{|e| e.chomp.split},
						     :postmapping_policy => :MPNewProcessor).output("test/test-output")

when "59.2.0.0.0"
  fairy.input(["sample/wc/data/sample_30M.txt", "sample/wc/data/sample_30M.txt"]).map(%{|e| e.chomp.split},
						     :postmapping_policy => :MPNewProcessor).output("test/test-output")

when "59.2.0.1"
  fairy.input(["sample/wc/data/sample_30M.txt"]).map(%{|e| e.chomp.split;e},
						     :postmapping_policy => :MPNewProcessor).output("test/test-output")


when "59.2.1"
  fairy.input(["sample/wc/data/sample_30M.txt"]).map(%{|e| e.chomp.split}).map(%{|e| e}, :postmapping_policy => :MPNewProcessor).output("test/test-output")

when "59.2.2"
  fairy.input(["sample/wc/data/sample_30M.txt"]).map(%{|e| e.chomp.split}).map(%{|e| e}).map(%{|e| e}, :postmapping_policy => :MPNewProcessor).output("test/test-output")


when "59.2.3"
  fairy.input(["sample/wc/data/sample_30M.txt"]).map(%{|e| e.chomp.split}).map(%{|e| e}).map(%{|e| e}).map(%{|e| e}, :postmapping_policy => :MPNewProcessor).output("test/test-output")

when "59.2.4"
  fairy.input(["sample/wc/data/sample_30M.txt"]).map(%{|e| e.chomp.split}).map(%{|e| e}).map(%{|e| e}).map(%{|e| e}).map(%{|e| e}, :postmapping_policy => :MPNewProcessor).output("test/test-output")

when "59.3"
  fairy.input(["sample/wc/data/sample_30M.txt"]).split(1).output("test/test-output")


when "59.3.1"
  fairy.input(["sample/wc/data/sample_30M.txt"]).mapf(%{|e| e.chomp.split}).split(5).output("test/test-output.vf")

when "59.3.2"
  f = fairy.input(["sample/wc/data/sample_30M.txt"]).mapf(%{|e| e.chomp.split}).split(1)
  f = f.seg_map(%{|i, block| i.to_a.each{|e| block.call e}})
  f.output("test/test-output")

when "59.4"
  fairy.input(["sample/wc/data/sample_30M.txt"]).mapf(%{|e| e.chomp.split}).output("test/test-output")


when "59.5"
  fairy.input(["sample/wc/data/sample_30M.txt"]).mapf(%{|e| e.chomp.split}).basic_group_by(%{|w| /[[:alpha:]]/ =~ w[0] ? w[0].upcase : /[[:digit:]]/ =~ w[0] ? "n" : "z" }).output("test/test-output")

#  sleep 1000

when "59.6"
  f = fairy.input(["sample/wc/data/sample_30M.txt"])
#  f = fairy.input(["sample/wc/data/fairy.cat"])
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-output")

when "59.6.1"
  f = fairy.input(["sample/wc/data/sample_30M.txt"])
#  f = fairy.input(["sample/wc/data/fairy.cat"])
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
ppp  })
  f = f.basic_group_by(%{|w| w.ord % 5})
  f = f.seg_map(%{|i, block| i.group_by{|w| w}.each{|key, value| block.call [key, value]}})
  f = f.map(%{|key, values| [key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-output")

when "59.6.2"
#  f = fairy.input(["sample/wc/data/sample_30M.txt", "sample/wc/data/sample_30M.txt"])
  f = fairy.input(["sample/wc/data/sample_30M.txt", 
		    "sample/wc/data/sample_30M.txt", 
		    "sample/wc/data/sample_30M.txt"])
o#  f = fairy.input(["sample/wc/data/fairy.cat", "sample/wc/data/fairy.cat"])
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-output")

when "59.E"
  f = fairy.input("test/test-output")
  f.here.each{|l| puts l}

when "59.3"
  fairy.input(["sample/wc/data/sample_30M.txt"]).seg_split(1).output("test/test-output")


when "59.3.1"
  fairy.input(["sample/wc/data/sample_30M.txt"]).mapf(%{|e| e.chomp.split}).seg_split(5).output("test/test-output.vf")

when "59.4"
  fairy.input(["sample/wc/data/sample_30M.txt"]).mapf(%{|e| e.chomp.split}).output("test/test-output")


when "59.5"
  fairy.input(["sample/wc/data/sample_30M.txt"]).mapf(%{|e| e.chomp.split}).basic_group_by(%{|w| /[[:alpha:]]/ =~ w[0] ? w[0].upcase : /[[:digit:]]/ =~ w[0] ? "n" : "z" }).output("test/test-output")

#  sleep 1000

when "59.6"
  f = fairy.input(["sample/wc/data/sample_30M.txt"])
#  f = fairy.input(["sample/wc/data/fairy.cat"])
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-output")

when "60", "BUG#135"
#   f = fairy.input(["sample/wc/data/sample_30M.txt", 
# 		    "sample/wc/data/sample_30M.txt", 
# 		    "sample/wc/data/sample_30M.txt", 
# 		    "sample/wc/data/sample_30M.txt", 
# 		    "sample/wc/data/sample_30M.txt", 
# 		    "sample/wc/data/sample_30M.txt", 
# 		    "sample/wc/data/sample_30M.txt"])
  f = fairy.input(["sample/wc/data/sample_120M.txt", 
		    "sample/wc/data/sample_120M.txt", 
		    "sample/wc/data/sample_120M.txt"])
  f.output("test/test-60-output")

when "61", "BUG#136"
  f = fairy.input(["sample/wc/data/sample_30M.txt"])
#  f = fairy.input(["sample/wc/data/fairy.cat"])
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-output")


when "61.1"
  f = fairy.input(["sample/wc/data/sample_30M.txt"])
#  f = fairy.input(["sample/wc/data/fairy.cat"])
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w}, :buffering_policy => {:buffering_class => :MergeSortBuffer})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-output")


when "61.2"
  f = fairy.input(["sample/wc/data/sample_30M.txt"])
#  f = fairy.input(["sample/wc/data/fairy.cat"])
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w}, 
		     :buffering_policy => {:buffering_class => :MergeSortBuffer})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-output")

when "62", "BUG#150"
  
  f = fairy.input(["sample/wc/data/fairy.cat",
		  "sample/wc/data/fairy.cat",
		  "sample/wc/data/fairy.cat"])
  f1 = f.map(%{|e| e}, :postmapping_policy => :MPSameProcessorQ)
  for l in f1.here
    puts l
  end

when "62.1"
  
  f = fairy.input(["file://gentoo//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat",
		  "file://gentoo//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat",
		    "sample/wc/data/fairy.cat",
		  "sample/wc/data/fairy.cat",
		  ])

  f1 = f.seg_map(%{|i, b| i.each{}; b.call "end"}, :postmapping_policy => :MPSameProcessorQ)
  for l in f1.here
    puts l
  end

when "62.2"
  
  
  f = fairy.exec([
		   "sample/wc/data/fairy.cat",
		   "sample/wc/data/fairy.cat",
"file://gentoo//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat",
		  "file://gentoo//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat",
		  ])

  f1 = f.map(%{|e| e})
  for l in f1.here
    puts l
  end

  f = fairy.exec([
		   "sample/wc/data/fairy.cat",
		   "sample/wc/data/fairy.cat",
"file://gentoo//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat",
		  "file://gentoo//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat",
		  ])

  f1 = f.map(%{|e| e}, :postmapping_policy => :MPSameProcessorQ)
  for l in f1.here
    puts l
  end

when "63", "new here", "REQ#139"
  f = fairy.input(["sample/wc/data/sample_30M.txt", 
		    "sample/wc/data/sample_30M.txt", 
		    "sample/wc/data/sample_30M.txt"])
  for l in f.here;end

when "64", "BUG#153"

  require 'timeout'

  10000.times{|i|
    res = "0" 
    t0 = Time.now
    begin
      timeout(30){
	fork { fairy = Fairy::Fairy.new; exit }
	Process.wait
      }
      res = "1" 
    rescue => e
      nil
    end

    t1 = Time.now
    puts [i+1, res, t1-t0].join(",")

    sleep 3
  }

when "65", "REQ#161"
  here = fairy.input(["/etc/passwd", "/etc/group"]).here
  for l in here
    puts l
  end
  sleep

when "66", "REQ#162"
  here = fairy.input(["/etc/passwd", "/etc/group"]).here
  for l in here
    puts l
  end
  sleep


when "66.1"
  here = fairy.input(["/etc/passwd", "/etc/group"]).output("test-66.vf")

  sleep

when "66.2"
  here = fairy.input(["sample/wc/data/sample_30M.txt",
		       "sample/wc/data/sample_30M.txt",
		       "sample/wc/data/sample_30M.txt",
		       "sample/wc/data/sample_30M.txt",
		       "sample/wc/data/sample_30M.txt",
		       "sample/wc/data/sample_30M.txt",
		       "sample/wc/data/sample_30M.txt",
		       "sample/wc/data/sample_30M.txt"]).seg_map(%{|i, b| sleep 10; i.each{|e| b.call e}}).output("test/test-66.vf")


when "66.3"
  iota = fairy.times(100000000, :SPLIT_NO=>10).output("test-66.vf")

when "66.4"
#   f = fairy.input(["sample/wc/data/sample_30M.txt", 
# 		    "sample/wc/data/sample_30M.txt", 
# 		    "sample/wc/data/sample_30M.txt", 
# 		    "sample/wc/data/sample_30M.txt", 
# 		    "sample/wc/data/sample_30M.txt", 
# 		    "sample/wc/data/sample_30M.txt", 
# 		    "sample/wc/data/sample_30M.txt", 
# 		    "sample/wc/data/sample_30M.txt", 
# 		    "sample/wc/data/sample_30M.txt"])

  f = fairy.input(["sample/wc/data/sample_10M.txt", 
		    "sample/wc/data/sample_10M.txt", 
		    "sample/wc/data/sample_10M.txt", 
		    "sample/wc/data/sample_10M.txt", 
		    "sample/wc/data/sample_10M.txt", 
		    "sample/wc/data/sample_10M.txt", 
		    "sample/wc/data/sample_10M.txt", 
		    "sample/wc/data/sample_10M.txt", 
		    "sample/wc/data/sample_10M.txt"])
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-66.vf")


when "66.5"
#  f = fairy.input(["sample/wc/data/sample_30M.txt"]*120)
  f = fairy.input(["sample/wc/data/sample_30M.txt"]*10)
#  f = fairy.input(["sample/wc/data/sample_10M.txt"]*30)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-66.vf")

when "67"
#  file = "sample/wc/data/fairy.cat"
  file = "/etc/passwd"
  f = fairy.input([file], :postmapping_policy => :MPNewProcessor, :postqueuing_policy=>{:queuing_class => :SortedQueue, :sort_by => %q{|l| v = ""; begin; v = l; rescue; end; v}}).here 
  for l in f
    puts l
  end

when "67.0"
  f = fairy.input(["/etc/passwd"], :postmapping_policy => :MPNewProcessor).here
  for l in f
    puts l
  end

when "67.0.1"
  f = fairy.input(["/etc/passwd"]).here
  for l in f
    puts l
  end

when "67.0.2"
  f = fairy.input(["/etc/passwd"], :postmapping_policy => :MPSameProcessorQ).here
  for l in f
    puts l
  end

when "67.1"
  f = fairy.input(["sample/wc/data/sample_30M.txt"], :postmapping_policy => :MPNewProcessor, :postqueuing_policy=>{:queuing_class => :SortedQueue, :sort_by => "{|l| l.split(/:/)}"}).output("test/test-67.vf")

when "68", "group_by2"
  input = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*1)
#  input = fairy.input(["sample/wc/data/fairy.cat"])
#  input = fairy.input(["/etc/passwd"])
  here = input.group_by2(%{|w| w[0]}).here
  for l in here
    puts l
  end

when "68.1"
  block_source = %{|w| w[0]}
#  input = fairy.input(["/etc/passwd"])
  input = fairy.input(["sample/wc/data/fairy.cat"])
  pre = input.merge_group_by(%{|e| proc{#{block_source}}.call(e).ord % 10}, 
			     :postqueuing_policy => {
			       :queuing_class => :SortedQueue, 
			       :sort_by => block_source
			     })
  post = pre.seg_map(%{|st, block| st.each{|i| while e = i.pop; block.call e; end}})
  for l in post.here
    puts l
  end

when "68.1.0"
  block_source = %{|w| w.chomp.split{/:/}[0]}
#  input = fairy.input(["/etc/passwd"])
  input = fairy.input(["sample/wc/data/fairy.cat"])
  pre = input.merge_group_by(%{|e| e.ord % 10})
  post = pre.seg_map(%{|st, block| st.each{|i| p 1; while e = i.pop; p e; block.call e; end}})
  for l in post.here
    puts l
  end

  sleep 10

when "68.2"
  input = fairy.input(["sample/wc/data/fairy.cat"])
#  input = fairy.input(["/etc/passwd"])
  f = input.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  here = f.group_by2(%{|w| w}).map(%{|values| [values.key, values.size].join(" ")}).here

  for l in here
    puts l
  end

when "68.3"
#  f = fairy.input(["sample/wc/data/sample_30M.txt"]*120)
#  f = fairy.input(["sample/wc/data/sample_30M.txt"]*10)
  f = fairy.input(["sample/wc/data/sample_10M.txt"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by2(%{|w| w})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-66.vf")

when "68.4"
#  f = fairy.input(["sample/wc/data/sample_30M.txt"]*120)
#  f = fairy.input(["sample/wc/data/sample_30M.txt"]*30)
  f = fairy.input(["sample/wc/data/sample_10M.txt"]*10)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by3(%{|w| w})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-66.vf")

when "69.1.0"
#  f = fairy.input(["sample/wc/data/sample_30M.txt"]*120)
  f = fairy.input(["sample/wc/data/sample_30M.txt"]*1)
#  f = fairy.input(["sample/wc/data/sample_10M.txt"]*10)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f.output("test/test-66.vf")


when "69.1.1"
#  f = fairy.input(["sample/wc/data/sample_30M.txt"]*120)
#  f = fairy.input(["sample/wc/data/sample_30M.txt"]*120)
  f = fairy.input(["sample/wc/data/sample_10M.txt"]*10)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
             },
	     :postmapping_policy => :MPSameProcessorQ)
  f.output("test/test-66.vf")

when "69.1.1.1"
#  f = fairy.input(["sample/wc/data/sample_30M.txt"]*120)
#  f = fairy.input(["sample/wc/data/sample_30M.txt"]*60)
  f = fairy.input(["sample/wc/data/sample_10M.txt"]*10)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
             },
	     :postmapping_policy => :MPSameProcessorQ,
	     :postqueuing_policy => {:queuing_class => :ChunkedPoolQueue})
  f.output("test/test-66.vf")

when "69.1.1.2"
#  f = fairy.input(["sample/wc/data/sample_30M.txt"]*120)
#  f = fairy.input(["sample/wc/data/sample_30M.txt"]*60)
  f = fairy.input(["sample/wc/data/sample_10M.txt"]*10)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
             },
	     :postmapping_policy => :MPSameProcessorQ,
	     :postqueuing_policy => {:queuing_class => :OnMemoryQueue})
  f.output("test/test-66.vf")

when "69.1.1.3"
#  f = fairy.input(["sample/wc/data/sample_30M.txt"]*120)
  f = fairy.input(["sample/wc/data/sample_30M.txt"]*1)
#  f = fairy.input(["sample/wc/data/sample_10M.txt"]*10)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
             },
	     :postmapping_policy => :MPSameProcessorQ,
	     :postqueuing_policy => {:queuing_class => :ChunkedPoolQueue})
  f.output("test/test-66.vf",
	   :prequeuing_policy => {:queuing_class => :ChunkedPoolQueue})


when "69.1.2"
#  f = fairy.input(["sample/wc/data/sample_30M.txt"]*120)
#  f = fairy.input(["sample/wc/data/sample_30M.txt"]*120)
  f = fairy.input(["sample/wc/data/sample_10M.txt"]*10)
#  f = fairy.input(["sample/wc/data/fairy.cat"]*960)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
             },
	     :postmapping_policy => :MPNewProcessorN)
  f.output("test/test-66.vf")

when "69.1.2.1"
#  F = FAIRY.INPUT(["SAMPLE/WC/DATA/SAMPLE_30M.txt"]*120)
  f = fairy.input(["sample/wc/data/sample_30M.txt"]*10)
#  f = fairy.input(["sample/wc/data/sample_10M.txt"]*10)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
             },
	     :postmapping_policy => :MPNewProcessorN,
	     :postqueuing_policy => {:queuing_class => :ChunkedPoolQueue})
  f.output("test/test-66.vf")

when "69.1.2.2"
#  F = FAIRY.INPUT(["SAMPLE/WC/DATA/SAMPLE_30M.txt"]*120)
#  f = fairy.input(["sample/wc/data/sample_30M.txt"]*2)
  f = fairy.input(["sample/wc/data/sample_10M.txt"]*10)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
             },
	     :postmapping_policy => :MPNewProcessorN,
	     :postqueuing_policy => {:queuing_class => :ChunkedPoolQueue})
  f.output("test/test-66.vf",
	   :prequeuing_policy => {:queuing_class => :ChunkedPoolQueue})

when "69.1.3.1"
#  F = FAIRY.INPUT(["SAMPLE/WC/DATA/SAMPLE_30M.txt"]*120)
  f = fairy.input(["sample/wc/data/sample_30M.txt"]*10)
#  f = fairy.input(["sample/wc/data/sample_10M.txt"]*10)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
             },
	     :postmapping_policy => :MPSameProcessorQ,
	     :postqueuing_policy => {:queuing_class => :ChunkedPoolQueue})
  f.output("test/test-66.vf",
	   :prequeuing_policy => {:queuing_class => :ChunkedSizedPoolQueue})

when "69.1.3.2"
#  F = FAIRY.INPUT(["SAMPLE/WC/DATA/SAMPLE_30M.txt"]*120)
  f = fairy.input(["sample/wc/data/sample_30M.txt"]*10)
#  f = fairy.input(["sample/wc/data/sample_10M.txt"]*10)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
             },
	     :postmapping_policy => :MPNewProcessorN,
	     :postqueuing_policy => {:queuing_class => :ChunkedPoolQueue})
  f.output("test/test-66.vf",
	   :prequeuing_policy => {:queuing_class => :ChunkedSizedPoolQueue})

when "69.1.3.3"
#  F = FAIRY.INPUT(["SAMPLE/WC/DATA/SAMPLE_30M.txt"]*120)
#  f = fairy.input(["sample/wc/data/sample_30M.txt"]*10)
  f = fairy.input(["sample/wc/data/sample_50M.txt"]*2)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
             },
	     :postmapping_policy => :MPNewProcessorN,
	     :postqueuing_policy => {:queuing_class => :ChunkedSizedPoolQueue})
  f.output("test/test-66.vf",
	   :prequeuing_policy => {:queuing_class => :ChunkedSizedPoolQueue})

when "69.2.0"
#  f = fairy.input(["sample/wc/data/sample_30M.txt"]*120)
#  f = fairy.input(["sample/wc/data/sample_30M.txt"]*120)
#  f = fairy.input(["sample/wc/data/sample_30M.txt"]*10)
  f = fairy.input(["sample/wc/data/sample_10M.txt"]*2)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-66.vf")

when "69.2.1"
#  f = fairy.input(["sample/wc/data/sample_30M.txt"]*120)
#  f = fairy.input(["sample/wc/data/sample_30M.txt"]*4)
#  f = fairy.input(["sample/wc/data/sample_10M.txt"]*10)
  f = fairy.input(["sample/wc/data/sample_30M.txt"]*60)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :postqueuing_policy => {:queuing_class => :ChunkedPoolQueue})
  f = f.map(%{|values| [values.key.inspect, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-66.vf")

when "69.2.2"
#  f = fairy.input(["sample/wc/data/sample_30M.txt"]*30)
  f = fairy.input(["sample/wc/data/sample_10M.txt"]*10)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :buffering_policy => {:buffering_class => :OnMemoryBuffer})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-66.vf")

when "69.2.3"
#  f = fairy.input(["sample/wc/data/sample_30M.txt"]*30)
  f = fairy.input(["sample/wc/data/sample_10M.txt"]*10)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :buffering_policy => {:buffering_class => :OnMemoryBuffer},
		     :postqueuing_policy => {:queuing_class => :ChunkedPoolQueue})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-66.vf")

when "69.2.4"
#  f = fairy.input(["sample/wc/data/sample_30M.txt"]*120)
#  f = fairy.input(["sample/wc/data/sample_30M.txt"]*4)
#  f = fairy.input(["sample/wc/data/sample_10M.txt"]*10)
#  f = fairy.input(["sample/wc/data/sample_10M.txt", "file://giant//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*60)
#  f = fairy.input(["sample/wc/data/sample_30M.txt"]*120)
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"]*10)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*10)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_50M.txt"]*2)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_120M.txt"]*2)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
#  f = fairy.input(["file://giant//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
#  f = fairy.input(["file://giant//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :postqueuing_policy => {:queuing_class => :ChunkedFileBufferdQueue})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-66.vf")

when "69.2.4.1"
#  f = fairy.input(["sample/wc/data/sample_30M.txt"]*120)
#  f = fairy.input(["sample/wc/data/sample_30M.txt"]*4)
#  f = fairy.input(["sample/wc/data/sample_10M.txt"]*10)
#  f = fairy.input(["sample/wc/data/sample_10M.txt", "file://giant//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*60)
#  f = fairy.input(["sample/wc/data/sample_30M.txt"]*120)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"]*360)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*2)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_50M.txt"]*2)
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_120M.txt"]*2)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
#  f = fairy.input(["file://giant//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
#  f = fairy.input(["file://giant//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :postqueuing_policy => {:queuing_class => :ChunkedSizedPoolQueue})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-66.vf")

when "69.2.5"
#  f = fairy.input(["sample/wc/data/sample_30M.txt"]*120)
#  f = fairy.input(["sample/wc/data/sample_30M.txt"]*4)
#  f = fairy.input(["sample/wc/data/sample_10M.txt"]*10)
#  f = fairy.input(["sample/wc/data/sample_10M.txt", "file://giant//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*60)
#  f = fairy.input(["sample/wc/data/sample_30M.txt"]*30)
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"]*30)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :postqueuing_policy => {:queuing_class => :ChunkedFileBufferdQueue},
		     :buffering_policy => {
		       :buffering_class => :ExtMergeSortBuffer})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-66.vf")


when "69.3.0"
#  f = fairy.input(["sample/wc/data/sample_30M.txt"]*120)
#  f = fairy.input(["sample/wc/data/sample_30M.txt"]*120)
#  f = fairy.input(["sample/wc/data/sample_30M.txt"]*60)
  f = fairy.input(["sample/wc/data/sample_10M.txt"]*10)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by2(%{|w| w})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-66.vf")

when "69.3.1"
#  f = fairy.input(["sample/wc/data/sample_30M.txt"]*120)
#  f = fairy.input(["sample/wc/data/sample_30M.txt"]*120)
  f = fairy.input(["sample/wc/data/sample_30M.txt"]*10)
#  f = fairy.input(["sample/wc/data/sample_10M.txt"]*10)
#  f = fairy.input(["sample/wc/data/fairy.cat"]*10)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by2(%{|w| w},
		      :postqueuing_policy => {
			:queuing_class => :SortedQueue,
			:sort_by => %{|w| w}
		      })
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-66.vf")

when "69.3.2"
#  f = fairy.input(["sample/wc/data/sample_30M.txt"]*120)
#  f = fairy.input(["sample/wc/data/sample_30M.txt"]*120)
#  f = fairy.input(["sample/wc/data/sample_30M.txt"]*30)
  f = fairy.input(["sample/wc/data/sample_10M.txt"]*10)
#  f = fairy.input(["sample/wc/data/fairy.cat"]*10)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by2(%{|w| w},
		      :postqueuing_policy => {
			:queuing_class => :SortedQueue1})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-66.vf")


when "69.4.0"
#  f = fairy.input(["sample/wc/data/sample_30M.txt"]*120)
#  f = fairy.input(["sample/wc/data/sample_30M.txt"]*120)
  f = fairy.input(["sample/wc/data/sample_30M.txt"]*30)
#  f = fairy.input(["sample/wc/data/sample_10M.txt"]*10)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by3(%{|w| w})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-66.vf")

when "69.4.1"
#  f = fairy.input(["sample/wc/data/sample_30M.txt"]*120)
#  f = fairy.input(["sample/wc/data/sample_30M.txt"]*120)
#  f = fairy.input(["sample/wc/data/sample_30M.txt"]*30)
  f = fairy.input(["sample/wc/data/sample_10M.txt"]*10)
#  f = fairy.input(["sample/wc/data/fairy.cat"]*10)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by3(%{|w| w},
		      :postqueuing_policy => {
			:queuing_class => :OnMemorySortedQueue,
			:sort_by => %{|w| w}
		      })
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-66.vf")

when "69.4.2"
#  f = fairy.input(["sample/wc/data/sample_30M.txt"]*120)
#  f = fairy.input(["sample/wc/data/sample_30M.txt"]*120)
#  f = fairy.input(["sample/wc/data/sample_30M.txt"]*30)
  f = fairy.input(["sample/wc/data/sample_10M.txt"]*10)
#  f = fairy.input(["sample/wc/data/fairy.cat"]*10)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by3(%{|w| w},
		      :postqueuing_policy => {
			:queuing_class => :SortedQueue1,
			:sort_by => %{|w| w}
		      })
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-66.vf")

when "70", "abort"
  
  require "timeout"

  begin
    timeout(20) {
#  f = fairy.input(["sample/wc/data/sample_30M.txt"]*120)
#  f = fairy.input(["sample/wc/data/sample_30M.txt"]*120)
      f = fairy.input(["sample/wc/data/sample_10M.txt"]*10)
      f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
      })
      f.output("test/test-66.vf")
    }
  rescue Timeout::Error
    puts "Abort!! ..."
    fairy.abort
    puts "Fin"
  end

when "70.1"
  require "timeout"

  begin
    timeout(20) {
#  f = fairy.input(["sample/wc/data/sample_30M.txt"]*120)
#  f = fairy.input(["sample/wc/data/sample_30M.txt"]*120)
#  f = fairy.input(["sample/wc/data/sample_30M.txt"]*10)
      f = fairy.input(["sample/wc/data/sample_10M.txt"]*10)
      f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
      })
      f = f.group_by(%{|w| w})
      f = f.map(%{|values| [values.key, values.size].join(" ")})
      #  f.here.each{|e| puts e.join(" ")}
      f.output("test/test-66.vf")
    }
  rescue Timeout::Error
    puts "Abort!! ..."
    fairy.abort
    puts "Fin"
  end

when "71", "REQ#183"
  
  iota = fairy.input(Fairy::InputIota, 100)
  f = iota.map(%{|e| e % 2 == 0 ? Import::TOKEN_NULLVALUE : e})
  for e in f.here
    puts e
  end

when "71.1"
  
  iota = fairy.input(Fairy::InputIota, 100)
  f = iota.seg_map(%{|i, callback| 
                   i.each{|e| callback.call(e % 2 == 0 ? Import::TOKEN_NULLVALUE : e)}})
  for e in f.here
    puts e
  end

when "71.2"
  
  iota = fairy.input(Fairy::InputIota, 100)
  f = iota.map(%{|e| e % 2 == 0 ? Import::TOKEN_NULLVALUE : e},
	       :postmapping_policy => :MPNewProcessorN)
  for e in f.here
    puts e
  end

when "72"
  fairy = Fairy::Fairy.new(:POSTMAPPING_POLICY => :MPNewProcessorN)
  iota = fairy.input(Fairy::InputIota, 100)
  f = iota.map(%{|e| e % 2 == 0 ? Import::TOKEN_NULLVALUE : e})
  for e in f.here
    puts e
  end

when "73", "REQ#144"
  iota = fairy.input(Fairy::InputIota, 100)
  f = iota.map(%{|e| e % 2 == 0 ? raise("foo") : e},
	       :ignore_exception => true)
  for e in f.here
    puts e
  end

when "73.1"
  
  iota = fairy.input(Fairy::InputIota, 100, :ignore_exception => false)
  f = iota.seg_map(%{|i, callback| 
                   i.each{|e| callback.call(e % 2 == 0 ? raise("foo") : e)}},
		 :ignore_exception => true)
  for e in f.here
    puts e
  end

when "73.1.1"
  
  iota = fairy.input(Fairy::InputIota, 100)
  f = iota.seg_map(%{|i, callback| 
                   raise "foo"
                   i.each{|e| callback.call(e % 2 == 0 ? raise("foo") : e)}},
		 :ignore_exception => true)
  for e in f.here
    puts e
  end

when "73.2"
  
  iota = fairy.input(Fairy::InputIota, 100)
  f = iota.map(%{|e| e % 2 == 0 ? raise("foo") : e},
	       :postmapping_policy => :MPNewProcessorN,
	       :ignore_exception => true)
  for e in f.here
    puts e
  end

when "74", "BUG#190"
  f = fairy.exec(["file://localhost/etc/passwd", "file://localhost/etc/group"]*10)
  for l in f.here
    puts l
  end

when "74.0.1"
  f = fairy.exec(["file://localhost/etc/passwd", "file://gentoo/etc/group"]*10)
  for l in f.here
    puts l
  end

when "74.1"
  f = fairy.exec(["file://localhost/etc/passwd", "file://localhost/etc/group"])
  for l in f.here
    puts l
  end
  f = fairy.exec(["file://localhost/etc/passwd", "file://localhost/etc/group"])
  for l in f.here
    puts l
  end
  f = fairy.exec(["file://localhost/etc/passwd", "file://localhost/etc/group"])
  for l in f.here
    puts l
  end

when "74.1.0"
  f = fairy.exec(["file://localhost/etc/passwd", "file://gentoo/etc/group"])
  for l in f.here
    puts l
  end
  f = fairy.exec(["file://localhost/etc/passwd", "file://gentoo/etc/group"])
  for l in f.here
    puts l
  end
  f = fairy.exec(["file://localhost/etc/passwd", "file://gentoo/etc/group"])
  for l in f.here
    puts l
  end

when "74.2"
  f = fairy.exec(["file://localhost/etc/passwd", "file://localhost/etc/group"])
  f = f.map(%{|e| sleep 10; e},
	    :postmapping_policy=>:MPSameProcessorQ)
  for l in f.here
    puts l
  end

when "74.2.0"
  f = fairy.exec(["file://localhost/etc/passwd", "file://gentoo/etc/group"])
  f = f.map(%{|e| sleep 10; e},
	    :postmapping_policy=>:MPSameProcessorQ)
  for l in f.here
    puts l
  end


when "74.3"
  f = fairy.exec(["file://localhost/etc/passwd", "file://localhost/etc/group"])
  for l in f.here
    puts l
  end
  f = fairy.exec(["file://localhost/etc/passwd", "file://localhost/etc/group"])
  for l in f.here
    puts l
  end
  f = fairy.exec(["file://localhost/etc/passwd", "file://localhost/etc/group"])
  for l in f.here
    puts l
  end
  f = fairy.exec(["file://localhost/etc/passwd", "file://localhost/etc/group"])
  for l in f.here
    puts l
  end
  f = fairy.exec(["file://localhost/etc/passwd", "file://localhost/etc/group"])
  for l in f.here
    puts l
  end
  f = fairy.exec(["file://localhost/etc/passwd", "file://localhost/etc/group"])
  for l in f.here
    puts l
  end
  f = fairy.exec(["file://localhost/etc/passwd", "file://localhost/etc/group"])
  for l in f.here
    puts l
  end

  f = fairy.exec(["file://localhost/etc/passwd", "file://localhost/etc/group"])
  f = f.map(%{|e| sleep 10; e},
	    :postmapping_policy=>:MPSameProcessorQ)
  for l in f.here
    puts l
  end

when "74.3.0"
  f = fairy.exec(["file://localhost/etc/passwd", "file://gentoo/etc/group"])
  for l in f.here
    puts l
  end
#   f = fairy.exec(["file://localhost/etc/passwd", "file://gentoo/etc/group"])
#   for l in f.here
#     puts l
#   end
#   f = fairy.exec(["file://localhost/etc/passwd", "file://gentoo/etc/group"])
#   for l in f.here
#     puts l
#   end
#   f = fairy.exec(["file://localhost/etc/passwd", "file://gentoo/etc/group"])
#   for l in f.here
#     puts l
#   end
#   f = fairy.exec(["file://localhost/etc/passwd", "file://gentoo/etc/group"])
#   for l in f.here
#     puts l
#   end
#   f = fairy.exec(["file://localhost/etc/passwd", "file://gentoo/etc/group"])
#   for l in f.here
#     puts l
#   end
#   f = fairy.exec(["file://localhost/etc/passwd", "file://gentoo/etc/group"])
#   for l in f.here
#     puts l
#   end

  f = fairy.exec(["file://localhost/etc/passwd", "file://gentoo/etc/group"]*3)
  f = f.map(%{|e| sleep 10; e},
	    :postqueuing_policy=>{:queuing_class => :FileBufferdQueue},
	    :postmapping_policy=>:MPSameProcessorQ)
  for l in f.here(:prequeuing_policy=>{:queuing_class => :ChunkedSizedPoolQueue})

    puts l
  end

when "75", "memory leak test"
#  f = fairy.input(["sample/wc/data/sample_30M.txt"]*120)
#  f = fairy.input(["sample/wc/data/sample_30M.txt"]*120)
  f = fairy.input(["sample/wc/data/sample_30M.txt"]*60)
#  f = fairy.input(["sample/wc/data/sample_10M.txt"]*10)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f.output("test/test-75.vf")

when "75.1"
#  f = fairy.input(["sample/wc/data/sample_30M.txt"]*120)
#  f = fairy.input(["sample/wc/data/sample_30M.txt"]*120)
  f = fairy.input(["sample/wc/data/sample_30M.txt"]*60)
#  f = fairy.input(["sample/wc/data/sample_10M.txt"]*10)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end},
	     :postmapping_policy=>:MPSameProcessorQ)
  f.output("test/test-75.vf")


when "75.2"
#  f = fairy.input(["sample/wc/data/sample_30M.txt"]*120)
#  f = fairy.input(["sample/wc/data/sample_30M.txt"]*120)
  f = fairy.input(["sample/wc/data/sample_30M.txt"]*60)
#  f = fairy.input(["sample/wc/data/sample_10M.txt"]*10)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end},
	     :postmapping_policy=>:MPNewProcessorN)
  f.output("test/test-75.vf")

when "76", "BUG#215"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"])
  for l in f.here
    puts l
  end
  
when "77", "BUG#220"
  f = fairy.input(["file://localhost//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat1"])
  for l in f.here
    puts l
  end

when "78", "REQ#227"
#  f = fairy.input(["sample/wc/data/sample_30M.txt"]*120)
#  f = fairy.input(["sample/wc/data/sample_30M.txt"]*4)
#  f = fairy.input(["sample/wc/data/sample_10M.txt"]*10)
#  f = fairy.input(["sample/wc/data/sample_10M.txt", "file://giant//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*60)
#  f = fairy.input(["sample/wc/data/sample_30M.txt"]*120)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"]*10)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*10)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*1)
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_50M.txt"]*2)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_120M.txt"]*2)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
#  f = fairy.input(["file://giant//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
#  f = fairy.input(["file://giant//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :postqueuing_policy => {:queuing_class => :ChunkedFileBufferdQueue})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "78.1"
#  f = fairy.input(["sample/wc/data/sample_30M.txt"]*120)
#  f = fairy.input(["sample/wc/data/sample_30M.txt"]*4)
#  f = fairy.input(["sample/wc/data/sample_10M.txt"]*10)
#  f = fairy.input(["sample/wc/data/sample_10M.txt", "file://giant//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*60)
#  f = fairy.input(["sample/wc/data/sample_30M.txt"]*120)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"]*10)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*10)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"]*1)
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_50M.txt"]*2)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_120M.txt"]*2)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
#  f = fairy.input(["file://giant//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
#  f = fairy.input(["file://giant//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :postqueuing_policy => {:queuing_class => :ChunkedFileBufferdQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :ChunkedPoolQueue}
)
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "78.1.1"
#  f = fairy.input(["sample/wc/data/sample_30M.txt"]*120)
#  f = fairy.input(["sample/wc/data/sample_30M.txt"]*4)
#  f = fairy.input(["sample/wc/data/sample_10M.txt"]*10)
#  f = fairy.input(["sample/wc/data/sample_10M.txt", "file://giant//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*60)
#  f = fairy.input(["sample/wc/data/sample_30M.txt"]*120)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*10)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*1)
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_50M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_120M.txt"]*4)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
#  f = fairy.input(["file://giant//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
#  f = fairy.input(["file://giant//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :postqueuing_policy => {:queuing_class => :ChunkedFileBufferdQueue},
		     :no_segment => 1)
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "78.2"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.seg_split(1)
  f = f.map(%{|key| [key].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "78.2.1"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*1)
  f = f.seg_split(1)
  f = f.map(%{|key| [key].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "79.0"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split.collect{|e| e+"\n"}
		    rescue
		      []
		    end
  })
  f.output("/home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M-split.txt")

when "79.1"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M-split.txt"]*1,
		  :dummy => 1)
  f = f.group_by(%{|w| w},
		     :postqueuing_policy => {:queuing_class => :ChunkedFileBufferdQueue},
		     :no_segment => 1)
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "79.2"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M-split.txt"]*1)
  f = f.group_by(%{|w| w},
		     :postqueuing_policy => {:queuing_class => :ChunkedFileBufferdQueue},
		     :no_segment => 1)
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "79.3"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M-split.txt"]*1)
  f = f.seg_split(1)
#  f = f.map(%{|key| [key].join(" ")})
  f.output("test/test-78.vf")

when "80"
  f = fairy.wc(
	       #["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1,
	       ["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"]*1,
	       "test/test-80.vf",
	       :no_segment => 1)

when "80.1"
  f = fairy.wc(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1,
	       "test/test-80.vf",
	       :no_segment => 1)

when "81.0"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :postqueuing_policy => {:queuing_class => :ChunkedFileBufferdQueue},
		     :no_segment => 1)
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "82.0" #78.1.1
#  f = fairy.input(["sample/wc/data/sample_30M.txt"]*120)
#  f = fairy.input(["sample/wc/data/sample_30M.txt"]*4)
#  f = fairy.input(["sample/wc/data/sample_10M.txt"]*10)
#  f = fairy.input(["sample/wc/data/sample_10M.txt", "file://giant//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*60)
#  f = fairy.input(["sample/wc/data/sample_30M.txt"]*120)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*10)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*1)
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_50M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_120M.txt"]*4)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
#  f = fairy.input(["file://giant//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
#  f = fairy.input(["file://giant//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :postqueuing_policy => {:queuing_class => :ChunkedFileBufferdQueue},
		     :no_segment => 1)
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "82.1"
#f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"])
#  f = fairy.input(["sample/wc/data/sample_30M.txt"]*120)
#  f = fairy.input(["sample/wc/data/sample_30M.txt"]*4)
#  f = fairy.input(["sample/wc/data/sample_10M.txt"]*10)
#  f = fairy.input(["sample/wc/data/sample_10M.txt", "file://giant//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*60)
#  f = fairy.input(["sample/wc/data/sample_30M.txt"]*120)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*10)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*1)
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_50M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_120M.txt"]*4)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
#  f = fairy.input(["file://giant//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
#  f = fairy.input(["file://giant//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :postqueuing_policy => {:queuing_class => :MarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :MarshaledQueue},
		     :no_segment => 1)
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "82.2"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :postqueuing_policy => {:queuing_class => :ChunkedPoolQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :ChunkedPoolQueue},
		     :no_segment => 1)
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "82.3"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :postqueuing_policy => {:queuing_class => :ChunkedPoolQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :ChunkedSizedPoolQueue},
		     :no_segment => 1)
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "82.4.0"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.seg_split(1)
  f = f.map(%{|key| [key].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "82.4.1"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.seg_split(1,
	      :postqueuing_policy => {:queuing_class => :MarshaledQueue}
	      )
  f = f.map(%{|key| [key].join(" ")},
	    :prequeuing_policy => {:queuing_class => :MarshaledQueue}
	    )
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "82.4.2"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.seg_split(1,
	      :postqueuing_policy => {:queuing_class => :ChunkedPoolQueue}
	      )
  f = f.map(%{|key| [key].join(" ")},
	    :prequeuing_policy => {:queuing_class => :ChunkedPoolQueue}
	    )
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "82.5.0"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.seg_split(1,
	      :postqueuing_policy => {:queuing_class => :MarshaledQueue}
	      )
  f = f.map(%{|key| [key].join(" ")},
	    :prequeuing_policy => {:queuing_class => :SizedMarshaledQueue}
	    )
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "82.5.1"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :postqueuing_policy => {:queuing_class => :MarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :SizedMarshaledQueue},
		     :no_segment => 1)
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "82.6.0"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.seg_split(1,
	      :postqueuing_policy => {:queuing_class => :FileMarshaledQueue}
	      )
  f = f.map(%{|key| [key].join(" ")},
	    :prequeuing_policy => {:queuing_class => :SizedMarshaledQueue}
	    )
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "82.6.1"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :SizedMarshaledQueue},
		     :no_segment => 1)
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "82.6.2"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :no_segment => 1)
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "82.7.0"
  f = fairy.wc(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"]*1, 
	       "test/test-78.vf",
	       :no_segment => 1)

when "82.7.1"
  f = fairy.wc(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"]*1, 
	       "test/test-78.vf",
	       :no_segment => 1,
	       :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
	       :postfilter_prequeuing_policy => {:queuing_class => :SizedMarshaledQueue})

when "82.7.2"
  f = fairy.wc(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"]*1, 
	       "test/test-78.vf",
	       :no_segment => 1,
	       :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
	       :postfilter_prequeuing_policy => {:queuing_class => :FileMarshaledQueue})

when "83.0"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :postqueuing_policy => {:queuing_class => :ChunkedFileBufferdQueue},
		     :no_segment => 1)
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "83.1"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :postqueuing_policy => {:queuing_class => :ChunkedFileBufferdQueue},
		     :no_segment => 1,
		     :buffering_policy => {:buffering_class => :DepqMergeSortBuffer})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "83.2"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :postqueuing_policy => {:queuing_class => :ChunkedFileBufferdQueue},
		     :no_segment => 1,
		     :buffering_policy => {:buffering_class => :DepqMergeSortBuffer2})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "83.3"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :postqueuing_policy => {:queuing_class => :ChunkedFileBufferdQueue},
		     :no_segment => 1,
		     :buffering_policy => {:buffering_class => :PQMergeSortBuffer})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "83.4.0"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :postqueuing_policy => {:queuing_class => :ChunkedFileBufferdQueue},
		     :no_segment => 1)
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "83.4.1"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :postqueuing_policy => {:queuing_class => :ChunkedFileBufferdQueue},
		     :no_segment => 1,
		     :buffering_policy => {:buffering_class => :PQMergeSortBuffer2})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "83.4.2"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_50M.txt"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :postqueuing_policy => {:queuing_class => :ChunkedFileBufferdQueue},
		     :no_segment => 1)
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "83.4.3"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_50M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :postqueuing_policy => {:queuing_class => :ChunkedFileBufferdQueue},
		     :no_segment => 1,
		     :buffering_policy => {:buffering_class => :PQMergeSortBuffer})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")


when "83.4.4"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_120M.txt"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :postqueuing_policy => {:queuing_class => :ChunkedFileBufferdQueue},
		     :no_segment => 1)
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "83.4.5"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_120M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :postqueuing_policy => {:queuing_class => :ChunkedFileBufferdQueue},
		     :no_segment => 1,
		     :buffering_policy => {:buffering_class => :PQMergeSortBuffer})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "83.4.6"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"]*16)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :postqueuing_policy => {:queuing_class => :ChunkedFileBufferdQueue},
		     :no_segment => 1)
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "83.4.7"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"]*16)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :postqueuing_policy => {:queuing_class => :ChunkedFileBufferdQueue},
		     :no_segment => 1,
		     :buffering_policy => {:buffering_class => :PQMergeSortBuffer})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")


when "84"
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*1)
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w})
  wc1 = f.map(%{|values| [values.key, values.size]})


  g = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*1)
#  g = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  g = g.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  g = g.group_by(%{|w| w})
  wc2 = g.map(%{|values| [values.key, values.size]})

  for l in wc1.equijoin2(wc2, 0).here
    p l
  end

when "84.1"
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*1)
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w})
  wc1 = f.map(%{|values| [values.key, values.size]})


  g = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*1)
#  g = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  g = g.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  g = g.group_by(%{|w| w})
  wc2 = g.map(%{|values| [values.key, values.size]})
  x = wc1.equijoin2(wc2, 0).map(%{|w1, w2| w1[0]})

  for l in x.here
    p l
  end

when "85.0"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"]*16)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :postqueuing_policy => {:queuing_class => :ChunkedFileBufferdQueue},
		     :no_segment => 1)
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "85.1"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"]*16)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :postqueuing_policy => {:queuing_class => :ChunkedFileBufferdQueue},
		     :no_segment => 1,
		     :buffering_policy => {:buffering_class => :PQMergeSortBuffer})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")


when "85.2"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"]*16)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 1,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :FileMarshaledQueue})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")



when "85.3"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"]*16)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 1,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :buffering_policy => {:buffering_class => :PQMergeSortBuffer})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "85.3.1"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"]*16)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 1,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :buffering_policy => {:buffering_class => :DepqMergeSortBuffer})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "85.4.0"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"]*16)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :postqueuing_policy => {:queuing_class => :ChunkedFileBufferdQueue},
		     :no_segment => 1)
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "85.4.1"
  f = fairy.wc(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"]*16, 
	       "test/test-78.vf",
	       :no_segment => 1)

when "85.4.2"
  f = fairy.wc(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"]*16, 
	       "test/test-78.vf",
	       :no_segment => 1,
	       :buffering_policy => {:buffering_class => :PQMergeSortBuffer})

when "85.4.3"
  f = fairy.wc(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"]*16, 
	       "test/test-78.vf",
	       :no_segment => 1,
	       :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
	       :postfilter_prequeuing_policy => {:queuing_class => :FileMarshaledQueue})

when "85.4.4"
  f = fairy.wc(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"]*16, 
	       "test/test-78.vf",
	       :no_segment => 1,
	       :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
	       :postfilter_prequeuing_policy => {:queuing_class => :FileMarshaledQueue},
	       :buffering_policy => {:buffering_class => :PQMergeSortBuffer})

when "87.0"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"])
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :postqueuing_policy => {:queuing_class => :ChunkedFileBufferdQueue},
		     :no_segment => 1)
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "87.1"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"])
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 1,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :buffering_policy => {:buffering_class => :PQMergeSortBuffer})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")


when "87.2"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_60M.txt"])
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :postqueuing_policy => {:queuing_class => :ChunkedFileBufferdQueue},
		     :no_segment => 1)
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "87.3"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_60M.txt"])
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 1,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :buffering_policy => {:buffering_class => :PQMergeSortBuffer})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "87.4"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_120M.txt"])
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :postqueuing_policy => {:queuing_class => :ChunkedFileBufferdQueue},
		     :no_segment => 1)
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "87.5"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_120M.txt"])
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 1,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :buffering_policy => {:buffering_class => :PQMergeSortBuffer})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "87.6"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_240M.txt"])
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :postqueuing_policy => {:queuing_class => :ChunkedFileBufferdQueue},
		     :no_segment => 1)
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "87.7"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_240M.txt"])
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 1,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :buffering_policy => {:buffering_class => :PQMergeSortBuffer})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "87.8"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_480M.txt"])
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :postqueuing_policy => {:queuing_class => :ChunkedFileBufferdQueue},
		     :no_segment => 1)
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "87.9"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_480M.txt"])
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 1,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :buffering_policy => {:buffering_class => :PQMergeSortBuffer})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "87.10"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_960M.txt"])
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :postqueuing_policy => {:queuing_class => :ChunkedFileBufferdQueue},
		     :no_segment => 1)
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "87.11"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_960M.txt"])
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 1,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :buffering_policy => {:buffering_class => :PQMergeSortBuffer})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "87.12"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_1920M.txt"])
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :postqueuing_policy => {:queuing_class => :ChunkedFileBufferdQueue},
		     :no_segment => 1)
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "87.13"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_1920M.txt"])
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 1,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :buffering_policy => {:buffering_class => :PQMergeSortBuffer})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")


when "87.14"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_3840M.txt"])
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :postqueuing_policy => {:queuing_class => :ChunkedFileBufferdQueue},
		     :no_segment => 1)
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "87.15"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_3840M.txt"])
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 1,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :buffering_policy => {:buffering_class => :PQMergeSortBuffer})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "87.2.0"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"]*2)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :postqueuing_policy => {:queuing_class => :ChunkedFileBufferdQueue},
		     :no_segment => 2)
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "87.2.1"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"]*2)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 2,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :buffering_policy => {:buffering_class => :PQMergeSortBuffer})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")


when "87.2.2"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_60M.txt"]*2)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :postqueuing_policy => {:queuing_class => :ChunkedFileBufferdQueue},
		     :no_segment => 2)
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "87.2.3"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_60M.txt"]*2)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 2,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :buffering_policy => {:buffering_class => :PQMergeSortBuffer})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "87.2.4"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_120M.txt"]*2)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :postqueuing_policy => {:queuing_class => :ChunkedFileBufferdQueue},
		     :no_segment => 2)
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "87.2.5"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_120M.txt"]*2)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 2,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :buffering_policy => {:buffering_class => :PQMergeSortBuffer})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "87.2.6"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_240M.txt"]*2)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :postqueuing_policy => {:queuing_class => :ChunkedFileBufferdQueue},
		     :no_segment => 2)
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "87.2.7"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_240M.txt"]*2)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 2,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :buffering_policy => {:buffering_class => :PQMergeSortBuffer})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "87.2.8"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_480M.txt"]*2)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :postqueuing_policy => {:queuing_class => :ChunkedFileBufferdQueue},
		     :no_segment => 2)
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "87.2.9"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_480M.txt"]*2)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 2,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :buffering_policy => {:buffering_class => :PQMergeSortBuffer})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "87.2.10"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_960M.txt"]*2)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :postqueuing_policy => {:queuing_class => :ChunkedFileBufferdQueue},
		     :no_segment => 2)
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "87.2.11"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_960M.txt"]*2)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 2,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :buffering_policy => {:buffering_class => :PQMergeSortBuffer})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "87.2.12"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_1920M.txt"]*2)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :postqueuing_policy => {:queuing_class => :ChunkedFileBufferdQueue},
		     :no_segment => 2)
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "87.2.13"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_1920M.txt"]*2)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 2,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :buffering_policy => {:buffering_class => :PQMergeSortBuffer})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")


when "87.2.14"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_3840M.txt"]*2)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :postqueuing_policy => {:queuing_class => :ChunkedFileBufferdQueue},
		     :no_segment => 2)
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "87.2.15"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_3840M.txt"]*2)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 2,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :buffering_policy => {:buffering_class => :PQMergeSortBuffer})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "87.3.1"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"]*2)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 2,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :SizedMarshaledQueue},
		     :buffering_policy => {:buffering_class => :PQMergeSortBuffer})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")


when "87.3.3"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_60M.txt"]*2)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 2,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :SizedMarshaledQueue},
		     :buffering_policy => {:buffering_class => :PQMergeSortBuffer})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "87.3.5"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_120M.txt"]*2)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 2,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :SizedMarshaledQueue},
		     :buffering_policy => {:buffering_class => :PQMergeSortBuffer})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "87.3.7"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_240M.txt"]*2)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 2,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :SizedMarshaledQueue},
		     :buffering_policy => {:buffering_class => :PQMergeSortBuffer})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "87.3.9"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_480M.txt"]*2)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 2,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :SizedMarshaledQueue},
		     :buffering_policy => {:buffering_class => :PQMergeSortBuffer})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "87.3.11"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_960M.txt"]*2)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 2,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :SizedMarshaledQueue},
		     :buffering_policy => {:buffering_class => :PQMergeSortBuffer})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "87.3.13"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_1920M.txt"]*2)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 2,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :SizedMarshaledQueue},
		     :buffering_policy => {:buffering_class => :PQMergeSortBuffer})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "87.3.15"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_3840M.txt"]*2)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 2,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :SizedMarshaledQueue},
		     :buffering_policy => {:buffering_class => :PQMergeSortBuffer})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "87.4.0"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_1920M.txt"]*2)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 2,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :buffering_policy => {
		       :buffering_class => :PQMergeSortBuffer,
		       :threshold => 100_000})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "87.4.1"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"]*2)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 2,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :SizedMarshaledQueue},
		     :buffering_policy => {
		       :buffering_class => :PQMergeSortBuffer,
		       :threshold => 100_000})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")


when "87.4.3"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_60M.txt"]*2)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 2,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :SizedMarshaledQueue},
		     :buffering_policy => {
		       :buffering_class => :PQMergeSortBuffer,
		       :threshold => 100_000})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "87.4.5"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_120M.txt"]*2)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 2,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :SizedMarshaledQueue},
		     :buffering_policy => {
		       :buffering_class => :PQMergeSortBuffer,
		       :threshold => 100_000})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "87.4.7"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_240M.txt"]*2)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 2,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :SizedMarshaledQueue},
		     :buffering_policy => {
		       :buffering_class => :PQMergeSortBuffer,
		       :threshold => 100_000})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "87.4.9"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_480M.txt"]*2)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 2,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :SizedMarshaledQueue},
		     :buffering_policy => {
		       :buffering_class => :PQMergeSortBuffer,
		       :threshold => 100_000})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "87.4.11"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_960M.txt"]*2)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 2,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :SizedMarshaledQueue},
		     :buffering_policy => {
		       :buffering_class => :PQMergeSortBuffer,
		       :threshold => 100_000})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "87.4.13"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_1920M.txt"]*2)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 2,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :buffering_policy => {
		       :buffering_class => :PQMergeSortBuffer,
		       :threshold => 3200_000})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "87.4.15"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_3840M.txt"]*2)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 2,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :SizedMarshaledQueue},
		     :buffering_policy => {
		       :buffering_class => :PQMergeSortBuffer,
		       :threshold => 100_000})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "87.5.0"

  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_960M.txt"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 1,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :buffering_policy => {
		       :buffering_class => :MergeSortBuffer,
		       :threshold => 400_000})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "87.5.1"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_960M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 1,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :buffering_policy => {
		       :buffering_class => :PQMergeSortBuffer,
		       :threshold => 400_000})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "88.0.1"

  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 1,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :buffering_policy => {
		       :buffering_class => :DirectMergeSortBuffer,
		       :threshold => 400_000})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "88.0.2"

  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 1,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :buffering_policy => {
		       :buffering_class => :DirectPQMergeSortBuffer,
		       :threshold => 400_000})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")


when "88.1.0"

  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_960M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 1,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :buffering_policy => {
		       :buffering_class => :MergeSortBuffer,
		       :threshold => 3_200_000})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "88.1.1"

  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_960M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 1,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :buffering_policy => {
		       :buffering_class => :DirectMergeSortBuffer,
		       :threshold => 25_600_000})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")


when "88.1.2"

  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_960M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 1,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :buffering_policy => {
		       :buffering_class => :PQMergeSortBuffer,
		       :threshold => 400_000})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")


when "88.1.3"

  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_960M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 1,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :buffering_policy => {
		       :buffering_class => :DirectPQMergeSortBuffer,
		       :threshold => 3_200_000})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "88.2.1"

  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_960M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 1,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :buffering_policy => {
		       :buffering_class => :DirectMergeSortBuffer,
		       :threshold => 3_200_000,
		       :chunk_size => 1000})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "88.2.3"

  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_960M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 1,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :buffering_policy => {
		       :buffering_class => :DirectPQMergeSortBuffer,
		       :threshold => 3_200_000,
		       :chunk_size => 1000})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")


when "89.0"

  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 1,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :buffering_policy => {
		       :buffering_class => :DirectFBMergeSortBuffer,
		       :threshold => 100_000,
		       :chunk_size => 1000})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "89.1.0"

  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_960M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 1,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :buffering_policy => {
		       :buffering_class => :DirectMergeSortBuffer,
		       :threshold => 3_200_000,
		       :chunk_size => 1000})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "89.1.1"

  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_960M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 1,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :buffering_policy => {
		       :buffering_class => :DirectFBMergeSortBuffer,
		       :threshold => 3_200_000,
		       :chunk_size => 5000})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "90.0"

  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_960M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 1,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :buffering_policy => {
		       :buffering_class => :MergeSortBuffer,
		       :threshold => 3_200_000})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "90.1"

  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_960M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 1,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :buffering_policy => {
		       :buffering_class => :DirectMergeSortBuffer,
		       :threshold => 3_200_000,
		       :chunk_size => 1000})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "90.2"

  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_960M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 1,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :buffering_policy => {
		       :buffering_class => :DirectFBMergeSortBuffer,
		       :threshold => 3_200_000,
		       :chunk_size => 5000})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "91.0"

  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_960M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 1,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :buffering_policy => {
		       :buffering_class => :DirectKBMergeSortBuffer,
		       :threshold => 100000})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "91.0.0"

  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_960M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 1,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :buffering_policy => {
		       :buffering_class => :DirectMergeSortBuffer,
		       :threshold => 100000})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "91.0.1"

  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_960M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 1,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :buffering_policy => {
		       :buffering_class => :DirectFBMergeSortBuffer,
		       :threshold => 100000})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "91.1.0"

  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_960M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 1,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :buffering_policy => {
		       :buffering_class => :DirectMergeSortBuffer,
		       :threshold => 3_200_000,
		       :chunk_size => 5000})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "91.1.1"

  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_960M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 1,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :buffering_policy => {
		       :buffering_class => :DirectKBMergeSortBuffer,
		       :threshold => 3_200_000,
		       :chunk_size => 5000})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")


when "91.1.2"

  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_960M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 1,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :buffering_policy => {
		       :buffering_class => :DirectKBMergeSortBuffer,
		       :threshold => 1_600_000,
		       :chunk_size => 8})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "92.0"

  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_960M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 1,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :buffering_policy => {
		       :buffering_class => :DirectKB2MergeSortBuffer,
		       :threshold => 100000})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "92.1"

  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_960M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 1,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :buffering_policy => {
		       :buffering_class => :DirectKB2MergeSortBuffer,
		       :threshold => 1_600_000,
		       :chunk_size => 256})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "93.1"

  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_960M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 1,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :buffering_policy => {
		       :buffering_class => :DirectMergeSortBuffer,
		       :threshold => 3_200_000,
		       :chunk_size => 1000})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "93.2"

  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_960M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 1,
		     :postqueuing_policy => {:queuing_class => :MarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :buffering_policy => {
		       :buffering_class => :DirectMergeSortBuffer,
		       :threshold => 3_200_000,
		       :chunk_size => 1000})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "93.3"

  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_960M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 1,
		     :postqueuing_policy => {
		       :queuing_class => :SizedMarshaledQueue,
		       :size => 640},
		     :postfilter_prequeuing_policy => {
		       :queuing_class => :FileMarshaledQueue
		     },
		     :buffering_policy => {
		       :buffering_class => :DirectMergeSortBuffer,
		       :threshold => 3_200_000,
		       :chunk_size => 1000})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "94.1"

  f = fairy.wc(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_960M.txt"]*1, 
	       "test/test-78.vf",
	       :no_segment => 1,
	       :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
	       :postfilter_prequeuing_policy => {:queuing_class => :FileMarshaledQueue},
	       :buffering_policy => {
		 :buffering_class => :DirectMergeSortBuffer,
		       :threshold => 1_600_000,
		       :chunk_size => 1000
	       })

when "95"

#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_960M.txt"]*1)
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 1,
		     :postqueuing_policy => {
		       :queuing_class => :FileMarshaledQueue,
		       :min_chunk_no => 5_120_000},
		     :postfilter_prequeuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :buffering_policy => {
		       :buffering_class => :DirectMergeSortBuffer,
		       :threshold => 1_600_000,
		       :chunk_size => 1000})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "96"
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_960M.txt"]*1)
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f.sort_by(%{|l| l}, :no_segment => 1).output("test/test-96.vf")

when "96.1"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_960M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f.sort_by(%{|l| l}, 
	    :no_segment => 1,
	    :postqueuing_policy => {
	      :queuing_class => :FileMarshaledQueue,
	      :min_chunk_no => 20_000},
	    :postfilter_prequeuing_policy => {:queuing_class => :FileMarshaledQueue},
	    :buffering_policy => {
	      :buffering_class => "NModGroupBy::DirectMergeSortBuffer",
	      :threshold => 1_600_000,
	      :chunk_size => 20000}).output("test/test-96.vf")

when "96.2"
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_960M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*1)
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f.sort_by(%{|l| l}, :no_segment => 1).output("test/test-96.vf")

when "96.3"
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_960M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*2)
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f.sort_by(%{|l| l}, :no_segment => 2).output("test/test-96.vf")

when "97", "BUG#250"
  
  input = fairy.exec(%w{ emperor }.map{|n| "file://#{n}"})
  map = input.map(%q{|uri| "Hello, #{`hostname`.chomp}!"})
  map.here.each{|hello| puts hello} 

when "97.1"
  
  input = fairy.exec(%w{ emperor }.map{|n| "file://#{n}"})
  map = input.map(%q{|uri| "Hello, #{`hostname`.chomp}!"})
  map.output("test/test97")

when "97.2"
  
  input = fairy.exec(%w{ emperor }.map{|n| "file://#{n}"})
  map = input.map(%q{|uri| "Hello, #{`hostname`.chomp}!"})
  map.here.each{|hello| puts hello} 

  GC.start

  sleep 100

when "97.3"
  
  input = fairy.exec(%w{ emperor }.map{|n| "file://#{n}"})
  map = input.map(%q{|uri| "Hello, #{`hostname`.chomp}!"})
  map.here.each{|hello| puts hello} 
  fairy.abort

  sleep 10

when "97.4"
  
  input = fairy.exec(%w{ emperor }.map{|n| "file://#{n}"})
  map = input.map(%q{|uri| "Hello, #{`hostname`.chomp}!"})
  map.here.each_with_bjobeach{|hello| puts hello} 

when "97.5"
  
  input = fairy.exec(%w{ emperor }.map{|n| "file://#{n}"})
  map = input.map(%q{|uri| "Hello, #{`hostname`.chomp}!"})
  map.here.each3{|hello| puts hello} 

when "98", "REQ#253"
when "98.1"
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"])
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 1,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :buffering_policy => {:buffering_class => :OnMemoryBuffer})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  f.here.each{|e| puts e}
  # f.output("test/test-78.vf")

when "98.2"
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"])
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = fairy.input(["file://emperor//etc/passwd"]*10)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 1,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :buffering_policy => {:buffering_class => :SimpleFileByKeyBuffer})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  f.here.each{|e| puts e}
  # f.output("test/test-78.vf")

when "98.3"
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"])
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
#  f = fairy.input(["file://emperor//etc/passwd"]*10)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 1,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :buffering_policy => {:buffering_class => :SimpleCommandSortBuffer})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  f.here.each{|e| puts e}
  # f.output("test/test-78.vf")


when "98.4"
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"])
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
#  f = fairy.input(["file://emperor//etc/passwd"]*10)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 1,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :buffering_policy => {:buffering_class => :CommandMergeSortBuffer})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  f.here.each{|e| puts e}
  # f.output("test/test-78.vf")


when "98.5"
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"])
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
#  f = fairy.input(["file://emperor//etc/passwd"]*10)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 1,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :buffering_policy => {:buffering_class => :MergeSortBuffer})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  f.here.each{|e| puts e}
  # f.output("test/test-78.vf")


when "98.6"
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"])
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
#  f = fairy.input(["file://emperor//etc/passwd"]*10)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 1,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :buffering_policy => {:buffering_class => :ExtMergeSortBuffer})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  f.here.each{|e| puts e}
  # f.output("test/test-78.vf")

when "98.7"
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"])
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
#  f = fairy.input(["file://emperor//etc/passwd"]*10)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 1,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :buffering_policy => {:buffering_class => :DepqMergeSortBuffer})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  f.here.each{|e| puts e}
  # f.output("test/test-78.vf")



when "98.8"
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"])
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
#  f = fairy.input(["file://emperor//etc/passwd"]*10)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 1,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :buffering_policy => {:buffering_class => :DepqMergeSortBuffer2})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  f.here.each{|e| puts e}
  # f.output("test/test-78.vf")


when "98.9"
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"])
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
#  f = fairy.input(["file://emperor//etc/passwd"]*10)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 1,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :buffering_policy => {:buffering_class => :PQMergeSortBuffer})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  f.here.each{|e| puts e}
  # f.output("test/test-78.vf")

when "98.10"
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"])
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
#  f = fairy.input(["file://emperor//etc/passwd"]*10)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 1,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :buffering_policy => {:buffering_class => :PQMergeSortBuffer2})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  f.here.each{|e| puts e}
  # f.output("test/test-78.vf")

when "98.11"
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"])
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
#  f = fairy.input(["file://emperor//etc/passwd"]*10)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 1,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :buffering_policy => {:buffering_class => :DirectOnMemoryBuffer})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  f.here.each{|e| puts e}
  # f.output("test/test-78.vf")

when "98.12"
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"])
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
#  f = fairy.input(["file://emperor//etc/passwd"]*10)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 1,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :buffering_policy => {:buffering_class => :DirectMergeSortBuffer})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  f.here.each{|e| puts e}
  # f.output("test/test-78.vf")

when "98.13"
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"])
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
#  f = fairy.input(["file://emperor//etc/passwd"]*10)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 1,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :buffering_policy => {:buffering_class => :DirectFBMergeSortBuffer})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  f.here.each{|e| puts e}
  # f.output("test/test-78.vf")

when "98.14"
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"])
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
#  f = fairy.input(["file://emperor//etc/passwd"]*10)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 1,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :buffering_policy => {:buffering_class => :DirectPQMergeSortBuffer})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  f.here.each{|e| puts e}
  # f.output("test/test-78.vf")


when "98.15"
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"])
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
#  f = fairy.input(["file://emperor//etc/passwd"]*10)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 1,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :buffering_policy => {:buffering_class => :DirectKBMergeSortBuffer})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  f.here.each{|e| puts e}
  # f.output("test/test-78.vf")

when "98.16"
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_30M.txt"])
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
#  f = fairy.input(["file://emperor//etc/passwd"]*10)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 1,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :buffering_policy => {:buffering_class => :DirectKB2MergeSortBuffer})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  f.here.each{|e| puts e}
  # f.output("test/test-78.vf")

when "99", "BUG#257"

  a = fairy.input(["test/test-98-a.txt"]).map(%{|ln| rec = ln.split; rec})
  b = fairy.input(["test/test-98-b.txt"]).map(%{|ln| rec = ln.split; rec})

  joined = a.equijoin(b, 0)

  joined.map(%{|from_a,from_b, *rest| [from_a, from_b].inspect}).output("test/test-98-out.txt")


when "99.1"

  a = fairy.input(["test/test-98-a.txt"]).map(%{|ln| rec = ln.split; rec})
  b = fairy.input(["test/test-98-b.txt"]).map(%{|ln| rec = ln.split; rec})

  joined = a.equijoin2(b, 0)

  joined.map(%{|from_a,from_b| [from_a, from_b].inspect}).output("test/test-98.1-out.txt")

when "100", "BUG#258"

#  answer = File.readlines("testdata.txt").map{|ln|
  answer = File.readlines("sample/wc/data/fairy.cat").map{|ln|
    n = ln.chomp.to_i
    n
  }.sort

  fairy = Fairy::Fairy.new

  result = []

  fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1).map(%{|ln|
    n = ln.chomp.to_i
    n
  }).sort_by(%{|n| n.to_i}).here.each{|ent|
    result << ent
  }

  p (result == answer)

  result.clear
  fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1).map(%{|ln|
    n = ln.chomp.to_i
    n
  }).sort_by(%{|n| -(n.to_i)}).here.each{|ent|
    result << ent
  }

  p (result == answer.reverse)

when "101", "BUG#265"
  
  input = fairy.exec(%w{ emperor }.map{|n| "file://#{n}"})
  map = input.map(%q{|uri| "Hello, #{`hostname`.chomp}!"})
  map.here.each{|hello| puts hello} 

when "101.1"
  
  input = fairy.exec(%w{ emperor giant}.map{|n| "file://#{n}"})
  map = input.map(%q{|uri| "Hello, #{`hostname`.chomp}!"})
  map.here.each{|hello| puts hello} 

when "102"
    f = fairy.input(["file://emperor//etc/passwd"]*1)
  f = f.map(%{|ln| begin
                      ln.chomp.split(":")[0]
		    rescue
		      []
		    end
  })
#  g = f.map(%{|l| l[0]}).here
  g = f.sort_by(%{|l|p l; l}).here
  g.each{|x| p x}

when "102.1"
    f = fairy.input(["file://emperor//etc/passwd"]*1)
  f = f.map(%{|ln| begin
                      ln.chomp.split(":")
		    rescue
		      []
		    end
  })
#  g = f.map(%{|l| l[0]}).here
#  g = f.sort_by(%Q{|l|p $$.to_s+l.inspect; l.class == Array ? l.join(" ") : l}).here
  g = f.sort_by(%Q{|l|l.class == Array ? l.join(" ") : l}).here
  g.each{|x| p x}

when "102.2"
    f = fairy.input(["file://emperor//etc/passwd"]*1)
  f = f.map(%{|ln| begin
                      ln.chomp.split(":")
		    rescue
		      []
		    end
  })
#  g = f.map(%{|l| l[0]}).here
  g = f.sort_by(%Q{|l|l[0]}).here
  g.each{|x| p x}

when "103", "REQ#268", "simple-hash"

  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		 :hash_module => "fairy/share/hash-simple-hash",
		 :no_segment => 1,
		 :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		 :postfilter_prequeuing_policy => {:queuing_class => :FileMarshaledQueue},
		 :buffering_policy => {
		   :buffering_class => :DirectMergeSortBuffer,
		   :threshold => 400_000})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "103.0"

  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		 :hash_module => "fairy/share/hash-md5",
		 :no_segment => 1,
		 :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		 :postfilter_prequeuing_policy => {:queuing_class => :FileMarshaledQueue},
		 :buffering_policy => {
		   :buffering_class => :DirectMergeSortBuffer,
		   :threshold => 400_000})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "104", "BUG#272"
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*1)
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| sleep 1; begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		 :no_segment => 1,
		 :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		 :postfilter_prequeuing_policy => {:queuing_class => :FileMarshaledQueue},
		 :buffering_policy => {
		   :buffering_class => :DirectMergeSortBuffer,
		   :threshold => 400_000})
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  #  f.here.each{|e| puts e.join(" ")}
  f.output("test/test-78.vf")

when "105", "REQ#263"

#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_960M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*1)
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f.sort_by(%{|l| l}, :pvs => ["L", "M"]).output("test/test-96.vf")

when "106", "BUG#274"

#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_960M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*1)
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f.sort_by(%{|l| l}, :no_segment=>24).output("test/test-96.vf")

when "107", "BUG#276"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.grep(/接続/)
  f.here.each{|e| p e}

when "107.1"
  f = fairy.input("/home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat")
  f = f.grep(/接続/)
  f.here.each{|e| p e}

when "107.2"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1,
		  :postqueuing_policy => {:queuing_class => :FileMarshaledQueue}
)
  f = f.grep(/接続/)
  f.here.each{|e| p e}

when "107.3"
  f = fairy.there(File.open("/home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"))
  f = f.grep(/接続/)
  f.here.each{|e| p e}

when "108"
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = fairy.input(["file://emperor//tmp/Downloads/sort-200M.log"]*1)
  f = f.grep(/num/, :ignore_exception => true)
  f.here.each{|e| p e}

when "109"

  va = fairy.input(Fairy::InputIota, 1000).to_va

  10.times do
    for l in fairy.input(va).here
      puts l
    end
  end

when "109.1"

  va = fairy.input(Fairy::InputIota, 1000).to_va

  100.times do
    for l in fairy.input(va, :postqueuing_policy => {:queuing_class => :FileMarshaledQueue}).here
      puts l
    end
  end

when "109.2"

  va = fairy.input(Fairy::InputIota, 1000).to_va

  100.times do
    for l in fairy.input(va).basic_group_by(1).here
      puts l
    end
  end

when "109.3"

  va = fairy.input(Fairy::InputIota, 1000).to_va

  100.times do
    for l in fairy.input(va).basic_mgroup_by([1]).here
      puts l
    end
  end

when "109.4.0"

  va = fairy.input(Fairy::InputIota, 1000).to_va

  10.times do
    va = fairy.input(va).to_va
    for l in fairy.input(va).here
      puts l
    end
  end

when "109.4.1"

  va = fairy.input(Fairy::InputIota, 1000, :SPLIT_NO => 1).to_va

  10.times do |no|
    puts "ITR: #{no}"
    va = fairy.input(va).split(10).to_va
    for l in fairy.input(va).here
      puts l
    end
  end

when "109.4.1.1"

  fairy.input(Fairy::InputIota, 1000, :SPLIT_NO => 1).output("test/test-109-0.vf")

  10.times do |no|
    puts "ITR: #{no}"
    va = fairy.input("test/test-109-#{no}.vf").split(10).output("test/test-109-#{no+1}.vf")
    for l in fairy.input("test/test-109-#{no+1}.vf").here
      puts l
    end
  end

when "109.4.2"

  va = fairy.input(Fairy::InputIota, 1000, :SPLIT_NO => 1).to_va

  10.times do |no|
    puts "ITR: #{no}"
    va = fairy.input(va).basic_group_by(%{|i| i.to_i % 10}).to_va
    for l in fairy.input(va).here
      puts l
    end
  end

when "109.4.2.1"

  fairy.input(Fairy::InputIota, 1000, :SPLIT_NO => 1).output("test/test-109-0.vf")

  10.times do |no|
    puts "ITR: #{no}"
    fairy.input("test/test-109-#{no}.vf").basic_group_by(%{|i| i.to_i % 10}).output("test/test-109-#{no+1}.vf")
    for l in fairy.input("test/test-109-#{no+1}.vf").here
      puts l
    end
  end

when "110.init", "BUG#61"
  fairy.input(Fairy::InputIota, 1000, :SPLIT_NO => 10).output("test/test-110.vf")

when "110.1"
  fairy.input(Fairy::InputIota, 1000, :SPLIT_NO => 5).output("test/test-110.vf")

when "111", "REQ#292"
  fairy.input(Fairy::InputIota, 1000, :SPLIT_NO => 5).map(%{|i| p i}).done

when "111.1"
  fairy.input(Fairy::InputIota, 10000, :SPLIT_NO => 500).map(%{|i| p i}).done

when "112"
  here = fairy.input(Fairy::InputIota, 1000, :SPLIT_NO => 20).here
  for n in here
    p n
  end

when "113", "REQ#201"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*2)
  f.output("test/test113")

when "113.0"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*2)
  f.here.each{|e| puts e}

when "114"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_960M.txt"])
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_120M.txt"])
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"])
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		 :no_segment => 1,
		 :postqueuing_policy => {
		   :queuing_class => :XMarshaledQueue,
		   :chunk_size => 10000},
		 :postfilter_prequeuing_policy => {
		   :queuing_class => :XMarshaledQueue,
		   :chunk_size => 10000},)
  f = f.map(%{|values| [values.key, values.size].join(" ")})
#  f.here.each{|e| puts e}
  f.output("test/test-114.vf")

when "114.NS"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_960M.txt"])
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_120M.txt"])
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		 :no_segment => 1,
		 :postqueuing_policy => {:queuing_class => :XMarshaledQueue},
		 :postfilter_prequeuing_policy => {:queuing_class => :XMarshaledQueue},
		 :use_string_buffer => false)
  f = f.map(%{|values| [values.key, values.size].join(" ")})
#  f.here.each{|e| puts e}
  f.output("test/test-114.vf")


when "114.F"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"])
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_120M.txt"])
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 1,
		     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
		     :postfilter_prequeuing_policy => {:queuing_class => :FileMarshaledQueue},)
  f = f.map(%{|values| [values.key, values.size].join(" ")})
#  f.here.each{|e| puts e}
  f.output("test/test-114.vf")


when "114.0"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"])
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  },		  
	     :postmapping_policy => :MPNewProcessorN,
	     :postqueuing_policy => {:queuing_class => :XMarshaledQueue},
	     )
#  f.here.each{|e| puts e}
  f.output("test/test-114.vf",
	   :prequeuing_policy => {:queuing_class => :XMarshaledQueue},
)

when "114.0.F"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"])
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  },		  
	     :postmapping_policy => :MPNewProcessorN,
	     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
	     )
#  f.here.each{|e| puts e}
  f.output("test/test-114.vf")

when "114.0.H"
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"])
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  },		  
	     :postmapping_policy => :MPNewProcessorN,
	     :postqueuing_policy => {:queuing_class => :FileMarshaledQueue},
	     )
  f.here.each{|e| puts e}
#  f.output("test/test-114.vf")

when "115"
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_240M.txt"]*1)
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 1,
		 :postqueuing_policy => {
		   :queuing_class => :XMarshaledQueue,
		   :chunk_size => 10000,
		   :log_mstore => true,
		   :buffers_cache_limit => 100},
		 :postfilter_prequeuing_policy => {
		   :queuing_class => :XMarshaledQueue,
		   :chunk_size => 10000,
		   :log_mstore => true,
		   :buffers_cache_limit => 100},)
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  f.output("test/test-pf.vf")
  #  f.here.each{|e| puts e.join(" ")}

when "116"
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_240M.txt"]*1)
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.xgroup_by(%{|w| w},
		     :no_segment => 1,
		 :postqueuing_policy => {
		   :queuing_class => :XMarshaledQueue,
		   :chunk_size => 10000,
		   :log_mstore => true,
		   :buffers_cache_limit => 100},
		 :postfilter_prequeuing_policy => {
		   :queuing_class => :XMarshaledQueue,
		   :chunk_size => 10000,
		   :log_mstore => true,
		   :buffers_cache_limit => 100},)
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  f.output("test/test-pf.vf")
  #  f.here.each{|e| puts e.join(" ")}

when "117"
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_240M.txt"]*1)
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		 :group_by => :XGroupBy,
		 :no_segment => 1,
		 :postqueuing_policy => {
		   :queuing_class => :XMarshaledQueue,
		   :chunk_size => 10000,
		   :log_mstore => true,
		   :buffers_cache_limit => 100},
		 :buffering_policy => {
		   :buffering_class => :DirectMergeSortBuffer,
		   :threshold => 400_000},
		 :postfilter_prequeuing_policy => {
		   :queuing_class => :XMarshaledQueue,
		   :chunk_size => 10000,
		   :log_mstore => true,
		   :buffers_cache_limit => 100},)
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  f.output("test/test-pf.vf")
  #  f.here.each{|e| puts e.join(" ")}

when "117.0"
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_240M.txt"]*1)
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w}, :no_segment => 1)
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  f.output("test/test-pf.vf")
  #  f.here.each{|e| puts e.join(" ")}

when "118.0"
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_240M.txt"]*2)
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w}, :no_segment => 2)
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  f.output("test/test-pf.vf")
  #  f.here.each{|e| puts e.join(" ")}
end

# test


