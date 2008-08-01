
require "front/fairy"

Thread.abort_on_exception=true

if ARGV[0] == "-njob-monitor"
  require "front/debug"
  ARGV.shift
  Fairy::Debug::njob_status_monitor_on
  $sleep = 1
end

fairy = Fairy::Fairy.new("19998", "localhost", "19999")

case ARGV[0]
when "0", "service get"
  p fairy.backend_controller

when "1", "input"
  p fairy.input(["/etc/passwd", "/etc/group"])
  sleep $sleep if $sleep 

when "2", "grep"
  p f = fairy.input(["/etc/passwd", "/etc/group"]).grep(/#{ARGV[1]}/)
  sleep $sleep if $sleep 

when "3", "here"
  here = fairy.input(["/etc/passwd", "/etc/group"]).here
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
    puts l
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
    puts l
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


when "3.4", "njob-monitor"
  require "front/debug"
  Fairy::Debug::njob_status_monitor_on
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
  wc = fairy.input(["test/test-4-data1", "test/test-4-data2"]).group_by(%{|w| w.chomp.split(/\s+/)[0]}).smap(%{|i, o| o.push([i.key, i.size])})
  wc.here.each{|w, n| puts "word: #{w}, count: #{n}"}
  sleep $sleep if $sleep 


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

when "6", "sort"
  LOCAL_SORT_SIZE = 10
  input_files = ["/etc/passwd", "/etc/group"]
  
  size = fairy.input(input_files).smap(%{|i, o| o.push i.size}).here.inject(0){|c, n| c += n}

  prob = 10.0/size

end

