
require "front/fairy"

Thread.abort_on_exception=true

if ARGV[0] == "-njob-monitor"
  require "front/debug"
  ARGV.shift
  Fairy::Debug::njob_status_monitor_on
end

fairy = Fairy::Fairy.new

case ARGV[0]
when "1", "input"
  p fairy.input(["/etc/passwd", "/etc/group"])

when "2", "grep"
  p f = fairy.input(["/etc/passwd", "/etc/group"]).grep(/#{ARGV[1]}/)

when "3", "here"
  here = fairy.input(["/etc/passwd", "/etc/group"]).here
  for l in here
    puts l
  end

when "3.1", "grep.here"
  here = fairy.input(["/etc/passwd", "/etc/group"]).grep(/#{ARGV[1]}/).here
  for l in here
    puts l
  end

when "3.2", "map.here"
  here = fairy.input(["/etc/passwd", "/etc/group"]).map(%{|l| l.chomp.split(/:/)}).here
  for l in here
    print l.join("-"), "\n"
  end

when "3.3", "smap"
  here = fairy.input(["/etc/passwd", "/etc/group"]).smap(%{|i,o| i.sort.each{|e|o.push e}; o.push_eos}).here
  for l in here
    puts l
  end

when "4", "group_by"
  wc = fairy.input(["test/test-4-data1", "test/test-4-data2"]).group_by(%{|w| w}, %{|w| [w, 1]}).smap(%{|i, o| o.push([i.key, i.size]);o.push_eos})
  wc.here.each{|w, n| print "word: #{w}, count: #{n}"}
end



