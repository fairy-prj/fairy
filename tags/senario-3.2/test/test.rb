
require "front/fairy"

Thread.abort_on_exception=true

case ARGV[0]
when "1", "input"
  fairy = Fairy::Fairy.new
  p fairy.input(["/etc/passwd", "/etc/group"])

when "2", "grep"
  fairy = Fairy::Fairy.new
  p f = fairy.input(["/etc/passwd", "/etc/group"]).grep(/#{ARGV[1]}/)

when "3", "here"
  fairy = Fairy::Fairy.new
  here = fairy.input(["/etc/passwd", "/etc/group"]).here
  for l in here
    puts l
  end

when "3.1", "grep.here"
  fairy = Fairy::Fairy.new
  here = fairy.input(["/etc/passwd", "/etc/group"]).grep(/#{ARGV[1]}/).here
  for l in here
    puts l
  end

when "3.2", "map.here"
  fairy = Fairy::Fairy.new
  here = fairy.input(["/etc/passwd", "/etc/group"]).map(%{|l| l.chomp.split(/:/)}).here
  for l in here
    print l.join("-"), "\n"
  end
end



