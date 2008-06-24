
require "front/fairy"

Thread.abort_on_exception=true

case ARGV[0]
when "1", "input"
  fairy = Fairy::Fairy.new
  p fairy.input(["/etc/passwd", "/etc/group"])

when "2", "grep"
  fairy = Fairy::Fairy.new
  p f = fairy.input(["/etc/passwd", "/etc/group"]).grep(/#{ARGV[1]}/)
  
end



