
require "front/fairy"

Thread.abort_on_exception=true

case ARGV[0]
when "input"
  fairy = Fairy::Fairy.new
  p f.input(["/etc/passwd", "/etc/group"])

when "grep"
  fairy = Fairy::Fairy.new
  p f = fairy.input(["/etc/passwd", "/etc/group"]).grep(/#{ARGV[1]}/)
  
end



