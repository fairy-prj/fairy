
require "backend/controller"

Thread.abort_on_exception=true

Fairy::Controller.start("19999")

puts "Service Start"

sleep
