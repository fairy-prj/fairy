#!/usr/local/bin/ruby
#
#   test-mutex.rb - 
#   	$Release Version: $
#   	$Revision: 1.1 $
#   	$Date: 1997/08/08 00:57:08 $
#   	by Keiju ISHITSUKA
#
# --
#
#   
#

@RCS_ID='-$Id:  $-'

require "thread"

Thread.abort_on_exception=true

$v = 0
$m = Mutex.new
$cv = ConditionVariable.new

def foo(val)
  Thread.start do
    $m.synchronize do
      puts "T IN:#{val}"
      $v = val
      $cv.signal
      puts "T OUT:#{val}"
    end
  end
end

Thread.start do
  $m.synchronize do
    loop do
      puts "WAIT IN"
      $cv.wait($m)
      puts "WAIT OUT"
      puts $v
    end
  end
end
sleep 1

100.times do |i|
  foo i
end

sleep 2



    


