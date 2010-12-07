#!/usr/bin/env ruby
# encoding: UTF-8

require "fairy"

if ARGV[0] == "-njob-monitor"
  require "fairy/share/debug"
  ARGV.shift
  $monitor_on = true
  $sleep = 1
end

fairy = Fairy::Fairy.new

if $monitor_on
  Fairy::Debug::njob_status_monitor_on(fairy)
end

case ARGV[0]
when "1"

  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_240M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.group_by(%{|w| w},
		     :no_segment => 1,)
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  f.output("test/test-pf.vf")
  #  f.here.each{|e| puts e.join(" ")}

when "2"

  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_240M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.seg_split(1)
  f = f.map(%{|w| [w].join(" ")})
  f.output("test/test-pf.vf")
  #  f.here.each{|e| puts e.join(" ")}

when "3"

  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_240M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.seg_split(1)
  f = f.map(%{|w| [w].join(" ")})
  f.output("test/test-pf.vf")
  #  f.here.each{|e| puts e.join(" ")}

when "4.0"

  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_240M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f.output("sample/wc/data/sample_240M_split.txt")
  #  f.here.each{|e| puts e.join(" ")}

when "4"
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_240M_split.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.seg_split(1)
  f = f.map(%{|w| [w].join(" ")})
  f.output("test/test-pf.vf")
  #  f.here.each{|e| puts e.join(" ")}

when "5"

  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_240M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.seg_split(1)
  f = f.post_mod_group_by_filter(Fairy::BlockSource.new(%{|w| w}))
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  f.output("test/test-pf.vf")
  #  f.here.each{|e| puts e.join(" ")}

when "6"

  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_240M.txt"]*1)
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
		 :hash_module => "fairy/share/hash-simple-hash")
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  f.output("test/test-pf.vf")
  #  f.here.each{|e| puts e.join(" ")}

when "7"

  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_240M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.seg_map(%{|i, b| i.each_slice(100){|*e| b.call e.join(" ")}})
  f = f.seg_split(1)
  f = f.map(%{|w| [w].join(" ")})
  f.output("test/test-pf.vf")
  #  f.here.each{|e| puts e.join(" ")}

when "7.1"

  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_240M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.seg_map(%{|i, b| i.each_slice(1){|*e| b.call e.join(" ")}})
  f = f.seg_split(1)
  f = f.map(%{|w| [w].join(" ")})
  f.output("test/test-pf.vf")
  #  f.here.each{|e| puts e.join(" ")}

when "8"

  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_240M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.seg_map(%{|i, b| i.each_slice(100){|*e| b.call e.join(" ")}})
  f = f.seg_split(1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.map(%{|w| [w].join(" ")})
  f.output("test/test-pf.vf")
  #  f.here.each{|e| puts e.join(" ")}

when "9"

  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_240M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.seg_split(1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.map(%{|w| [w].join(" ")})
  f.output("test/test-pf.vf")
  #  f.here.each{|e| puts e.join(" ")}

when "10"

  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_240M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.map(%{|w| [w].join(" ")})
  f.output("test/test-pf.vf")
  #  f.here.each{|e| puts e.join(" ")}

when "11"

  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_240M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.map(%{|w| [w].join(" ")})
  f.done


end
