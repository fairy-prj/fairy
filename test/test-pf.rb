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

when "2.1"

  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_240M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.seg_split(1,
		  :postqueuing_policy => {
		    :queuing_class => :FileMarshaledQueue,
		    :min_chunk_no => 1000000})
  f = f.map(%{|w| [w].join(" ")})
  f.output("test/test-pf.vf")
  #  f.here.each{|e| puts e.join(" ")}

when "2.2"

  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_240M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
  f = f.seg_split(1,
		  :postqueuing_policy => {
		    :queuing_class => :FileMarshaledQueue,
		    :transfar_marshal_string_array_optimize => true})
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

when "12"

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
		 :postqueuing_policy => {
		   :queuing_class => :XMarshaledQueue,
		   :chunk_size => 10000,
		   :log_mstore => true,},
		 :postfilter_prequeuing_policy => {
		   :queuing_class => :XMarshaledQueue,
		   :chunk_size => 10000,
		   :log_mstore => true,},)
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  f.output("test/test-pf.vf")
  #  f.here.each{|e| puts e.join(" ")}

when "13"

  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_240M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*1)
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
		    :chunk_size => 10000,},
		  :postfilter_prequeuing_policy => {
		    :queuing_class => :XMarshaledQueue,
		    :chunk_size => 10000,},)
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  f.output("test/test-pf.vf")
  #  f.here.each{|e| puts e.join(" ")}


when "14"

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
		 :group_by => :XGroupBy,
		 :no_segment => 1,
		 :postqueuing_policy => {
		   :queuing_class => :XMarshaledQueue,
		   :chunk_size => 10000,},
		 :buffering_policy => {
		   :buffering_class =>  :XDirectMergeSortBuffer,
		   :threshold => 400_000,
		   :CHUNCK_SIZE => 1000},
		 :postfilter_prequeuing_policy => {
		   :queuing_class => :XMarshaledQueue,
		   :chunk_size => 10000,},)
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  f.output("test/test-pf.vf")
  #  f.here.each{|e| puts e.join(" ")}

when "15.GFD"

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
		 :group_by => :GroupBy,
		 :no_segment => 1,
		 :postqueuing_policy => {
		   :queuing_class => :FileMarshaledQueue,
		   :chunk_size => 10000,},
		 :buffering_policy => {
		   :buffering_class =>  :DirectMergeSortBuffer,
		   :threshold => 400_000,
		   :CHUNCK_SIZE => 1000},
		 :postfilter_prequeuing_policy => {
		   :queuing_class => :FileMarshaledQueue,
		   :chunk_size => 10000,},)
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  f.output("test/test-pf.vf")
  #  f.here.each{|e| puts e.join(" ")}

when "15.GXD"

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
		 :group_by => :GroupBy,
		 :no_segment => 1,
		 :postqueuing_policy => {
		   :queuing_class => :XMarshaledQueue,
		   :chunk_size => 10000,},
		 :buffering_policy => {
		   :buffering_class =>  :DirectMergeSortBuffer,
		   :threshold => 400_000,
		   :CHUNCK_SIZE => 1000},
		 :postfilter_prequeuing_policy => {
		   :queuing_class => :XMarshaledQueue,
		   :chunk_size => 10000,},)
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  f.output("test/test-pf.vf")
  #  f.here.each{|e| puts e.join(" ")}

when "15.XXD"

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
		 :group_by => :XGroupBy,
		 :no_segment => 1,
		 :postqueuing_policy => {
		   :queuing_class => :XMarshaledQueue,
		   :chunk_size => 10000,},
		 :buffering_policy => {
		   :buffering_class =>  :DirectMergeSortBuffer,
		   :threshold => 400_000,
		   :CHUNCK_SIZE => 1000},
		 :postfilter_prequeuing_policy => {
		   :queuing_class => :XMarshaledQueue,
		   :chunk_size => 10000,},)
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  f.output("test/test-pf.vf")
  #  f.here.each{|e| puts e.join(" ")}


when "15.XXX"

#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_960M.txt"]*1)
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
		 :group_by => :XGroupBy,
		 :no_segment => 1,
		 :postqueuing_policy => {
		   :queuing_class => :XMarshaledQueue,
		   :chunk_size => 10000,},
		 :buffering_policy => {
		   :buffering_class =>  :XDirectMergeSortBuffer,
		   :threshold => 400_000,
		   :CHUNCK_SIZE => 1000},
		 :postfilter_prequeuing_policy => {
		   :queuing_class => :XMarshaledQueue,
		   :chunk_size => 10000,},)
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  f.output("test/test-pf.vf")
  #  f.here.each{|e| puts e.join(" ")}

when "15.GFX"

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
		 :group_by => :GroupBy,
		 :no_segment => 1,
		 :postqueuing_policy => {
		   :queuing_class => :FileMarshaledQueue,
		   :chunk_size => 10000,},
		 :buffering_policy => {
		   :buffering_class =>  :XDirectMergeSortBuffer,
		   :threshold => 400_000,
		   :CHUNCK_SIZE => 1000},
		 :postfilter_prequeuing_policy => {
		   :queuing_class => :FileMarshaledQueue,
		   :chunk_size => 10000,},)
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  f.output("test/test-pf.vf")
  #  f.here.each{|e| puts e.join(" ")}

when "15.GXX"

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
		 :group_by => :GroupBy,
		 :no_segment => 1,
		 :postqueuing_policy => {
		   :queuing_class => :XMarshaledQueue,
		   :chunk_size => 10000,},
		 :buffering_policy => {
		   :buffering_class =>  :XDirectMergeSortBuffer,
		   :threshold => 400_000,
		   :CHUNCK_SIZE => 1000},
		 :postfilter_prequeuing_policy => {
		   :queuing_class => :XMarshaledQueue,
		   :chunk_size => 10000,},)
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  f.output("test/test-pf.vf")
  #  f.here.each{|e| puts e.join(" ")}

when "15.XFX"

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
		 :group_by => :XGroupBy,
		 :no_segment => 1,
		 :postqueuing_policy => {
		   :queuing_class => :FileMarshaledQueue,
		   :chunk_size => 10000,},
		 :buffering_policy => {
		   :buffering_class =>  :XDirectMergeSortBuffer,
		   :threshold => 400_000,
		   :CHUNCK_SIZE => 1000},
		 :postfilter_prequeuing_policy => {
		   :queuing_class => :FileMarshaledQueue,
		   :chunk_size => 10000,},)
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  f.output("test/test-pf.vf")
  #  f.here.each{|e| puts e.join(" ")}

when "15.XFD"

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
		 :group_by => :XGroupBy,
		 :no_segment => 1,
		 :postqueuing_policy => {
		   :queuing_class => :FileMarshaledQueue,
		   :chunk_size => 10000,},
		 :buffering_policy => {
		   :buffering_class =>  :DirectMergeSortBuffer,
		   :threshold => 400_000,
		   :CHUNCK_SIZE => 1000},
		 :postfilter_prequeuing_policy => {
		   :queuing_class => :FileMarshaledQueue,
		   :chunk_size => 10000,},)
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  f.output("test/test-pf.vf")
  #  f.here.each{|e| puts e.join(" ")}

when "15.XXoX"

#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_960M.txt"]*1)
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
		 :group_by => :XGroupBy,
		 :no_segment => 1,
		 :postqueuing_policy => {
		   :queuing_class => :XMarshaledQueue,
		   :use_string_buffer => false,
		   :chunk_size => 10000,},
		 :buffering_policy => {
		   :buffering_class =>  :XDirectMergeSortBuffer,
		   :threshold => 400_000,
		   :CHUNCK_SIZE => 1000},
		 :postfilter_prequeuing_policy => {
		   :queuing_class => :XMarshaledQueue,
		   :use_string_buffer => false,
		   :chunk_size => 10000,},)
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  f.output("test/test-pf.vf")
  #  f.here.each{|e| puts e.join(" ")}

when "16.GFD"

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
		 :group_by => :GroupBy,
		 :no_segment => 1,
		 :postqueuing_policy => {
		   :queuing_class => :FileMarshaledQueue,
		   :chunk_size => 10000,},
		 :buffering_policy => {
		   :buffering_class =>  :DirectMergeSortBuffer,
		   :threshold => 400_000,
		   :CHUNCK_SIZE => 1000},
		 :postfilter_prequeuing_policy => {
		   :queuing_class => :FileMarshaledQueue,
		   :chunk_size => 10000,},)
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  f.output("test/test-pf.vf")
  #  f.here.each{|e| puts e.join(" ")}
when "16.XXX"

#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_960M.txt"]*1)
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
		 :group_by => :XGroupBy,
		 :no_segment => 1,
		 :postqueuing_policy => {
		   :queuing_class => :XMarshaledQueue,
		   :chunk_size => 10000,},
		 :buffering_policy => {
		   :buffering_class =>  :XDirectMergeSortBuffer,
		   :threshold => 400_000,
		   :CHUNCK_SIZE => 1000},
		 :postfilter_prequeuing_policy => {
		   :queuing_class => :XMarshaledQueue,
		   :chunk_size => 10000,},)
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  f.output("test/test-pf.vf")
  #  f.here.each{|e| puts e.join(" ")}

when "16.XXXs"

#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_960M.txt"]*1)
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
		 :group_by => :XGroupBy,
		 :no_segment => 1,
		 :postqueuing_policy => {
		   :queuing_class => :XMarshaledQueue,
		   :chunk_size => 10000,},
		 :buffering_policy => {
		   :buffering_class =>  :XDirectMergeSortBuffer,
		   :threshold => 400_000,
		   :CHUNCK_SIZE => 1000},
		 :postfilter_prequeuing_policy => {
		   :queuing_class => :XSizedQueue,
		   :chunk_size => 10000,},)
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  f.output("test/test-pf.vf")
  #  f.here.each{|e| puts e.join(" ")}

when "17.FD"
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_960M.txt"]*1)
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_240M.txt"]*1)
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
	      :buffering_class => "PGroupBy::DirectMergeSortBuffer",
	      :threshold => 1_600_000,
	      :chunk_size => 20000}).output("test/test-pf.vf")

when "17.XD"
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_960M.txt"]*1)
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_240M.txt"]*1)
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
	      :queuing_class => :XMarshaledQueue,
	      :min_chunk_no => 20_000},
	    :postfilter_prequeuing_policy => {:queuing_class => :XMarshaledQueue},
	    :buffering_policy => {
	      :buffering_class => "PGroupBy::DirectMergeSortBuffer",
	      :threshold => 1_600_000,
	      :chunk_size => 20000}).output("test/test-pf.vf")

when "17.XX"
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_960M.txt"]*1)
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_240M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
# f = f.sort_by(%{|l| l}, 
# 	    :no_segment => 1,
# 	    :postqueuing_policy => {
# 	      :queuing_class => :XMarshaledQueue,
# 	      :min_chunk_no => 20_000},
# 	    :postfilter_prequeuing_policy => {:queuing_class => :XMarshaledQueue},
# 	    :buffering_policy => {
# 	      :buffering_class => "PGroupBy::XDirectMergeSortBuffer",
# 	      :threshold => 1_600_000,
# 	      :chunk_size => 20000}).output("test/test-pf.vf")

  f = f.sort_by(%{|l| l}, :no_segment => 1)
  f.output("test/test-pf.vf")

when "17.XsX"
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_960M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_240M.txt"]*1)
  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_10M.txt"]*1)
#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/fairy.cat"]*1)
  f = f.mapf(%{|ln| begin
                      ln.chomp.split
		    rescue
		      []
		    end
  })
# f = f.sort_by(%{|l| l}, 
# 	    :no_segment => 1,
# 	    :postqueuing_policy => {
# 	      :queuing_class => :XMarshaledQueue,
# 	      :min_chunk_no => 20_000},
# 	    :postfilter_prequeuing_policy => {:queuing_class => :XMarshaledQueue},
# 	    :buffering_policy => {
# 	      :buffering_class => "PGroupBy::XDirectMergeSortBuffer",
# 	      :threshold => 1_600_000,
# 	      :chunk_size => 20000}).output("test/test-pf.vf")

  f = f.sort_by(%{|l| l}, :no_segment => 1)
  f.output("test/test-pf.vf")

when "18.GFD"

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
		 :group_by => :GroupBy,
		 :no_segment => 1,
		 :postqueuing_policy => {
		   :queuing_class => :FileMarshaledQueue,
		   :chunk_size => 100000,},
		 :buffering_policy => {
		   :buffering_class =>  :DirectMergeSortBuffer,
		   :threshold => 400_000,
		   :CHUNCK_SIZE => 1000},
		 :postfilter_prequeuing_policy => {
		   :queuing_class => :FileMarshaledQueue,
		   :chunk_size => 100000,},)
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  f.output("test/test-pf.vf")
  #  f.here.each{|e| puts e.join(" ")}

when "18.XXX"

#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_960M.txt"]*1)
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
		 :group_by => :XGroupBy,
		 :no_segment => 1,
		 :postqueuing_policy => {
		   :queuing_class => :XMarshaledQueue,
		   :chunk_size => 100000,},
		 :buffering_policy => {
		   :buffering_class =>  :XDirectMergeSortBuffer,
		   :threshold => 400_000,
		   :CHUNCK_SIZE => 1000},
		 :postfilter_prequeuing_policy => {
		   :queuing_class => :XMarshaledQueue,
		   :chunk_size => 100000,},)
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  f.output("test/test-pf.vf")
  #  f.here.each{|e| puts e.join(" ")}

when "18.XXXs"

#  f = fairy.input(["file://emperor//home/keiju/public/a.research/fairy/git/fairy/sample/wc/data/sample_960M.txt"]*1)
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
		 :group_by => :XGroupBy,
		 :no_segment => 1,
		 :postqueuing_policy => {
		   :queuing_class => :XMarshaledQueue,
		   :chunk_size => 100000,},
		 :buffering_policy => {
		   :buffering_class =>  :XDirectMergeSortBuffer,
		   :threshold => 400_000,
		   :CHUNCK_SIZE => 1000},
		 :postfilter_prequeuing_policy => {
		   :queuing_class => :XSizedQueue,
		   :queues_limit => 1000,
		   :chunk_size => 10000,},)
  f = f.map(%{|values| [values.key, values.size].join(" ")})
  f.output("test/test-pf.vf")
  #  f.here.each{|e| puts e.join(" ")}

end
