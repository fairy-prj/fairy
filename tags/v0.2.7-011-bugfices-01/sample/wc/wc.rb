#!/usr/bin/env ruby

require 'fairy'

BASE = File.dirname($0)

fairy = Fairy::Fairy.new("localhost", 19999);

finput = fairy.input(BASE+"/data/fairy.cat")
fmap = finput.smap(%{|i,o|
  i.each{|ln|
    ln.chomp.split.each{|w| o.push(w)}
  }
})
fshuffle = fmap.group_by(%{|w| w.hash % 2})
freduce = fshuffle.smap(%q{|i,o| o.push("#{i.key}\t#{i.size}")})

freduce.output(BASE+"/data/wc-out")

