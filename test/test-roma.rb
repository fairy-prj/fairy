#!/usr/bin/env ruby

require 'fairy'

fairy = Fairy::Fairy.new
f0 = fairy.roma(
  'fairy-xm01_11211', 
  :format=>[5,7,1], 
  :select=>%{|k,v,t| v.to_i >= 500 && v.to_i < 600}, 
  :map=>%{|k,v,t| [k.sub(/^key-/, ''), v, Time.at(t).strftime('%Y-%m-%d %H:%M:%S')]},
  :nice=>20
)
f1 = f0.map(%{|*ary| ary.join(",")})
f1.here.each{|str| puts str}


