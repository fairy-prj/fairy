#!/usr/bin/env ruby
# encoding: UTF-8
# 
# Copyright (C) 2007-2010 Rakuten, Inc.
# 

require 'rubygems'
require 'fairy'
require 'yaml'

yml_path = File.expand_path(File.dirname(__FILE__) + "/../tools/cap_recipe/cluster.yml")
cluster = YAML.load_file(yml_path)

fairy = Fairy::Fairy.new
input = fairy.exec(cluster['nodes'].map{|n| "file://#{n}/"})
map = input.map(%q{|uri| "#{`hostname -f`.chomp} (#{`hostname -i`.chomp}) is alive."})
map.here.each{|responce| puts responce}


