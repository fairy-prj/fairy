# encoding: utf-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

module Fairy
  class BaseAPP
    
    def self.start
      @@APP = new
      @@APP.parse_arg
      @@APP.load_conf
      @@APP.configure_conf
      @@APP.start
    end

    def self.start_subcommand(prog, *opts)
      @@APP.start_subcommand(prog, *opts)
    end
    def self.start_subcommand2(prog, *opts)
      Process.fork do
	start_subcommand(prog, *opts)
      end
    end

    def initialize
      @home = nil
      @conf = nil
      @CONF = {}
    end

    def parse_arg
      @opt = option_parser
      @opt.order!(ARGV)
    end

    def option_parser
      opt = OptionParser.new{|opt|
	opt.on("--home=VAL"){|val| @home = val}
	opt.on("--conf=VAL"){|val| @conf = val}
	CONF.props.each do |prop|
	  opt.on("--CONF.#{prop}=VAL") {|val| @CONF[prop] = val}
	end
      }
      if block_given?
	yield opt
      end
    end

    def conf_to_arg
      @CONF.collect{|prop, source| "--CONF.#{prop}=#{source}"}
    end

    def load_conf
      if @home
	ENV["FAIRY_HOME"] = @home
#	ENV["FAIRY_HOME"] = File.expand_path(@home)
	conf = @home+"/etc/fairy.conf"
	if File.exists?(conf)
	  CONF.load_conf conf
	end
      end
	
      if @conf
	ENV["FAIRY_CONF"] = @conf
	CONF.load_conf @conf
      end

      @CONF.each do |key, value|
	begin
	  eval "CONF.#{key}=#{value}"
	rescue
	  puts "CONF.#{key}=#{value} が不正です."
	  exit 1
	end
      end
      if  ENV["RUBYLIB"]
	ENV["RUBYLIB"] = CONF.LIB + ":" + ENV["RUBYLIB"]
      else
	ENV["RUBYLIB"] = CONF.LIB
      end
    end

    def configure_conf
      Conf.configure_common_conf
    end

    def start
      ERR::Raise ERR::INTERNAL::ShouldDefineSubclass
    end

    def start_subcommand(prog, *opts)
      opts.push *conf_to_arg
#       Process.fork do
# 	Log.stop_export
# 	ObjectSpace.each_object(IO) do |io|
# 	  begin
# 	    if ![0, 1, 2].include?(io.fileno )
# 	      io.close
# 	    end
# 	  rescue
# 	  end
# 	end
# 	exec(prog, *opts)
#       end

      Process.spawn(prog, *opts)
#      system("#{prog} #{opts.join(' ')}&")

    end

  end
end

