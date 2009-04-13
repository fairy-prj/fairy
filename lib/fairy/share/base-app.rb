# encoding: utf-8

module Fairy
  class BaseAPP
    
    def self.start
      @@APP = new
      @@APP.parse_arg
      @@APP.load_conf
      @@APP.start
    end

    def self.start_subcommand(prog, *opts)
      @@APP.start_subcommand(prog, *opts)
    end

    def initialize
      @home = nil
      @conf = nil
      @CONF = {}
    end

    def parse_arg
      opt = option_parser
      opt.order!(ARGV)
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

      ENV["RUBYLIB"] = CONF.LIB + ":" + ENV["RUBYLIB"]
    end

    def start
      raise "サブクラスで定義してください"
    end

    def start_subcommand(prog, *opts)
      opts.push *conf_to_arg
      Process.fork do
	exec(prog, *opts)
      end
    end

  end
end

