#!/usr/bin/env ruby
# encoding: UTF-8
#
# Configuration File Generator for fairy
#
# Copyright (C) 2011 Rakuten, Inc.
#

require 'pp'
require 'readline'
require 'fileutils'


class FairyConfWizard

  GRP_BY_N_SEG_FACTOR = 4  # This is *heuristic* value.

  def self.splash
         #1234567890123456789012345678901234567890123
    puts "*" * 43
    puts "** Configuration File Generator for fairy"
    puts "**                   (C) 2011 Rakuten, Inc."
    puts "*" * 43
  end

  def initialize
    unless fairy_home = ENV["FAIRY_HOME"]
      fairy_home = "/home/fairy"
    end

    tmpl_path = fairy_home + "/etc/fairy.conf.tmpl"
    if FileTest.file?(tmpl_path) && FileTest.readable?(tmpl_path)
      ask_something(
        "Do you want to use #{tmpl_path} for the template?",
        :acceptable => %w{y n},
        :default => "y"
      ){|input, opt|
        if input.downcase == "y"
          begin
            @tmpl = File.open(tmpl_path, "r")
          rescue => e
            puts e.message
            @tmpl = nil
          end
        end
        true
      }
    end

    unless @tmpl
      ask_something(
        "Where is your template for fairy.conf?"
      ){|input, opt|
        begin
          @tmpl = File.open(input, "r")
          true
        rescue => e
          puts e.message
          false
        end
      }
    end

    @conf_path = fairy_home + "/etc/fairy.conf"
    @master_host = "localhost"
    @master_port = 19999
    @group_by_n_seg = 1
    #@ext_disable = false
    @vf_root = fairy_home + "/Repos"
    @tmp_dir = "/tmp/fairy/tmpbuf"
    @log_file = "/tmp/fairy/log"
    @opt_enable = false
    @prompt = "> "
  end

  def ask_something(question, opt={})
    acceptable = opt[:acceptable]
    default = opt[:default]
    once = opt[:once]
    integer = opt[:integer]

    str = " "
    if acceptable
      str << "[" 
      str << acceptable.map{|it|
        ret = it.downcase
        if default && (ret == default)
          ret.upcase!
        end
        ret
      }.join("/")
      str << "]"
    elsif default
      str << "[%s]" % default
    end

    loop do
      $stdout.write(question)
      $stdout.write(str + "\n")
      $stdout.flush

      input = Readline.readline("> ", true)
      #input.chomp!
      input.rstrip!

      if default && input.empty?
        input = default
      end

      if integer
        begin
          #input = input.to_i
          input = Integer(input)
        rescue
          puts "WRONG number!" % input
          redo
        end
        if (integer == :positive) && (input < 0)
          puts "The number must be POSITIVE!"
          redo
        elsif (integer == :negative) && (input >= 0)
          puts "The number must be NAGATIVE!"
          redo
        end
      end

      if acceptable && !acceptable.map{|it| it.downcase}.include?(input.downcase)
        redo
      end

      if block_given?
        ret = yield(input, opt)
      else
        ret = true
      end

      if ret || once
        return ret
      end
    end
  end

  def ask_test1
    ask_something(
      "Do you like pizza?",
      :acceptable => %w{y n},
      :default => "y"
    ){|input, opt|
      if input.downcase == "y"
        puts "Yes, you do."
      else
        puts "No, you don't."
      end
      true
    }
  end

  def ask_test2
    ask_something(
      "Which kind of food do you like?",
      :default => "pizza"
    ){|input, opt|
      if input.downcase == "sushi"
        puts "Sorry, I don't have any sushi. Tell me another one."
        false
      else
        puts "You like %s." % input
        true
      end
    }
  end

  def ask_conf_path
    ask_something(
      "Which path do you want to write new fairy.conf into?",
      :default => @conf_path
    ){|input, opt|
      input = File.expand_path(input)
      #p input
      if FileTest.exist?(input)
        puts "%s already exists." % input
        ask_something(
          "Do you want to override existing file?",
          :acceptable => %w{y n},
          :default => "n",
          :once => true,
          :super => input
        ){|input, opt|
          if input.downcase == "y"
            @conf_path = opt[:super]
            true
          else
            false
          end
        }
      else
        @conf_path = input
        true
      end
    }
  end

  def ask_master_host
    ask_something(
      "Enter your master server name.",
      :default => @master_host
    ){|input, opt|
      @master_host = input
      true
    }
  end

  def ask_master_port
    ask_something(
      "Which port your master server listen to?",
      :default => @master_port,
      :integer => :positive
    ){|input, opt|
      @master_port = input
      true
    }
  end

  def ask_no_of_nodes
    ask_something(
      "How many CPUs (cores) does your cluster have?",
      :integer => :positive
    ){|input, opt|
      @group_by_n_seg = input * GRP_BY_N_SEG_FACTOR
      true
    }
  end

#  def ask_ext_disable
#    ask_something(
#      "Do you want to use extentions written in C? (Faster, strongly recommended)",
#      :acceptable => %w{y n},
#      :default => "y"
#    ){|input, opt|
#      if input.downcase == "n"
#        @ext_disable = true;
#      end
#      true
#    }
#  end

  def ask_vf_root
    ask_something(
      "Which directory do you want to put data (VFile segments) into?\nYou can embed Ruby code with \#{ ... } style.",
      :default => @vf_root
    ){|input, opt|
      @vf_root = input
      true
    }
  end

  def ask_tmp_dir
    ask_something(
      "Which directory do you want to use for temporary directory?\nYou can embed Ruby code with \#{ ... } style.",
      :default => @tmp_dir
    ){|input, opt|
      @tmp_dir = input
      true
    }
  end

  def ask_log_file
    ask_something(
      "Which path do you want to put master server's log file into?\nYou can embed Ruby code with \#{ ... } style.",
      :default => @log_file
    ){|input, opt|
      @log_file = input
      true
    }
  end

  def ask_opt_enable
    ask_something(
      "Do you want to turn some optimizations on?\nThat makes fairy faster in some situations. But fairy may become unstable",
      :acceptable => %w{y n},
      :default => "n"
    ){|input, opt|
      if input.downcase == "y"
        @opt_enable = true;
      end
      true
    }
  end

  def commit
    #pp self

    conf = File.open(@conf_path + ".part", "w")

    entries = {
      "MASTER_HOST"         => [@master_host, {:skip => 1}],
      "MASTER_PORT"         => [@master_port],
      "GROUP_BY_NO_SEGMENT" => [@group_by_n_seg],
      "VF_ROOT"             => [@vf_root, {:embed_ruby => true}],
      "TMP_DIR"             => [@tmp_dir, {:embed_ruby => true}],
      "LOG_FILE"            => [@log_file, {:embed_ruby => true}],
    }

    if @opt_enable
      entries["GROUP_BY_GROUPING_OPTIMIZE"] = [true];
      entries["SORT_CMP_OPTIMIZE"] = [true];
      entries["BLOCK_USE_STDOUT"] = [false];
    end

    @tmpl.each{|ln|
      ln.chomp!
      conf.puts ln

      entries.each {|name,ary|
        if ln.match(%r{\A\s*##CONF\.#{name}\s*=})
          puts_entry(conf, name, ary[0], ary[1])

          entries.delete(name)
          break
        end
      }
    }

    conf.close
    FileUtils.mv(conf.path, @conf_path)
  end

  def puts_entry(io, name, value, opt)
    opt ||= {}
    
    if value.kind_of?(String) && opt[:embed_ruby]
      strval = %{"#{value.gsub('"', '\\"')}"}
    else
      strval = value.inspect
    end

    io.puts "CONF.#{name} = #{strval}"

    if opt[:skip]
      opt[:skip].times{
        @tmpl.gets
      }
    end
  end

  def run
    puts "Template: #{@tmpl.path}"

    #ask_test1
    #ask_test2
    ask_conf_path
    ask_master_host
    ask_master_port
    ask_no_of_nodes
    #ask_ext_disable
    ask_vf_root
    ask_tmp_dir
    ask_log_file
    ask_opt_enable

    puts "Writing into: #{@conf_path}"
    commit

    puts "done."
  end
end


FairyConfWizard.splash
wzd = FairyConfWizard.new
wzd.run



