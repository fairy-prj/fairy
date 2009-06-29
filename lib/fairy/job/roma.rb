# encoding: UTF-8


module Fairy
  module RomaInterface
    DUMP_KEY_PREFIX = '__dump_by_fairy__'
    DEFAULT_HASH_NAME = 'roma'
    DEFAULT_FORMAT    = [5, 7]

    attr_reader :dump_id, :dump_key

    #
    # input data from ROMA
    #
    # ex) 
    #   require 'fairy'
    #  
    #   fairy = Fairy::Fairy.new
    #   f0 = fairy.roma(
    #     'fairy-xm01_11211',
    #     :format=>[5,7,1],
    #     :select=>%{|k,v,t| v.to_i >= 500 && v.to_i < 600},
    #     :map=>%{|k,v,t| [k.sub(/^key-/, ''), v, v.to_i, Time.at(t).strftime('%Y-%m-%d %H:%M:%S')]},
    #     :nice=>20
    #   )
    #   f1 = f0.map(%{|*ary| ary.join(",")})
    #   f1.output('./roma_test.out')
    #
    # available options are:
    #   :hash     name of destination hash. '*' means all hashes (defalut='roma')
    #   :format   fields of CSV. specify by Array (default=[5, 7])
    #             available fields:
    #               0: virtual node ID
    #               1: last modified time (UNIX timestamp)
    #               2: logical clock
    #               3: expire time
    #               4: key length
    #               5: key
    #               6: value length
    #               7: value
    #   :select   block source to select particular records
    #   :map      block source to apply to each records
    #   :nice     priority of process (LOW 20 <--> -20 HIGH) 
    #
    def roma(addr_port, opts={})
      addr_port = [addr_port] if addr_port.kind_of?(String)

      require 'roma/client/export/rdump'

      @@roma_dump_serial_no ||= 0
      @@roma_dump_serial_no += 1
      @dump_id = "#{`hostname`.chomp}_#{Time.now.strftime("%y%m%d%H%M%S")}_#{$$}_#{@@roma_dump_serial_no}"
      @dump_key = DUMP_KEY_PREFIX+dump_id
      dumpinfo = Roma::Client::Export::RomaDump.dump(dump_key, dump_id, addr_port)

      ##############################################
      # structure of dumpinfo
      # {
      #   "hostname1_port1" => "STARTED path"
      #   "hostname1_port2" => "STARTED path"
      #   "hostname2_port1" => "STARTED path"
      #   ...
      # }
      ##############################################

      uris = []
      dumpinfo.each{|host_port, stat_path|
        m = host_port.match(/\A(.*)_\d+\z/)
        host = m[1]
        m2 = stat_path.match(/\A\S+\s+(.*)\z/)
        path = m2[1]
        uris << 'file://'+host+path
      }

      f0 = exec(uris)

      if opts[:hash]
        self.def_pool_variable(:dest_hash, opts[:hash])
      else
        self.def_pool_variable(:dest_hash, DEFAULT_HASH_NAME)
      end

      if opts[:nice]
        self.def_pool_variable(:nice, opts[:nice])
      else
        self.def_pool_variable(:nice, 0)
      end

      f1 = f0.mapf(%q{|uri|
        dumpfiles = Dir.glob(URI(uri).path+"/#{@dest_hash}/*.dump")
        dumpfiles
      }, :BEGIN => %q{
        require 'uri'
        @dest_hash = @Pool.dest_hash
        system("renice #{@Pool.nice} #{$$}") unless @Pool.nice.zero?
      })

      if opts[:format]
        self.def_pool_variable(:format, opts[:format])
      else
        self.def_pool_variable(:format, DEFAULT_FORMAT)
      end

      if opts[:select]
        src = add_proc_src(opts[:select])
        eval(src)  # syntax check
        self.def_pool_variable(:select, src)
      else
        self.def_pool_variable(:select, '')
      end

      if opts[:map]
        src = add_proc_src(opts[:map])
        eval(src)  # syntax check
        self.def_pool_variable(:map, src)
      else
        self.def_pool_variable(:map, '')
      end

      f2 = f1.mapf(%q{|dumpf|
        records = []
        File.open(dumpf, 'rb'){|io|
          Roma::Client::Export::RomaDump.load(io) {|*rec|
            rec_formatted = @format.inject([]){|res, idx| res << rec[idx]}
            next if @select && !@select.call(*rec_formatted)
            if @map
              records << @map.call(rec_formatted)
            else
              records << rec_formatted
            end
          }
        }
        records
      }, :N => 1, :BEGIN => %q{
        require 'roma/client/export/rdump'
        @format = @Pool.format.dc_deep_copy
        @select = eval(@Pool.select) unless @Pool.select.empty?
        @map = eval(@Pool.map) unless @Pool.map.empty?
      })

      f2  
    end

    private
    def add_proc_src(src)
      if src =~ /\A{.*}\z/
        'Proc.new'+src
      else
        'Proc.new{'+src+'}'
      end
    end
  end
  def_fairy_interface RomaInterface
end


