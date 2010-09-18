# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

require "fairy/node/p-io-filter"

require "fairy/share/file-place"

module Fairy
  class PWC<PIOFilter
    Processor.def_export self

    ST_ALL_IMPORTED = :ST_ALL_IMPORTED
    ST_WAIT_EXPORT_FINISH = :ST_WAIT_EXPORT_FINISH
    ST_EXPORT_FINISH = :ST_EXPORT_FINISH

    def PWC.open(processor, bjob, opts, fn)
      nfile = PWC.new(processor, bjob, opts)
      nfile.open(fn)
    end

    def initialize(id, ntask, bjob, opts=nil)
      super
      @file = nil

      @exports = {}
      @exports_queue = Queue.new
      
      @counter = {}

      @mod = opts[:n_mod_group_by] 
      @mod ||= CONF.N_MOD_GROUP_BY

      mod = opts[:hash_module]
      mod ||= CONF.HASH_MODULE
      require mod
      @hash_generator = Fairy::HValueGenerator.new(bjob.hash_seed)

      @hash_optimize = CONF.HASH_OPTIMIZE
      @hash_optimize = opts[:hash_optimize] if opts.key?(:hash_optimize)
    end

    def open(nfileplace)
      @file_name = nfileplace.path
      self.no = nfileplace.no
      begin
	@file = File.open(@file_name)
      rescue 
	e = $!.exception($!.message+ "(vfile entry##{nfileplace.no}: #{nfileplace.url})")
	e.set_backtrace($!.backtrace)

	Log::error_exception(e)
	handle_exception(e)
	raise e
      end
#      start
      self
    end
    DeepConnect::def_method_spec(self, "REF open(VAL)")

    def add_export(key, export)
      @exports[key] = export
#      @exports_queue.push [key, export]
      # [BUG#171]同期処理でないとまずい.
      @bjob.add_exports(key, export, self)
    end

    def start_export
      Log::debug(self, "START_EXPORT")

      start do
	hash_opt = @opts[:hash_optimize]
	hash_opt = CONF.HASH_OPTIMIZE if hash_opt.nil?
	
#	if hash_opt
	  @key_proc = eval("proc{|w| w}", @context.binding)
#	else
#	  @key_proc = BBlock.new("|w| w", @context, self)
#	end
	
	policy = @opts[:postqueuing_policy]
	begin
	  @file.each do |ln|
	    (begin
	      ln.chomp.split
	    rescue
	      []
	    end).each do |e|
	      key = hash_key(e)
	      export = @exports[key]
	      unless export
		export = Export.new(policy)
		export.njob_id = @id
		export.add_key(key)
		add_export(key, export)
		@counter[key] = 0
	      end
	      export.push e
	      @counter[key] += 1
	    end
	  end
	rescue
	  Log::debug_exception(self)
	  raise
	ensure
	  @exports_queue.push nil
	  @exports.each{|key, export| 
	    Log::debug(self, "G0 #{key} => #{@counter[key]}")	    
	    export.push END_OF_STREAM}
	end
      end
    end

    def terminate
      @wait_cv = @terminate_mon.new_cv
      wait_export_finish
      super
    end

    def hash_key(e)
      @hash_generator.value(@key_proc.yield(e)) % @mod
    end


    def wait_export_finish

Log::debug(self, "G1")

      self.status = ST_ALL_IMPORTED

Log::debug(self, "G2")
      # すべての, exportのoutputが設定されるまで待っている
      # かなりイマイチ
#      for key, export in @exports
#Log::debug(self, "G2.key = #{export.key}: WAIT")
#	export.output
#Log::debug(self, "G2.key = #{export.key}: RESUME")
#      end

Log::debug(self, "G3")
      # ここの位置が重要
      self.status = ST_WAIT_EXPORT_FINISH
      # ここもいまいち
Log::debug(self, "G4")
      for key,  export in @exports
Log::debug(self, "G4.WAIT #{key}")
	export.fib_wait_finish(@wait_cv)
      end
Log::debug(self, "G5")
      self.status = ST_EXPORT_FINISH
    end

    class PPostFilter<PIOFilter
      Processor.def_export self
    
      ST_OUTPUT_FINISH = :ST_OUTPUT_FINISH

      def initialize(id, ntask, bjob, opt, vf)
	super
	@vfile = vf

	@buffering_policy = @opts[:buffering_policy]
	@buffering_policy ||= CONF.MOD_GROUP_BY_BUFFERING_POLICY

	@mod = @opts[:n_mod_group_by] 
	@mod ||= CONF.N_MOD_GROUP_BY

	mod = @opts[:hash_module]
	mod ||= CONF.HASH_MODULE
	require mod
	@hash_generator = Fairy::HValueGenerator.new(bjob.hash_seed)

	@hash_optimize = CONF.HASH_OPTIMIZE
	@hash_optimize = @opts[:hash_optimize] if @opts.key?(:hash_optimize)
      end

      def input=(input)
	super
	start
      end

      def hash_key(e)
	@hash_proc.yield(e)
      end

      def basic_start(&block)
	Log::debug(self, "START")
	output_uri = gen_real_file_name
	@vfile.set_real_file(no, output_uri)

	Log::debug(self, "write real file: #{output_uri}")
	begin
	  output_file = URI.parse(output_uri).path
	rescue
	  Log::debug_exception(self)
	  raise
	end

	unless File.exist?(File.dirname(output_file))
	  create_dir(File.dirname(output_file))
	end

	@key_value_buffer = 
	  eval("NModGroupBy::#{@buffering_policy[:buffering_class]}").new(self, @buffering_policy)
#	if @hash_optimize
	  @hash_proc = eval("proc{|w| w}")
#	else
#	  @hash_proc = BBlock.new("|w| w", @context, self)
#	end

	case @key_value_buffer
	when PGroupBy::DirectOnMemoryBuffer
	  @input.each do |e|
	    @key_value_buffer.push(e)
	    e = nil
	  end
	else
	  @input.each do |e|
	    key = hash_key(e)
	    @key_value_buffer.push(key, e)
	    e = nil
	  end
	end

	File.open(output_file, "w") do |io|
	  Log::debug(self, "start write real file: #{output_uri}")
	  @key_value_buffer.each do |key, values|
	    io.puts [key, values.size].join(" ")
	  end
	  @key_value_buffer = nil
	  Log::debug(self, "finish write real file: #{output_uri}")
	end

	self.status = ST_OUTPUT_FINISH
      end

      def create_dir(path)
	unless File.exist?(File.dirname(path))
	  create_dir(File.dirname(path))
	end
	begin
	  Dir.mkdir(path)
	rescue Errno::EEXIST
	  # 無視
	end
      end

      IPADDR_REGEXP = /::ffff:([0-9]+\.){3}[0-9]+|[0-9a-f]+:([0-9a-f]*:)[0-9a-f]*/

      def gen_real_file_name
	host= processor.addr
	root = CONF.VF_ROOT
	prefix = CONF.VF_PREFIX
	base_name = @vfile.base_name
	no = @input.no
	

	if IPADDR_REGEXP =~ host
	  begin
	    host = Resolv.getname(host)
	  rescue
	    # ホスト名が分からない場合 は そのまま ipv6 アドレスにする
	    host = "[#{host}]"
	  end
	end
	
	format("file://#{host}#{root}/#{prefix}/#{base_name}-%03d", no)
      end

    end
  end
end


