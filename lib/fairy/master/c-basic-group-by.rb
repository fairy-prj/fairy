# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

require "fairy/master/c-io-filter"
require "fairy/master/c-inputtable"

module Fairy
  class CBasicGroupBy<CIOFilter
    Controller.def_export self

    def initialize(controller, opts, block_source)
      super
      @block_source = block_source

      @no_of_exports = 0

      # key -> [export, ...]
      @exports = {}
      @exports_mutex = Mutex.new
      @exports_cv = ConditionVariable.new

#      @pre_exports_queue = Queue.new
      @exports_queue = Queue.new

      @each_export_by_thread = nil
      @each_export_by_thread_mutex = Mutex.new
    end

    def start_create_nodes
      super

      start_watch_all_node_imported
    end

#    def each_export(&block)
#      while pair = @exports_queue.pop
#	block.call pair
#      end
#    end

#     def next_filter(mapper)
#       ret = super
#       unless ret
# 	@each_export_by_thread_mutex.synchronize do
# 	  @each_export_by_thread.join if @each_export_by_thread
# 	end
#       end
#       ret 
#     end

    def each_assigned_filter(&block)
      super

      @each_export_by_thread_mutex.synchronize do
	@each_export_by_thread.join if @each_export_by_thread
      end
    end

#     def each_export_by(njob, mapper, &block)
#       return if @each_export_by_thread

#       begin
# 	while pair = @exports_queue.pop
# 	  exp, njob = pair
# 	  Log::debug(self, "EXPORT_BY, #{exp.key}")
# 	  block.call exp
# 	end
#       rescue
# 	Log::fatal_exception
#       end
#       @each_export_by_thread = true
#     end

    def each_export_by(njob, mapper, &block)
      @each_export_by_thread_mutex.synchronize do
	return if @each_export_by_thread

	@each_export_by_thread = Thread.start{
	  # すべての njob がそろうまで待つ
	  # 後段が先にスケジュールされてデッドロックするのを避けるため.
	  number_of_nodes

	  begin
	    while pair = @exports_queue.pop
	      exp, njob = pair
Log::debug(self, "EXPORT_BY, #{exp.key}")
	      block.call exp

	      @exports_mutex.synchronize do
		if @exports[exp.key].first == exp
		  @exports[exp.key][1..-1].each do |e|
		    e.output = exp.output
		  end
		end
	      end
	    end
	  rescue
	    Log::fatal_exception
	    raise
	  end
	}
      end
    end

    def bind_export(exp, imp)
      # do nothing
    end

    def add_exports(key, export, njob)
      @exports_mutex.synchronize do
	if exports = @exports[key]
	  export.output = exports.first.output if exports.first.output?
	  export.no = exports.first.no
	  exports.push export
	else
	  export.no = @no_of_exports
	  @no_of_exports += 1
	  @exports[key] = [export]
	  @exports_queue.push [export, njob]
#	  @pre_exports_queue.push [export, njob]
	end
      end
    end

    def update_exports(key, export, njob)
      add_exports(key, export, njob)
      nil
    end

    def node_class_name
      "PGroupBy"
    end

    def njob_creation_params
      [@block_source]
    end

    def start_watch_all_node_imported
      Thread.start do
	# すべての njob がそろうまで待つ
	# 後段が先にスケジュールされてデッドロックするのを避けるため.
Log::debug(self, "START_WATCH_ALL_NODE_IMPORTED: S")
	number_of_nodes

Log::debug(self, "START_WATCH_ALL_NODE_IMPORTED: 1")


Log::debug(self, "START_WATCH_ALL_NODE_IMPORTED: 2")
	# すべての exports がそろうまで待つ
	@nodes_status_mutex.synchronize do
	  while !all_node_imported?
	    @nodes_status_cv.wait(@nodes_status_mutex)
	  end
	end
	@exports_queue.push nil

Log::debug(self, "START_WATCH_ALL_NODE_IMPORTED: 4")
	for key, exports in @exports
	  exports.first.output_no_import = exports.size
	end
Log::debug(self, "START_WATCH_ALL_NODE_IMPORTED: E")
      end
      nil
    end

    def start_watch_all_node_imported_ORG
      Thread.start do
	# すべての njob がそろうまで待つ
	# 後段が先にスケジュールされてデッドロックするのを避けるため.
Log::debug(self, "START_WATCH_ALL_NODE_IMPORTED: S")
	number_of_nodes

Log::debug(self, "START_WATCH_ALL_NODE_IMPORTED: 1")
	# すでに存在するexportsを下流に送る
	@exports_mutex.synchronize do
Log::debug(self, "START_WATCH_ALL_NODE_IMPORTED: 1.1")
	  @pre_exports_queue.push nil
	  while pair = @pre_exports_queue.pop
Log::debug(self, "START_WATCH_ALL_NODE_IMPORTED: 1.2: EXP.NO: #{pair[0].no}")
	    @exports_queue.push pair
	  end
Log::debug(self, "START_WATCH_ALL_NODE_IMPORTED: 1.3")
	end

Log::debug(self, "START_WATCH_ALL_NODE_IMPORTED: 2")
	# すべての exports がそろうまで待つ
	@nodes_status_mutex.synchronize do
	  while !all_node_imported?
	    @nodes_status_cv.wait(@nodes_status_mutex)
	  end
	end

Log::debug(self, "START_WATCH_ALL_NODE_IMPORTED: 3")
	# 残りのexportsを下流に送る
	@pre_exports_queue.push nil
	while pair = @pre_exports_queue.pop
Log::debug(self, "START_WATCH_ALL_NODE_IMPORTED: 3.1: EXP.NO: #{pair[0].no}")
	  @exports_queue.push pair
	end
	@exports_queue.push nil
	
Log::debug(self, "START_WATCH_ALL_NODE_IMPORTED: 4")
#Log::debug(self, "START: setting for EXPOTRS.SIZE")
	for key, exports in @exports
#	  exports[1..-1].each do |exp|
#	    exp.output=exports.first.output
#	  end

#Log::debug(self, "EXPOTRS.SIZE=#{exports.size}")
	  exports.first.output_no_import = exports.size
	end
#Log::debug(self, "END: setting for EXPOTRS.SIZE")
Log::debug(self, "START_WATCH_ALL_NODE_IMPORTED: E")
      end
      nil
    end

    def all_node_arrived?
      @nodes_mutex.synchronize{@number_of_nodes}
    end

    def all_node_imported?
      # すべてのnjobがそろったか?
      return false unless @nodes_mutex.synchronize{@number_of_nodes}

      each_node(:exist_only) do |node|
	st = @nodes_status[node]
	# こちらはNG: outputが設定されていないとまずい.
	# すべてのnodeがそろったとしてもすべてのexportがそろっているとは限らない
#	unless [:ST_FINISH, :ST_EXPORT_FINISH, :ST_WAIT_EXPORT_FINISH, :ST_ALL_IMPORTED].include?(st)
	unless [:ST_FINISH, :ST_EXPORT_FINISH, :ST_WAIT_EXPORT_FINISH].include?(st)
	  return false
	end
      end
      true
    end
  end

  class CBasicMGroupBy<CBasicGroupBy
    Controller.def_export self

    def node_class_name
      "PBasicMGroupBy"
    end
  end


end
