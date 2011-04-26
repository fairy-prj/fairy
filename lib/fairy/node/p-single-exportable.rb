# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

require "fairy/node/p-io-filter"
 
module Fairy
  module PSingleExportable
    include Enumerable

    END_OF_STREAM = PFilter::END_OF_STREAM

    ST_WAIT_EXPORT_FINISH = :ST_WAIT_EXPORT_FINISH
    ST_EXPORT_FINISH = :ST_EXPORT_FINISH

    def initialize(id, ntask, bjob, opts=nil, *rests)
      super
    end

    attr_reader :export

#    def no=(no)
#      super
#      @export.no = no
#    end

# とりあえず
#     def start(&block)
#       super do
# 	begin
# 	  if @import
# 	    @export.add_key(@import.key)
# 	  end
# 	  block.call
# 	ensure
# 	  @export.push END_OF_STREAM
# 	  wait_export_finish
# 	end
#       end
#     end

    def start_export
      Log::debug(self, "START_EXPORT")

      policy = @opts[:postqueuing_policy]
      @export = Export.new(policy)
      @export.njob_id = @id
      @export.no = @no
      @export.key = @key

      start do
	each{|e| @export.push e}
	@export.push END_OF_STREAM
      end

      @export
    end

    def start(&block)
      super do
 	begin
 	  block.call
 	ensure
# 	  @export.push END_OF_STREAM
 	end
      end
    end

    def terminate
      @wait_cv = @terminate_mon.new_cv
      wait_export_finish
      super
    end

    def wait_export_finish
      self.status = ST_WAIT_EXPORT_FINISH
      @terminate_mon.synchronize do
	@export.fib_wait_finish(@wait_cv)
      end
      self.status = ST_EXPORT_FINISH
    end
  end

  class PSingleExportFilter<PIOFilter
    include PSingleExportable
  end

  class PSingleExportInput<PIOFilter
    include PSingleExportable
  end
end
