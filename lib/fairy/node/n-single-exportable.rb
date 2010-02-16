# encoding: UTF-8

require "fairy/node/n-filter"
 
module Fairy
  module NSingleExportable
    include Enumerable

    END_OF_STREAM = NJob::END_OF_STREAM

    ST_WAIT_EXPORT_FINISH = :ST_WAIT_EXPORT_FINISH
    ST_EXPORT_FINISH = :ST_EXPORT_FINISH

    def initialize(processor, bjob, opts=nil, *rests)
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

    def terminate(mon)
      @wait_cv = mon.new_cv

      wait_export_finish
      super
    end

    def wait_export_finish
      self.status = ST_WAIT_EXPORT_FINISH
      @export.fib_wait_finish(@wait_cv)
      self.status = ST_EXPORT_FINISH
    end
  end

  class NSingleExportFilter<NFilter
    include NSingleExportable
  end

  class NSingleExportInput<NFilter
    include NSingleExportable
  end
end
