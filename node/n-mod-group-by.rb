# encoding: UTF-8

require "node/n-filter"
require "node/n-group-by"

module Fairy
  class NModGroupBy<NGroupBy
    Processor.def_export self

    def initialize(processor, bjob, opts, block_source)
      super
      
      @mod = CONF.N_MOD_GROUP_BY

      mod = CONF.HASH_MODULE
      require mod
      @hash_generator = Fairy::HValueGenerator.new(bjob.hash_seed)
    end

    def key(e)
      @hash_generator.value(super) % @mod
    end

    class NPostFilter<NSingleExportFilter
      Processor.def_export self

      def initialize(processor, bjob, opts, block_source)
	super
	@block_source = block_source
      end

      def start
	super do
	  @key_value = {}
	  @hash_proc = BBlock.new(@block_source, @context, self)

	  @import.each do |e|
	    key = key(e)
	    @key_value[key] = [] unless @key_value.key?(key)
	    @key_value[key].push e
	  end
	  for key, values in @key_value
	    #Log::debug(self, key)
	    @export.push [key, values]
	  end
	end
      end

      def key(e)
	@hash_proc.yield(e)
      end
    end

#   class NPostAfterModFilter<NSingleExportFilter
#     Processor.def_export self

#     def initialize(processor, bjob, opts, block_source)
#       super
#       @block_source = block_source
#     end

#     def start
#       super do
# 	@import.each{|e| @export.push e}
#       end
#     end

#   end

  end
end


