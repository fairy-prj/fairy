
require "node/n-filter"
require "node/n-group-by"

module Fairy
  class NModGroupBy<NGroupBy
    Processor.def_export self

    def initialize(processor, bjob, opts, block_source)
      super
      
      @mod = CONF.N_MOD_GROUP_BY
    end

    def key(e)
      super.hash % @mod
    end
  end

  class NPreAfterModFilter<NSingleExportFilter
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
	  @key_value[e] = [] unless @key_value.key?(e)
	  @key_value[e].push e
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


