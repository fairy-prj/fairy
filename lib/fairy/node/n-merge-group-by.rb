# encoding: UTF-8

require "fairy/node/n-filter"
require "fairy/node/n-group-by"

module Fairy
  class NMergeGroupBy<NGroupBy
    Processor.def_export self

    def initialize(processor, bjob, opts, block_source)
      super
    end

#     class NPostFilter<NSingleExportFilter
#       Processor.def_export self

#       def initialize(processor, bjob, opts, block_source)
# 	super
# 	@block_source = block_source
#       end

#       def start
# 	basic_start do
# 	  @key_value = {}
# 	  @hash_proc = BBlock.new(@block_source, @context, self)

# 	  @import.each do |e|
# 	    key = key(e)
# 	    @key_value[e] = [] unless @key_value.key?(e)
# 	    @key_value[e].push e
# 	  end
# 	  for key, values in @key_value
# 	    @export.push [key, values]
# 	  end
# 	end
#       end

#       def key(e)
# 	@hash_proc.yield(e)
#       end
#     end
  end
end


