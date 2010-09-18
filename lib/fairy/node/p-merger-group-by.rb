# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

require "fairy/node/p-io-filter"
require "fairy/node/p-basic-group-by"

module Fairy
  class PMergeGroupBy<PBasicGroupBy
    Processor.def_export self

    def initialize(id, ntask, bjob, opts, block_source)
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


