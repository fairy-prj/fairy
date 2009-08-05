# encoding: UTF-8

require "fairy/node/njob"
require "fairy/node/port"
require "fairy/node/n-single-exportable"

module Fairy
  class NThere<NJob
    Processor.def_export self

    include NSingleExportable

    def initialize(processor, bjob, opts)
      super
    end

    def open(nthere_place)
      self.no = nthere_place.no
      @enumerable = nthere_place.enumerable
    end

    def basic_each(&block)
      @enumerable.each do |e| 
	if e.__deep_connect_reference? && !PORT_KEEP_IDENTITY_CLASS_SET[e.class]
	  e = e.deep_connect_deep_copy
	end
	block.call  e
      end
    end

#     def start
#       super do
# 	@enumerable.each do |e| 
# 	  if e.__deep_connect_reference? && !PORT_KEEP_IDENTITY_CLASS_SET[e.class]
# 	    e = e.deep_connect_deep_copy
# 	  end
# 	  @export.push e
# 	end
#       end
#     end
  end
end
