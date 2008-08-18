
require "node/njob"
require "node/n-single-exportable"

module Fairy
  class NIdentity<NFilter
    include NSingleExportable

    def initialize(processor, bjob, opts=nil)
      super
    end

    def start
      super do
	@import.each{|e| @export.push e}
      end
    end
  end

end
