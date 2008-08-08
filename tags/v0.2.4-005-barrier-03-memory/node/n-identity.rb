
require "node/njob"
require "node/n-single-exportable"

module Fairy
  class NIdentity<NFilter
    include NSingleExportable

    def initialize(processor, bjob)
      super(processor, bjob)
    end

    def start
      super do
	@bjob.wait_export
	@import.each{|e| @export.push e}
      end
    end
  end

end
