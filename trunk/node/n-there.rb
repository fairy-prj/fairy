
require "node/njob"
require "node/port"
require "node/n-single-exportable"

module Fairy
  class NThere<NJob
    include NSingleExportable


    def initialize(processor, bjob, opts, enumerable)
      super
      @enumerable = enumerable
    end

    def start
      super do
	@enumerable.each{|e| @export.push e}
      end
    end
  end
end
