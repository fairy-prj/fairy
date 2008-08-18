
require "node/njob"
require "node/port"
require "node/n-single-exportable"

module Fairy
  class NThere<NJob
    include NSingleExportable


    def initialize(processor, bjob, opts, enumerator)
      super
      @enumerator = enumerator
    end

    def start
      super do
	@enumerator.each{|e| puts "XXXX:#{e}"; @export.push e}
      end
    end
  end
end
