
require "job/filter"

module Fairy
  class Inject<Filter

    module Interface
      # filter.inject(%{...}, :init_value = val)
      def inject(block_source, opts = nil)
	inject = Inject.new(@fairy, opts, block_source)
	inject.input = self
	#DeepConnect::future{inject.value}
	inject
      end
    end
    Fairy::def_job_interface Interface

    def initialize(fairy, opts, block_source)
      super
      @block_source = block_source
    end

    def backend_class_name
      "BInject"
    end

    def value
      backend.value
    end
  end
end
