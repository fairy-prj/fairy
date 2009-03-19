# encoding: UTF-8

require "fairy/job/filter"

module Fairy

  class Junction<Filter
    module Interface
      def junction(opts = nil, &block)
	subfairy = Fairy.create_subfairy(@fairy)
	junction_ps = Junction.new(subfairy, opts)
	junction_ps.input = self

	last_filter = yield subfairy, junction_ps
	
	junction_sp = Junction.new(@fairy, opts)
	junction_sp.input = last_filter
	junction_sp
      end
      alias sub junction
    end
    Fairy::def_job_interface Interface

    def backend_class_name
      "BJunction"
    end
  end
end
