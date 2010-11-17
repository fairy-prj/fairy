# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

require "fairy/client/filter"
require "fairy/share/varray"

module Fairy
  class OutputNull<Filter
    module Interface
      # Usage:
      # ... .done
      #
      def done
	output_null = OutputNull.output(@fairy, opts=nil)
	output_null.input = self
	output_null
      end
      alias do done
    end
    Fairy::def_filter_interface Interface
    
    def self.output(fairy, opts)
      output = new(fairy, opts)
      output
    end

    def initialize(fairy, opts=nil)
      super
    end

    def backend_class_name
      "COutputNull"
    end

    def output
      backend.output
    end

    def input=(job)
      @input = job
      backend.input=job.backend
      backend.wait_all_output_finished
    end
  end
end
