
require "thread"

module Fairy

  def def_job_interface(mod)
    Job.instance_eval{include mod}
  end
  module_function :def_job_interface

  class Job
    def initialize(fairy, *opts)
      @fairy = fairy
      @ref = backend_class.new(fairy.controller, *opts)
    end

    def backend_class
      unless klass = @fairy.name2backend_class(backend_class_name)
	raise "バックエンドクラス#{backend_class_name}が分かりません"
      end
      klass
    end

    def backend
      @ref
    end

    def backend=(v)
      @ref=v
    end

    def def_pool_variable(vname, value = nil)
      backend.def_pool_variable(vname, value)
    end

  end
end

require "job/ffile"
require "job/output"
require "job/each-element-mapper"
require "job/each-substream-mapper"
require "job/each-element-selector"
require "job/here"
require "job/group-by"
require "job/zipper"
require "job/splitter"
require "job/shuffle"
require "job/barrier"
