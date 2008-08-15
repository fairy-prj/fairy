

module Fairy

  def self.def_job_interface(mod)
    Job.instance_eval{include mod}
  end

  def Fairy.def_job_interface(mod)
    Job.instance_eval{include mod}
  end

  class Job
    def initialize(fairy, opts, *rests)
      @fairy = fairy
      @opts = opts
      @ref = backend_class.new(fairy.controller, opts, *rests)
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
