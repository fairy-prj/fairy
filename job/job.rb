# encoding: UTF-8

module Fairy

  JobInterfaces = []
  def self.def_job_interface(mod)
    JobInterfaces.push mod
    Job.instance_eval{include mod}
  end

  def Fairy.def_job_interface(mod)
    ::Fairy.def_job_interface(mod)
  end

  @PostInitializers = []
  def self.def_post_initialize(&block)
    @PostInitializers.push block
  end

  def self.post_initialize
    @PostInitializers.each{|proc| proc.call}
  end

  class Job
    def initialize(fairy, opts, *rests)
      @fairy = fairy
      @opts = opts
      @opts = {} unless @opts
      if @opts[:BEGIN]
	@opts[:BEGIN] = BlockSource.new(@opts[:BEGIN])
      end
      if @opts[:END]
	@opts[:END] = BlockSource.new(@opts[:END])
      end
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
