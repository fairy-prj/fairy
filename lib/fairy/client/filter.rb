# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

module Fairy

  JobInterfaces = []

  def self.def_filter_interface(mod)
    JobInterfaces.push mod
    Job.instance_eval{include mod}
  end

  def Fairy.def_filter_interface(mod)
    ::Fairy.def_filter_interface(mod)
  end

  @PostInitializers = []
  def self.def_post_initialize(&block)
    @PostInitializers.push block
  end

  def self.post_initialize
    @PostInitializers.each{|proc| proc.call}
  end

  class Filter
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
	ERR::Raise ERR::INTERNAL::NoRegisterService, backend_class_name
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
