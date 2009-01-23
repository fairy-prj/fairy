
require "thread"

module Fairy
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

    def output(vfn, opts = nil)
      if !vfn.kind_of?(String) || VFile.vfile?(vfn)
	outputter = FFileOutput.output(@fairy, vfn)
	outputter.input = self
	outputter
      else
	outputter = LFileOutput.output(@fairy, vfn)
	outputter.input = self
	outputter
      end
    end

    def map(block_source, opts = nil)
      raise "ブロックは受け付けられません" if block_given?
      mapper = EachElementMapper.new(@fairy, block_source)
      mapper.input=self
      mapper
    end

    def smap(block_source, opts = nil)
      raise "ブロックは受け付けられません" if block_given?
      mapper = EachSubStreamMapper.new(@fairy, block_source)
      mapper.input=self
      mapper
    end

    def select(block_source, opts = nil)
      raise "ブロックは受け付けられません" if block_given?
      mapper = EachElementSelector.new(@fairy, block_source)
      mapper.input=self
      mapper
    end

    def grep(regexp, opts = nil)
      select %{|e| /#{regexp.source}/ === e}
    end

    def here(opts = nil)
      here = Here.new(@fairy)
      here.input= self
      here
    end

    def group_by(hash_block, opts = nil)
      group_by = GroupBy.new(@fairy, hash_block)
      group_by.input = self
      group_by
    end

    # jpb.zip(opts,...,filter,...,block_source, opts,...)
    def zip(*others)
      block_source = nil
      if others.last.kind_of?(String)
	block_source = others.pop
      end
      others, opts = others.partition{|e| e.kind_of?(Job)}
      if opts.last.kind_of?(Hash)
	h = opts.pop
      else
	h = {}
      end
      opts.each{|e| h[e] = true}

      zip = Zipper.new(@fairy, others, block_source, h)
      zip.input = self
      zip
    end

    def split(n, opts=nil)
      splitter = Splitter.new(@fairy, n, opts)
      splitter.input = self
      splitter
    end

    def shuffle(block_source, opts = nil)
      shuffle = Shuffle.new(@fairy, block_source)
      shuffle.input = self
      shuffle
    end

    def barrier(opts = nil)
      barrier = Barrier.new(@fairy, opts)
      barrier.input = self
      barrier
    end

    def def_pool_variable(vname, value = nil)
      backend.def_pool_variable(vname, value)
    end

  end
end

require "job/ffile"
require "job/ffile-output"
require "job/local-file-output"
require "job/each-element-mapper"
require "job/each-substream-mapper"
require "job/each-element-selector"
require "job/here"
require "job/group-by"
require "job/zipper"
require "job/splitter"
require "job/shuffle"
require "job/barrier"
