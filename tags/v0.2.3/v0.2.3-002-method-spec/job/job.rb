
require "thread"

module Fairy
  class Job
    def initialize(fairy, *opts)
      @fairy = fairy
#      opts = opt2backend(opts)
#      atom = Atom.new(backend_class, :new, fairy.backend_controller, *opts)
#      p atom
#      @ref = @fairy.send_atom(atom)
      @ref = backend_class.new(fairy.controller, *opts)
    end

#     def opt2backend(opts)
#       opts.collect do |e| 
# 	case e
# 	when Job
# 	  e.backend
# 	when Array
# 	  opt2backend(e)
# 	else
# 	  e
# 	end
#       end
#     end

    def backend_class
      unless klass = @fairy.name2backend_class(backend_class_name)
	raise "バックエンドクラス#{backend_class_name}が分かりません"
      end
      klass
    end

    def backend
#      @ref.value
      @ref
    end

    def backend=(v)
#      @ref.value= v
      @ref=v
    end

    def map(block_source)
      raise "ブロックは受け付けられません" if block_given?
      mapper = EachElementMapper.new(@fairy, block_source)
      mapper.input=self
      mapper
    end

    def smap(block_source)
      raise "ブロックは受け付けられません" if block_given?
      mapper = EachSubStreamMapper.new(@fairy, block_source)
      mapper.input=self
      mapper
    end

    def select(block_source)
      raise "ブロックは受け付けられません" if block_given?
      mapper = EachElementSelector.new(@fairy, block_source)
      mapper.input=self
      mapper
    end

    def grep(regexp)
      select %{|e| /#{regexp.source}/ === e}
    end

    def here
      here = Here.new(@fairy)
      here.input= self
      here
    end

    def group_by(hash_block)
      group_by = GroupBy.new(@fairy, hash_block)
      group_by.input = self
      group_by
    end

    # jpb.zip(opts,...,filter,...,block_source)
    def zip(*others)
      block_source = nil
      if others.last.kind_of?(String)
	block_source = others.pop
      end
      others, opts = others.partition{|e| e.kind_of?(Job)}
      zip = Zipper.new(@fairy, opts, others, block_source)
      zip.input = self
      zip
    end
  end
end

require "job/each-element-mapper"
require "job/each-substream-mapper"
require "job/each-element-selector"
require "job/here"
require "job/group-by"
require "job/zipper"
