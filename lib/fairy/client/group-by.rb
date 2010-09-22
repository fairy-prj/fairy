# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

require "fairy/client/basic-group-by"
require "fairy/client/merge-group-by"

module Fairy
  class GroupBy<IOFilter

    module Interface
      def group_by(hash_block, opts = nil)
	hash_block = BlockSource.new(hash_block) 
	mod_group_by = GroupBy.new(@fairy, opts, hash_block)
	mod_group_by.input = self
	mod_group_by
      end
    end
    Fairy::def_filter_interface Interface
    ::Fairy::def_post_initialize{post_initialize}

    UnhandleMethods = [
      :post_mod_group_by_filter,
      :post_merge_group_by_filter
    ]
    def self.post_initialize
      for interface in ::Fairy::FilterInterfaces
	for m in interface.instance_methods
	  m = m.intern if m.kind_of?(String)
	  next if UnhandleMethods.include?(m)
	
	  m = m.id2name
	  GroupBy::module_eval %{
            def #{m}(*argv, &block)
	      post_mod_group_by_filter(@block_source, @opts).#{m}(*argv, &block)
	    end
          }
	end
      end
    end

    def initialize(fairy, opts, block_source)
      super
      @block_source = block_source
    end

    def backend_class_name
      "CGroupBy"
    end

    class PostFilter<IOFilter
      module Interface
	def post_mod_group_by_filter(hash_block, opts = nil)
	  post_mod_group_by_filter = PostFilter.new(@fairy, opts, hash_block)
	  post_mod_group_by_filter.input = self
	  post_mod_group_by_filter
	end
	Fairy::def_filter_interface Interface
      end

      def initialize(fairy, opts, block_source)
	super
	@block_source = block_source
      end

      def backend_class_name
	"CGroupBy::CPostFilter"
      end
    end

#   class PostAfterModFilter<Filter
#     module Interface
#       def post_after_mod_filter(hash_block, opts = nil)
# 	post_after_mod_filter = PostAfterModFilter.new(@fairy, opts, hash_block)
# 	post_after_mod_filter.input = self
# 	post_after_mod_filter
#       end
#     end
#     Fairy::def_filter_interface Interface

#     def initialize(fairy, opts, block_source)
#       super
#       @block_source = block_source
#     end

#     def backend_class_name
#       "BPostAfterModFilter"
#     end
#   end
  
  end
end


Fairy.def_filter(:mod_group_by2) do |fairy, input, block_source, opts = {}|
  my_begin = %{
    require CONF.GROUP_BY_HASH_MODULE
    hash = Fairy::HValueGenerator.new(@Pool.HASH_SEED)
    mod = CONF.GROUP_BY_NO_SEGMENT
  }
  if opts[:BEGIN]
    opts[:BEGIN].cat my_begin
  else
    opts[:BEGIN] = my_begin
  end
  if opts[:postqueuing_policy]
    if !opts[:postqueuing_policy][:sort_by]
      opts[:postqueuing_policy][:sort_by] = block_source
    end
  else
    opts[:postqueuing_policy] = {
      :queuing_class => :SortedQueue, 
      :sort_by => block_source
    }
  end

  pre = input.merge_group_by(%{|e| hash.value(proc{#{block_source}}.call(e)) % mod}, 
			     opts)
  post = pre.seg_map(%{|i, block|
    sort_proc = proc{#{block_source}}

    key = nil
    ary = []
    buf = i.map{|st| [st, st.pop.dc_deep_copy]}.select{|st, v|!v.nil?}.sort_by{|st, v| sort_proc.call(v)}
    while st_min = buf.shift
      st, min = st_min
      if key == sort_proc.call(min)
         ary.push min
      else
         block.call ary unless ary.empty?
         key = sort_proc.call(min)
         ary = [min]
      end
      next unless v = st.pop.dc_deep_copy # 取りあえずの対応
      buf.push [st, v]
      buf = buf.sort_by{|st0, v0| sort_proc.call(v0)}
    end
    if !ary.empty?
      block.call [key, ary]
    end
    })
end

Fairy.def_filter(:mod_group_by3) do |fairy, input, block_source, opts = {}|

  my_begin = %{
    require CONF.GROUP_BY_HASH_MODULE
    hash = Fairy::HValueGenerator.new(@Pool.HASH_SEED)
    mod = CONF.GROUP_BY_NO_SEGMENT
  }
  if opts[:BEGIN]
    opts[:BEGIN].cat my_begin
  else
    opts[:BEGIN] = my_begin
  end
  if opts[:postqueuing_policy]
    if !opts[:postqueuing_policy][:sort_by]
      opts[:postqueuing_policy][:sort_by] = %{|k, v| k}
    end
  else
    opts[:postqueuing_policy] = {
      :queuing_class => :SortedQueue, 
      :sort_by => %{|k, v| k}
    }
  end

  key_pair = input.map(%{|v| [proc{#{block_source}}.call(v), v]})
  pre = key_pair.merge_group_by(%{|k, v| hash.value(k) % mod}, opts)
  post = pre.smap2(%{|i, block|
    key = nil
    ary = []
    buf = i.map{|st| [st, st.pop.dc_deep_copy]}.select{|st, pair|!pair.nil?}.sort_by{|st, pair| pair.first}
    while st_min = buf.shift
      st, min_pair = st_min
      if key == min_pair.first
         ary.push min_pair.last
      else
         block.call [key, ary] unless ary.empty?
         key = min_pair.first
         ary = [min_pair.last]
      end
      next unless v = st.pop.dc_deep_copy # 取りあえずの対応
      buf.push [st, v]
      buf = buf.sort_by{|st0, v0| v0.first}
    end
    if !ary.empty?
      block.call ary
    end
    },
		   :postqueuing_policy => {:queuing_class => :OnMemoryQueue}
)
  
end
