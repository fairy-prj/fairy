# encoding: UTF-8

require "job/group-by"

module Fairy
  class ModGroupBy<Filter

    module Interface
      def mod_group_by(hash_block, opts = nil)
	hash_block = BlockSource.new(hash_block) 
	mod_group_by = ModGroupBy.new(@fairy, opts, hash_block)
	mod_group_by.input = self
	mod_group_by
      end
    end
    Fairy::def_job_interface Interface
    ::Fairy::def_post_initialize{post_initialize}

    UnhandleMethods = [
      :post_mod_group_by_filter,
      :post_merge_group_by_filter
    ]
    def self.post_initialize
      for interface in ::Fairy::JobInterfaces
	for m in interface.instance_methods
	  m = m.intern if m.kind_of?(String)
	  next if UnhandleMethods.include?(m)
	
	  m = m.id2name
	  ModGroupBy::module_eval %{
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
      "BModGroupBy"
    end

    class PostFilter<Filter
      module Interface
	def post_mod_group_by_filter(hash_block, opts = nil)
	  post_mod_group_by_filter = PostFilter.new(@fairy, opts, hash_block)
	  post_mod_group_by_filter.input = self
	  post_mod_group_by_filter
	end
	Fairy::def_job_interface Interface
      end

      def initialize(fairy, opts, block_source)
	super
	@block_source = block_source
      end

      def backend_class_name
	"BModGroupBy::BPostFilter"
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
#     Fairy::def_job_interface Interface

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
