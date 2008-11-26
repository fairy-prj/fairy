
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
      :pre_after_mod,
      :post_after_mod
    ]
    def self.post_initialize
      for interface in ::Fairy::JobInterfaces
	for m in Filter.instance_methods
	  m = m.intern if m.kind_of?(String)
	  next UnhandleMethods.include?(m)
	
	  m = m.id2name
	  module_eval %q{
            def #{m}(*argv, &block)
	      pre_after_mod_filter(@opts).#{m}(*argv, &block).post_after_mod_filter(@opts)
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
  end

  class PreAfterModFilter<Filter
    module Interface
      def pre_after_mod_filter(opts = nil)
	pre_after_mod_filter = AfterModFilter.new(@fairy, opts)
	pre_after_mod_filter.input = self
	pre_after_mod_filter
      end
      Fairy::def_job_interface Interface
    end

    def backend_class_name
      "BPreAfterModFilter"
    end
  end

  class PostAfterModFilter<Filter
    module Interface
      def post_after_mod_filter(opts = nil)
	post_after_mod_filter = AfterModFilter.new(@fairy, opts)
	post_after_mod_filter.input = self
	post_after_mod_filter
      end
    end
    Fairy::def_job_interface Interface

    def backend_class_name
      "BPostAfterModFilter"
    end
  end
  
end
