# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

require "fairy/client/basic-group-by"

module Fairy
  class MergeGroupBy<IOFilter

    module Interface
      def merge_group_by(hash_block, opts = nil)
	hash_block = BlockSource.new(hash_block) 
	merge_group_by = MergeGroupBy.new(@fairy, opts, hash_block)
	merge_group_by.input = self
	merge_group_by
      end
    end
    Fairy::def_filter_interface Interface
#    ::Fairy::Def_Post_Initialize{Post_Initialize}

#     Unhandlemethods = [
#       :Post_Mod_Group_By_Filter,
#       :Post_Merge_Group_By_Filter
#     ]
#     Def Self.Post_Initialize
#       For Interface In ::Fairy::Jobinterfaces
# 	For M In Interface.Instance_Methods
# 	  M = M.Intern If M.Kind_Of?(String)
# 	  Next If Unhandlemethods.Include?(M)
	
# 	  M = M.Id2name
# 	  Mergegroupby::Module_Eval %{
#             Def #{M}(*Argv, &Block)
# 	      Post_Merge_Group_By_Filter(@Block_Source, @Opts).#{M}(*Argv, &Block)
# 	    End
#           }
# 	End
#       End
#     End

    def initialize(fairy, opts, block_source)
      super
      @block_source = block_source
    end

    def backend_class_name
      "CMergeGroupBy"
    end

#     class PostFilter<Filter
#       module Interface
# 	def post_merge_group_by_filterr(hash_block, opts = nil)
# 	  post_merge_group_by_filter = PostFilter.new(@fairy, opts, hash_block)
# 	  post_merge_group_by_filter.input = self
# 	  post_merge_group_by_filter
# 	end
# 	Fairy::def_filter_interface Interface
#       end

#       def initialize(fairy, opts, block_source)
# 	super
# 	@block_source = block_source
#       end

#       def backend_class_name
# 	"BMergeGroupBy::BPostFilter"
#       end
#     end
  end
end
