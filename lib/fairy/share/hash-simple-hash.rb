# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

#require "simple_hash"

module Fairy
  module HValueGenerator
    module SimpleHash
      def SimpleHash.value(key)
	case key
	when String
	  Fairy::SimpleHash.hash(key)
	else
	  ERR::Raise ERR::NoImpliment, "non-string key(#{key.inspect})"
	end
      end
#      module_function :hash
    end

    def self.create_seed;end
    def self.new(seed)
      SimpleHash
    end
  end
end

