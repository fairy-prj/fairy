# encoding: UTF-8

require "util_ext"

module Fairy
  module HValueGenerator
    module SimpleHash
      def SimpleHash.value(key)
	case key
	when String
	  Niive::Util.simple_hash(key)
	else
	  raise "Yet, Not Implement for non-string key(#{key.inspect})" 
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

