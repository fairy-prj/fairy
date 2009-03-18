# encoding: UTF-8

require "digest/md5"

module Fairy
  module HValueGenerator
    module MD5
      def MD5.value(key)
	case key
	when String
	  Digest::MD5.digest("key").unpack("@12N").first
	else
	  raise "Yet, Not Implement for non-string key" 
	end
      end
#      module_function :hash
    end

    def self.create_seed;end
    def self.new(seed)
      HValueGenerator::MD5
    end
  end
end

