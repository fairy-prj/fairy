# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

module Fairy
  module HValueGenerator
    module RB18
      def RB18.value(key)
	key.hash
      end
    end

    def self.create_seed;end
    def self.new(seed)
      RB18
    end
  end
end

