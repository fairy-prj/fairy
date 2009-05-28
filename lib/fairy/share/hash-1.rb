# encoding: UTF-8

module Fairy
  module HValueGenerator
    module Hash1
      def Hash1.value(key)
	1
      end
    end

    def self.create_seed;end
    def self.new(seed)
      Hash1
    end
  end
end

