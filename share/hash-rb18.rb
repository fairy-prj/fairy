require "digest/md5"

module Fairy
  module HashGenerator
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

