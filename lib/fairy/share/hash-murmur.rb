# encoding: UTF-8

module Fairy

  module HValueGenerator

    # mhash is modiftied from MurmerHash(http://murmurhash.googlepages.com/).
    class MurMur
      FIX_MASK = 0x3fff_ffff
      MASK24 = 0xffff_ffff_ffff
      MAGIC = 0x7fd652ad & FIX_MASK
      R = 18

      def initialize(h)
	@seed = h + 0xdeadbeef & FIX_MASK
#	@postfix = [@seed].pack("N")[1..3]
	@postfix = "000"
      end

      def value(data)
	case data
	when String
	  str_hash(data)
	else
	  ERR::Raise ERR::NoImpliment, "non-string key(#{data.inspect})"
	end
      end

      def str_hash(data)
	h = @seed

	for k in (data+@postfix).unpack("N*")
	  k *= MAGIC
	  k ^= k >> R
	  k &= FIX_MASK
	  k *= MAGIC
	  k &= FIX_MASK

	  h = @seed * MAGIC
	  k &= FIX_MASK
	  h ^= k
	end

	h ^= h >> 13
	h *= MAGIC
	h &= FIX_MASK
	h ^= h >> 15
	h
      end

      def MurMur.create_seed
	rand(FIX_MASK)
      end
    end


    def self.create_seed
      MurMur.create_seed
    end

    def self.new(seed)
      MurMur.new(seed)
    end

  end
end
