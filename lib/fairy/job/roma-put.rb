# encoding: UTF-8

require "fairy/job/filter"
require "fairy/job/each-substream-mapper"

module Fairy
  module RomaPutInterface

    DEFAULT_SPLIT = 100

    def roma_put(ap, base_key, split=DEFAULT_SPLIT, opts={})
      ap = [ap] if ap.kind_of?(String)

      @fairy.def_pool_variable(:__ROMA_PUT_ap, ap)
      @fairy.def_pool_variable(:__ROMA_PUT_base_key, base_key)
      @fairy.def_pool_variable(:__ROMA_PUT_split, split)

      if opts[:nice]
	@fairy.def_pool_variable(:__ROMA_PUT_nice, opts[:nice])
      else
	@fairy.def_pool_variable(:__ROMA_PUT_nice, 0)
      end

      smap(%{|i, block|
          system('renice '+@Pool.__ROMA_PUT_nice.to_s+' '+$$.to_s) unless @Pool.__ROMA_PUT_nice.zero?

          require 'roma/client/rclient'

          @roma = Roma::Client::RomaClient.new(@Pool.__ROMA_PUT_ap.dc_deep_copy)
          @base_key = @Pool.__ROMA_PUT_base_key + $$.to_s + '_' + __id__.to_s
          @split = @Pool.__ROMA_PUT_split

          buf = []
          cnt = 0
          put_cnt = 0
          i.each{|e|
            cnt += 1
            buf << e
            if (cnt % @split == 0)
              key = @base_key + ('%03d' % put_cnt)
              @roma[key] = buf.join(',')
              block.call(key)
              buf.clear
              put_cnt += 1
            end
          }
          if buf.size > 0
            key = @base_key + ('%03d' % put_cnt)
            @roma[key] = buf.join(',')
            block.call(key)
            buf.clear
          end
        })
    end

    Fairy::def_job_interface RomaPutInterface

  end
end
