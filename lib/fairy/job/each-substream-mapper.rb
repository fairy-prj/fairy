# encoding: UTF-8

require "fairy/job/filter"

module Fairy

  class EachSubStreamMapper<Filter
    module Interface
      def smap(block_source, opts = nil)
	ERR::Raise ERR::CantAcceptBlock if block_given?
	block_source = BlockSource.new(block_source) 
	mapper = EachSubStreamMapper.new(@fairy, opts, block_source)
	mapper.input=self
	mapper
      end

      # emap(%{|enum| enum.collect{..})
      def emap(block_source, opts = nil)
	ERR::Raise ERR::CantAcceptBlock if block_given?
	map_source = %{|i, o| proc{#{block_source}}.call(i).each{|e| o.push e}}
	smap(map_source, opts)
      end

      def map_flatten(block_source, opts = nil)
	ERR::Raise ERR::CantAcceptBlock if block_given?
	map_source = %{|i, o| 
          ary = i.map{|*e| proc{#{block_source}}.call(*e)}
          if o.respond_to?(:push_buf)
             o.push_buf ary.flatten(#{opts[:N] if opts})
          else
             ary.each{|e| e.each{|ee| o.push ee}}
          end}
	smap(map_source, opts)
      end
      alias mapf map_flatten


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

        smap(%{|i, o|
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
              o.push(key)
              buf.clear
              put_cnt += 1
            end
          }
          if buf.size > 0
            key = @base_key + ('%03d' % put_cnt)
            @roma[key] = buf.join(',')
            o.push(key)
            buf.clear
          end
        })
      end


    end
    Fairy::def_job_interface Interface

    def initialize(fairy, opts, block_source)
      super
      @block_source = block_source
    end

    def backend_class_name
      "BEachSubStreamMapper"
    end
  end
end
