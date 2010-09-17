# encoding: UTF-8

module Fairy

  SPLIT_NO = CONF.IOTA_SPLIT_NO

  class Iota < Job
    module Interface

      # Usage:
      # fairy.iota(no)....
      #
      def iota(times, opts={})
	Iota.input(self, opts, times)
      end
      alias times iota
      
    end
    Fairy::def_fairy_interface Interface

    def self.input(fairy, opts, n)
      unless opts[:SPLIT_NO]
	opts[:SPLIT_NO] = SPLIT_NO
      end
      iota = new(fairy, opts, n)
      iota.start
      iota
    end
    
    
    def backend_class_name
      "BIota"
    end

    def start
      backend.start
    end
  end
end


    
      
      