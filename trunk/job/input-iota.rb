module Fairy

  DEFAULT_SPLIT_NO = 4

  class Iota < Job
    def self.input(fairy, opts, n)
      unless opts[:SPLIT_NO]
	opts[:SPLIT_NO] = DEFAULT_SPLIT_NO
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


    
      
      
