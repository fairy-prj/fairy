
module Fairy
  class There < Job
    module Interface

      # Usage:
      # ○ fairy.there(enumerator)....
      #    enumerable.there(fairy)....
      #    enumerable | fairy.there
      def there(enumerator = nil, opts={})
	There.input(self, opts, enumerator)
      end
    end
    Fairy::def_fairy_interface Interface

    Enumerable.module_eval %{def there(fairy); fairy.there(self); end}

    def self.input(fairy, opts, enumerator)
      self.start(fairy, opts, enumerator)
    end

    def self.start(fairy, opts, enumerator)
      there = new(fairy, opts, enumerator)
      there.start
      there
    end

    def initialize(fairy, opts, enumerator)
      super
      @enumerator = enumerator
    end

    def backend_class_name
      "BThere"
    end

    def start
      backend.start
    end
  end
end
