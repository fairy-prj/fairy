# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

require "fairy/client/filter"

module Fairy
  class There < Filter
    module Interface

      # Usage:
      # ○ fairy.there(enumeratable)....
      # ○ enumerable.there(fairy)....
      #    enumerable | fairy.there
      def there(enumerable = nil, opts={})
	There.input(self, opts, enumerable)
      end
    end
    Fairy::def_fairy_interface Interface

    Enumerable.module_eval %{def there(fairy); fairy.there(self); end}

    def self.input(fairy, opts, enumerable)
      self.start(fairy, opts, enumerable)
    end

    def self.start(fairy, opts, enumerable)
      there = new(fairy, opts, enumerable)
      there.start
      there
    end

    def initialize(fairy, opts, enumerable)
      super
      @enumerable = enumerable
    end

    def backend_class_name
      "CThere"
    end

    def start
      backend.start
    end
  end
end
