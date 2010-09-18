# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

module Fairy
  module Debug
    def njob_status_monitor_on(fairy)
#      require "backend/bjob"
      
      bjob = fairy.name2backend_class("CFilter")
      bjob.watch_status = true
    end

    for method in self.instance_methods
      module_function method
    end

  end
end

