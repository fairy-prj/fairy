
module Fairy
  module Debug
    def njob_status_monitor_on
      require "backend/bjob"
      
      BJob::watch_status = true
    end

    for method in self.instance_methods
      module_function method
    end

  end
end

