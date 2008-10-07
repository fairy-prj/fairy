
require "backend/bjob"

module Fairy
  class BInput<BJob
    def initialize(*rests)
      super
      
      @create_node_thread = nil
      @create_node_mutex = Mutex.new
    end

    def output=(output)
      @output = output
    end

    def start
      @create_node_thread = Thread.start {
	create_and_start_nodes
      }
    end
  end
end
