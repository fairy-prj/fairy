# encoding: UTF-8

require "e2mmap"

module Fairy
  extend Exception2MessageMapper

  class BreakCreateNode<Exception;end

  class NodeNotArrived<StandardError;end

end
