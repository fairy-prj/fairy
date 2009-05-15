# encoding: UTF-8

module Fairy
  class Inspector
    def initialize(obj)
      @obj = obj
    end

    def exec str
      @obj.instance_eval str
    end
  end
end
