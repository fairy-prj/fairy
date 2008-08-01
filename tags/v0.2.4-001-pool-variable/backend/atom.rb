
module Fairy

  class Atom
    def initialize(receiver, message, *args)
      @receiver = receiver
      @message = message
      @args = args
    end

    attr_reader :receiver, :message, :args
    
  end
end

  
