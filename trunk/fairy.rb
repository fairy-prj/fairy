
require "deep-connect/deep-connect.rb"

require "job/ffile"
require "job/local-file-input"

module Fairy

  class Fairy

    def initialize(master_host, master_port)
      @name2backend_class = {}

      @deep_connect = DeepConnect.start(0)
      @master_deepspace = @deep_connect.open_deepspace(master_host, master_port)
      @master = @master_deepspace.import("Master")

      @controller = @master.assgin_controller

    end

    attr_reader :controller

    def name2backend_class(backend_class_name)
      if klass = @name2backend_class[backend_class_name]
	return klass 
      end
      
      if klass =  @controller.import(backend_class_name)
	@name2backend_class[backend_class_name] = klass
      end
      klass
    end

    def input(ffile_descripter, opts = nil)
      if !ffile_descripter.kind_of?(String) || VFile.vfile?(ffile_descripter)
	FFile.input(self, ffile_descripter)
      else
	LFileInput.input(self, ffile_descripter, opts)
      end
    end

#     def send_atom(atom)
#       ref = Reference.new
#       Thread.start do
# 	ref.value = @backend_controller.send_atom(atom)
#       end
#       ref
#     end
  end
end

