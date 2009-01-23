
require "share/varray"

module Fairy
  module InputInterface
    def input(desc, *opts)
      if opts.last.kind_of?(Hash)
	opts_h = opts.pop
      else
	opts_h = {}
      end

      case desc
#      when Enumerable
#	There.input(self, opts_h, desc, *opts)
      when VArray
	  InputVArray.input(self, opts_h, desc)
      when DeepConnect::Reference
	if desc.peer_class.name == "Fairy::VArray"
	  InputVArray.input(self, opts_h, desc)
	else
	  raise "まだサポートしていません(#{desc}, #{desc.peer_class})"
	end
      when Class
	desc.input(self, opts_h, *opts)
      else
	if !desc.kind_of?(String) || VFile.vfile?(desc)
	  FFile.input(self, opts_h, desc)
	else
	  LFileInput.input(self, opts_h, desc)
	end
      end
    end
  end
  def_fairy_interface InputInterface
end


