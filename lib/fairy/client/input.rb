# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

require "fairy/share/varray"

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
	  ERR::Raise ERR::NoImpliment "#{desc}, #{desc.peer_class}"
	end
      when Class
	desc.input(self, opts_h, *opts)
      else
	if !desc.kind_of?(String) || VFile.vfile?(desc)
	  InputFile.input(self, opts_h, desc)
	else
	  InputLocalFile.input(self, opts_h, desc)
	end
      end
    end
  end
  def_fairy_interface InputInterface
end


