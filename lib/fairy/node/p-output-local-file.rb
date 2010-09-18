# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

require "uri"

require "fairy/node/p-here"

module Fairy
  class POutputLocalFile<PHere
    Processor.def_export self
  end
end
