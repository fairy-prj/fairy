# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#
require "xthread"
require "fiber-mon"
require "fairy/string_buffer"

begin
  require "fairy/xmarshaled_queue"
rescue LoadError
end

