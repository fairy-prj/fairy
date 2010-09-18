# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

module Fairy
  begin
    if CONF.DEFAULT_EXTERNAL
      Encoding.default_external = CONF.DEFAULT_EXTERNAL
    end
    if CONF.DEFAULT_INTERNAL
      Encoding.default_internal = CONF.DEFAULT_INTERNAL
    end
  rescue NameError
    ERR.Raise ERR::NoSupportRubyEncoding, RUBY_VERSION
  end
end
