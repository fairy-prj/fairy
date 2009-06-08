begin
  require 'irb/src_encoding'
  require "irb/magic-file"
rescue LoadError
end
require "irb/locale"

module Fairy
  LC_MESSAGES = IRB::Locale.new
  LC_MESSAGES.load(CONF.LIB+"/fairy/share/exceptions.rb")
end

