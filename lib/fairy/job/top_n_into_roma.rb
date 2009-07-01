# encoding: UTF-8

require 'rbconfig'

module Fairy
  module TopNIntoRomaInterface
    def top_n_into_roma(ap, key, n, sep=',')
      ap = [ap] if ap.kind_of?(String)

      buf = []
      cnt = 0
      if Config::CONFIG['RUBY_INSTALL_NAME'] =~ /jruby/
        here.each{|s|
          cnt += 1
          buf << s if cnt <= n
        }
      else
        here.each{|s|
          cnt += 1
          buf << s
          break if cnt == n
        }
      end
      roma = Roma::Client::RomaClient.new(ap)
      roma[key] = buf.join(sep)
    end
  end
  def_job_interface TopNIntoRomaInterface
end


