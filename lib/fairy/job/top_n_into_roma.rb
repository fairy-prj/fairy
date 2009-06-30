# encoding: UTF-8


module Fairy
  module TopNIntoRomaInterface
    def top_n_into_roma(ap, key, n, sep=',')
      ap = [ap] if ap.kind_of?(String)

      buf = []
      cnt = 0
      here.each{|s|
        cnt += 1
        buf << s
        break if cnt == n
      }
      roma = Roma::Client::RomaClient.new(ap)
      roma[key] = buf.join(sep)
    end
  end
  def_job_interface TopNIntoRomaInterface
end


