
require "share/murmurhash"

case ARGV[0]
when "1"
  hasher = Fairy::MMHash.new(1)
  h = hasher.mhash(ARGV[1])
  p h
  p h.class

when "2"
  require "benchmark"

  hasher = Fairy::MMHash.new(1)
  p Benchmark::measure{1000000.times{hasher.mhash("12345678")}}

  require "digest/md5"
  p Benchmark::measure{1000000.times{Digest::MD5.digest("12345678")}}

  require "digest/sha1"
  p Benchmark::measure{1000000.times{Digest::SHA1.digest("12345678")}}

when "3"
  
  require "benchmark"
  require "digest/md5"
  p Benchmark::measure{1000000.times{Digest::MD5.digest("12345678")[-4,4].unpack("N")}}
  p Benchmark::measure{1000000.times{Digest::MD5.hexdigest("12345678")[-8,8].hex}}
  p Benchmark::measure{1000000.times{Digest::MD5.hexdigest("12345678").hex}%0x3fff_ffff}

  p Benchmark::measure{1000000.times{Digest::MD5.digest("12345678").unpack("N*").last}}

  p Benchmark::measure{1000000.times{Digest::MD5.digest("12345678").unpack("@12N").first}}
  p Benchmark::measure{1000000.times{Digest::MD5.digest("12345678").unpack("x12N").first}}

when "4"
  require "benchmark"
  require "digest/md5"

  p Benchmark::measure{1000000.times{Digest::MD5.digest("12345678").unpack("@12N").first}}

  digest = Digest::MD5.new
  p Benchmark::measure{1000000.times{digest<<"12345678"; digest.digest!.unpack("x12N").first}}
  
  
  
end

  
  
