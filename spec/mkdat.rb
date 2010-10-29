#!/usr/bin/env ruby
# encoding: UTF-8
# 
# Copyright (C) 2007-2010 Rakuten, Inc.
# 

DATA_A = "testdata.txt"
DATA_B = "testdata_multi.txt"
DATA_C1 = "testdata_join_a.txt"
DATA_C2 = "testdata_join_b.txt"

MAX = 1000
MAX_N_ELEMS_PER_LINE = 20


def get_bufsiz(io)
  bufsiz = nil
  loop do
    bufsiz = rand(MAX_N_ELEMS_PER_LINE + 1)
    break unless bufsiz.zero?
    io.puts
  end
  bufsiz
end


#
# Desired data are:
#
#         A/B
#    1 +-------+
#      |  (1)  |
#  100 +-------+
#      |  (2)  |
#  200 +-------+
#      |  (3)  |
#  300 +-------+
#      |  (4)  |
#  400 +-------+
#      |  (5)  |
#  500 +-------+
#      |  (6)  |
#  600 +-------+
#      |  (7)  |
#  700 +-------+
#      |  (8)  |
#  800 +-------+
#      |  (9)  |
#  900 +-------+
#      |  (10) |
# 1000 +-------+
# 

ary = []

1.step(MAX, 100).each{|i|
  (i..MAX).each{|j|
    ary << j
  }
}

ary.shuffle!

io_a = File.open(DATA_A, "w")
io_b = File.open(DATA_B, "w")

buf = []
bufsiz = get_bufsiz(io_b)

ary.each{|e|
  io_a.puts e
  buf << e
  if buf.size == bufsiz
    io_b.puts buf.join(" ")
    buf.clear
    bufsiz = get_bufsiz(io_b)
  end
}

io_b.puts buf.join(" ") unless buf.size.zero?

[io_a, io_b].each{|io| io.close}


#
# Desired data are:
#
#        C1      C2
#   1 +-------+
#     |  (2)  |
#  50 +-------+
#     |       |
#     |  (1)  |-------+  200
#     |       |       |
# 400 +-------+       |
#     |  (3)  |  (1)  |
# 450 +-------+       |
#     |       |       |
#     |  (2)  |-------+  500
#     |       |       |
# 550 +-------+       |
#     |  (3)  |  (2)  |
# 600 +-------+       |
#     |       |       |
#     |  (1)  |-------+  700
#     |       |       |
# 800 +-------+  (1)  |
#             |       |
#             +-------+ 1000
#

ary_a = []
ary_b = []

(1..MAX).each{|i|
  if i <= 800
    ary_a << [i, "A1"] 
  end

  if (i <= 50) || ((i > 400) && (i <= 600))
    ary_a << [i, "A2"]
  end

  if ((i > 400) && (i <= 450)) || ((i > 550) && (i <= 600))
    ary_a << [i, "A3"]
  end

  if i > 200
    ary_b << [i, "B1"]
  end

  if (i > 500) && (i <= 700)
    ary_b << [i, "B2"]
  end
}

ary_a.shuffle!
ary_b.shuffle!

io_c1 = File.open(DATA_C1, "w")
io_c2 = File.open(DATA_C2, "w")

ary_a.each{|e| io_c1.puts e.join(" ")}
ary_b.each{|e| io_c2.puts e.join(" ")}

[io_c1, io_c2].each{|io| io.close}


