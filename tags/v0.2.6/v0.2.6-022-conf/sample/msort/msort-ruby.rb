va = File.open("/etc/passwd").sort
pvs = va.values_at(va.size.div(3), (va.size*2).div(3), -1)

divs = va.group_by{|e|
  key = pvs.find{|pv| e <= pv}
  key ? key : pvs.last}

out = []
buf = divs.map{|key, div| [div.shift, div]}.sort_by{|v, div|  v}
while min_pair = buf.shift
p min_pair
  out.push min_pair[0]
  next if min_pair[1].empty?
  n = min_pair[1].shift
  idx = buf.rindex{|key, div| key <= n}
  idx ? buf.insert(idx+1, [n, min_pair[1]]) : buf.unshift([n, min_pair[1]])
end

for l in out
  puts l
end

