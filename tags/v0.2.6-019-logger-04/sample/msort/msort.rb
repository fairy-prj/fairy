
# pattern1 

ss = fairy.input(va).lift_down
divs = ss.map{|s|
  s.group_by(%{|e|
     key = @Pool.pvs.find{|pv| e <= pv}
     key ? key : @Pool.pvs.last})}
msort = divs[0].join(*divs[1,-1]){|f0, *rest|
  f0.merge(*rest, %{|stary, out|
     buf = stary.map{|st| [st, st.pop]}.select{|pair| pair[1] != EOS}.sort{|p1, p2| p1[1] <=> p2[1]}
     while min = buf.pop
        out.push min[1]
        n = min[0].pop
        next if n == EOF
        idx = buf.rindex{|b| b[1] < n}
        idx ? buf.insert(idx+1, [min[0], n]) : buf.unshift [min[0], n]
    end})}
shuffle = msort.sort{|f1, f2| f1.key <=> f2.key}.lift_up
puts "RESULT:"
for l in shuffle.here
  puts l
end

# pattern2

ss = fairy.input(va).as_bundle
  s.group_by(%{|e|
     key = @Pool.pvs.find{|pv| e <= pv}
     key ? key : @Pool.pvs.last})
}

msort = divs[0].join(*divs[1,-1]){|f0, *rest|
  f0.merge(*rest, %{|stary, out|
     buf = stary.map{|st| [st, st.pop]}.select{|pair| pair[1] != EOS}.sort{|p1, p2| p1[1] <=> p2[1]}
     while min = buf.pop
        out.push min[1]
        n = min[0].pop
        next if n == EOF
        idx = buf.rindex{|b| b[1] < n}
        idx ? buf.insert(idx+1, [min[0], n]) : buf.unshift [min[0], n]
    end})}
shuffle = msort.sort{|f1, f2| f1.key <=> f2.key}.lift_up
puts "RESULT:"
for l in shuffle.here
  puts l
end

