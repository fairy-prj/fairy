# encoding: UTF-8

require "job/merge-group-by"

Fairy.def_filter(:sort_by) do |fairy, input, block_source, opts = {}|
#Fairy.def_filter(:sort_by) do |fairy, input, block_source, *opts|
  
  sampling_ratio_1_to = opts[:sampling_ratio]
  sampling_ratio_1_to ||= Fairy::CONF.SORT_SAMPLING_RATIO_1_TO
  pvn = opts[:pvn]
  pvn ||= Fairy::CONF.SORT_N_GROUP_BY
  
  va = input.emap(%{|i| 
    sort_proc = proc{#{block_source}}
    i.to_a.collect{|e| [sort_proc.call(e), e]}.sort_by{|e| e.first}}).to_va

  if va.size/sampling_ratio_1_to < Fairy::CONF.SORT_SAMPLING_MIN
    sampling_ratio_1_to = Fairy::CONF.SORT_SAMPLING_MIN.div(va.size)
  end
  if va.size/sampling_ratio_1_to > Fairy::CONF.SORT_SAMPLING_MAX
    sampling_ratio_1_to = Fairy::CONF.SORT_SAMPLING_MAX.div(va.size)
  end

  Fairy::Log::debug(self, "SAMPLING: RATIO: 1/#{sampling_ratio_1_to}")
  sample = fairy.input(va).select(%{|e| (i += 1) % #{sampling_ratio_1_to} == 0},
				  :BEGIN=>%{i = 0}).here.sort_by{|e| e.first}.map{|e| e.first}

  idxes = (1...pvn).collect{|i| (sample.size*i).div(pvn)}
  idxes.push -1
  pvs = sample.values_at(*idxes)
  Fairy::Log::debug(self, "PVS: #{pvs.inspect}")
  fairy.def_pool_variable(:pvs, pvs)

  div = fairy.input(va).merge_group_by(%{|e| 
    key = @Pool.pvs.find{|pv| e.first <= pv}
    key ? key : @Pool.pvs.last})

  msort = div.smap(%{|i, o|
    buf = i.map{|st| [st, st.pop.dc_deep_copy]}.select{|st, v|!v.nil?}.sort_by{|st, v| v.first}
    while st_min = buf.shift
      st, min = st_min
      o.push min.last
      next unless v = st.pop.dc_deep_copy # 取りあえずの対応
      idx = buf.rindex{|st0, v0| v0.first <= v.first}
      idx ? buf.insert(idx+1, [st, v]) : buf.unshift([st, v])
    end})
  
  shuffle = msort.eshuffle(%{|i| i.sort{|s1, s2| s1.key <=> s2.key}})
#  shuffle = msort.eshuffle(%{|i| i.sort_by{|s1| Log::debug(self, s1.key.inspect); s1.key}})
end

