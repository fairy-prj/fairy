# encoding: UTF-8

require "job/merge-group-by"

Fairy.def_filter(:sort) do |fairy, input, opts = {}|
  
  sampling_ratio_1_to = opts[:sampling_ratio]
  sampling_ratio_1_to ||= Fairy::CONF.SORT_SAMPLING_RATIO_1_TO
  pvn = opts[:pvn]
  pvn ||= Fairy::CONF.SORT_N_GROUP_BY
  
  va = input.emap(%{|i| i.to_a.sort}).to_va

  if va.size/sampling_ratio_1_to < Fairy::CONF.SORT_SAMPLING_MIN
    sampling_ratio_1_to = Fairy::CONF.SORT_SAMPLING_MIN.div(va.size)
  end
  if va.size/sampling_ratio_1_to > Fairy::CONF.SORT_SAMPLING_MAX
    sampling_ratio_1_to = Fairy::CONF.SORT_SAMPLING_MAX.div(va.size)
  end

  Fairy::Log::debug(self, "SAMPLING: RATIO: 1/#{sampling_ratio_1_to}")
  sample = fairy.input(va).select(%{|e| (i += 1) % #{sampling_ratio_1_to} == 0},
				    :BEGIN=>%{i = 0}).here.sort

  idxes = (1...pvn).collect{|i| (sample.size*i).div(pvn)}
  idxes.push -1
  pvs = sample.values_at(*idxes)
  fairy.def_pool_variable(:pvs, pvs)

  div = fairy.input(va).merge_group_by(%{|e| 
    key = @Pool.pvs.find{|pv| e <= pv}
    key ? key : @Pool.pvs.last})

  msort = div.smap(%{|i, o|
    buf = i.map{|st| [st, st.pop]}.select{|st, v|!v.nil?}.sort_by{|st, v| v}
    while st_min = buf.shift
      st, min = st_min
      o.push min
      next unless v = st.pop
      idx = buf.rindex{|st, vv| vv <= v}
      idx ? buf.insert(idx+1, [st, v]) : buf.unshift([st, v])
    end})

  shuffle = msort.eshuffle(%{|i| i.sort{|s1, s2| s1.key <=> s2.key}})
end

