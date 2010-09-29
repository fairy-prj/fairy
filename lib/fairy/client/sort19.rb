# encoding: UTF-8
#
# Copyright (C) 2007-2010 Rakuten, Inc.
#

require "fairy/client/merge-group-by"

Fairy.def_filter(:sort_by_with_va, :sub => true) do |fairy, input, block_source, opts = {}|
  
  sampling_ratio_1_to = opts[:sampling_ratio]
  sampling_ratio_1_to ||= Fairy::CONF.SORT_SAMPLING_RATIO_1_TO
  pvn = opts[:pvn]
  pvn ||= Fairy::CONF.SORT_NO_SEGMENT
  
  va = input.emap(%{|i| 
    sort_proc = proc{#{block_source}}
    i.to_a.collect{|e| [sort_proc.call(e), e]}.sort_by{|e| e.first}}).to_va

  if va.size/sampling_ratio_1_to < Fairy::CONF.SORT_SAMPLING_MIN
    #sampling_ratio_1_to = Fairy::CONF.SORT_SAMPLING_MIN.div(va.size)
    sampling_ratio_1_to = va.size.div(Fairy::CONF.SORT_SAMPLING_MIN)
    sampling_ratio_1_to = 1 if sampling_ratio_1_to.zero?
  end
  if va.size/sampling_ratio_1_to > Fairy::CONF.SORT_SAMPLING_MAX
    sampling_ratio_1_to = va.size.div(Fairy::CONF.SORT_SAMPLING_MAX)
  end

  Fairy::Log::debug(self, "SAMPLING: RATIO: 1/#{sampling_ratio_1_to}")
  Fairy::Log::debug(self, "SAMPLING: VA SIZE: #{va.size}")
  sample = fairy.input(va).select(%{|e| (i += 1) % #{sampling_ratio_1_to} == 0},
				  :BEGIN=>%{i = 0}).here.sort_by{|e| e.first}.map{|e| e.first}

  Fairy::Log::debug(self, "SAMPLING: SAMPLE: %s", sample.inspect)

  idxes = (1...pvn).collect{|i| (sample.size*i).div(pvn)}
  idxes.push -1
  pvs = sample.values_at(*idxes)
  Fairy::Log::debug(self, "PVS: #{pvs.inspect}")
  fairy.def_pool_variable(:pvs, pvs)

  div = fairy.input(va).merge_group_by(%{|e| 
    key = @Pool.pvs.find{|pv| e.first <= pv}
    key ? key : @Pool.pvs.last},
				       :postqueuing_policy => {:queuing_class => :PoolQueue}

)

  msort = div.seg_map(%{|i, block|
    buf = i.map{|st| [st, st.pop.dc_deep_copy]}.select{|st, v|!v.nil?}.sort_by{|st, v| v.first}
    while st_min = buf.shift
      st, min = st_min
      block.call min.last
      next unless v = st.pop.dc_deep_copy # 取りあえずの対応
      idx = buf.rindex{|st0, v0| v0.first <= v.first}
      idx ? buf.insert(idx+1, [st, v]) : buf.unshift([st, v])
    end})
  
  shuffle = msort.seg_eshuffle(%{|i| i.sort{|s1, s2| s1.key <=> s2.key}})
#  shuffle = msort.eshuffle(%{|i| i.sort_by{|s1| Log::debug(self, s1.key.inspect); s1.key}})
end

