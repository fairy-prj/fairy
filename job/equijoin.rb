require "job/join"

Fairy.def_filter(:equijoin) do |fairy, input, other, *no|
  puts no1 = no2 = no[0]
  puts no2 = no[1] if no[1]

  mod = Fairy::CONF.HASH_MODULE
  require mod
  seed = Fairy::HashGenerator.create_seed
  fairy.def_pool_variable(:HASH_SEED, seed)

  main = input.group_by(%{|e| @hgen.value(e[#{no1}]) % CONF.N_MOD_GROUP_BY},
			:BEGIN=>%{
                           mod = CONF.HASH_MODULE
                           require mod
                           @hgen = Fairy::HashGenerator.new(@Pool[:HASH_SEED])
                        })
  other2 = other.group_by(%{|e| @hgen.value(e[#{no2}]) % CONF.N_MOD_GROUP_BY},
			:BEGIN=>%{
                           mod = CONF.HASH_MODULE
                           require mod
                           @hgen = Fairy::HashGenerator.new(@Pool[:HASH_SEED])
                        }).barrier(:mode=>:NODE_CREATION, :cond=>:NODE_ARRIVED, :buffer=>:MEMORY)


  main.join(other2, %{|in0, in1, out|

    next unless in0 && in1    

    ary_m = in0.to_a.group_by{|e| e[#{no1}]}
    ary_o = in1.to_a.group_by{|e| e[#{no2}]}

    ary_m.each do |key, values|
      o_values = ary_o[key]
      next unless o_values
      values.each do |value|
        o_values.each do |o_value|
          ary = [*value].push *o_value
          out.push ary
        end
      end
    end
  }, :by => :key)
end
