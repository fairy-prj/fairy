# encoding: UTF-8

require "fairy/job/join"

Fairy.def_filter(:equijoin) do |fairy, input, other, *no|
  puts no1 = no2 = no[0]
  puts no2 = no[1] if no[1]

#  mod = Fairy::CONF.HASH_MODULE
#  require mod
#  seed = Fairy::HValueGenerator.create_seed
#  fairy.def_pool_variable(:HASH_SEED, seed)

  main = input.group_by(%{|e| @hgen.value(e[#{no1}]) % CONF.N_MOD_GROUP_BY},
			:BEGIN=>%{
                           mod = CONF.HASH_MODULE
                           require mod
                           @hgen = Fairy::HValueGenerator.new(@Pool[:HASH_SEED])
                        }).barrier(:mode=>:NODE_CREATION, :cond=>:NODE_ARRIVED, :buffer=>:MEMORY)
  other2 = other.group_by(%{|e| @hgen.value(e[#{no2}]) % CONF.N_MOD_GROUP_BY},
			:BEGIN=>%{
                           mod = CONF.HASH_MODULE
                           require mod
                           @hgen = Fairy::HValueGenerator.new(@Pool[:HASH_SEED])
                        }).barrier(:mode=>:NODE_CREATION, :cond=>:NODE_ARRIVED, :buffer=>:MEMORY)


  main.join(other2, %{|in0, in1, out_block|

    next unless in0 && in1    

    ary_m = in0.to_a.group_by{|e| e[#{no1}]}
    ary_o = in1.to_a.group_by{|e| e[#{no2}]}

    ary_m.each do |key, values|
      o_values = ary_o[key]
      next unless o_values
      values.each do |value|
        o_values.each do |o_value|
          out_block.call([value, o_value])
        end
      end
    end
  }, :by => :key)
end

Fairy.def_filter(:equijoin2) do |fairy, input, other, *no|
  puts no1 = no2 = no[0]
  puts no2 = no[1] if no[1]

  main = input.map(%{|e| [e[#{no1}], 0, e]})
  other = other.map(%{|e| [e[#{no2}], 1, e]})
  
  main.cat(other).mod_group_by(%{|e| e[0]}).mapf(%{|key, values|
      parted = values.group_by{|value| value[1]}
      if parted[0] && parted[1]
         parted[0].collect{|e| e[2]}.product(parted[1].collect{|e| e[2]})       
      else
         []
      end
  })

#   main.cat(other).mod_group_by(%{|e| e[0]}).emap(%{|key, values|
#      puts "XXXX: \#{key.inspect}"
#      puts "XXXS: \#{values.inspect}"

#      parted = values.group_by{|value| value[1]}
#      parted[0].product(parted[1])
#   })
end

			       
  
