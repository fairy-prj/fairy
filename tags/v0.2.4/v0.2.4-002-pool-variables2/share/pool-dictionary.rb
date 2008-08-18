
module Fairy
  class PoolDictionary
    
    def initialize
      @pool = {}
      @pool_mutex = Mutex.new
      @pool_cv = ConditionVariable.new
    end

    def def_variable(vname, value = nil)
      @pool_mutex.synchronize do
	if @pool.key?(vname)
	  raise "すでに変数#{vname}は登録されています"
	end
	@pool[vname] = value
	
	instance_eval "def #{vname}; self[:#{vname}]; end"
	instance_eval "def #{vname}=(v); self[:#{vname}]=v; end"
      end
    end

    def [](name)
      @pool_mutex.synchronize do
	raise "変数#{name}は登録されていません" unless @pool.key?(name)
	@pool[name]
      end
    end

    def []=(name, value)
      @pool_mutex.synchronize do
	raise "変数#{name}は登録されていません" unless @pool.key?(name)
	@pool[name] = value
      end
    end
  end
end
