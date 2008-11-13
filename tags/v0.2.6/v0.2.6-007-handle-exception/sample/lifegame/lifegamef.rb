require "matrix"

require "fairy"

class LifeGameF

  Offsets =  [
    [-1, -1], [-1, 0], [-1, 1], 
    [0, -1],  [0, 0],  [0, 1], 
    [1, -1],  [1, 0],  [1, 1]
  ]
  InitialPositions = [
             [-1, 0], [-1, 1],
    [0, -1], [0, 0],
             [1, 0],
  ]

  def initialize(with, height)
    @fairy = Fairy::Fairy.new("localhost", "19999")

    offset = Vector[with/2, height/2]

    @va = InitialPositions.map{|e| Vector[*e]+offset}.there(@fairy).split(2).to_va
    @fairy.def_pool_variable(:offsets, Offsets.map{|p| Vector[*p]})
  end

  def next
    f1 = @fairy.input(@va).mgroup_by(%{|v| @Pool.offsets.collect{|o| v + o}},
		      :BEGIN=>%{require "matrix"})
    @va = f1.smap(%{|i, o| 
      lives = i.to_a
puts "KEY: \#{i.key}"
puts "Lives: \#{lives.inspect}"
      if lives.include?(i.key) && (lives.size == 3 or lives.size == 4)
puts "A"
        o.push i.key
      elsif lives.size == 3
puts "B"
        o.push i.key
      end
    }, :BEGIN=>%{require "matrix"}).to_va
  end
  alias nextgen next

  def each_life &block
    @va.to_a.each &block
  end

  def live?(vec)
    @va.include?(vec)
  end

  def kill(vec)
    
  end

  def born(vec)
  end

end

