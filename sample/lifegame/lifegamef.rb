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

  def initialize(width, height)
    @width = width
    @height = height 

    @fairy = nil
  end

  def init
    @fairy = Fairy::Fairy.new("localhost", "19999")

    offset = Vector[@width/2, @height/2]

    @va = InitialPositions.map{|e| Vector[*e]+offset}.there(@fairy).split(2).to_va
    @fairy.def_pool_variable(:offsets, Offsets.map{|p| Vector[*p]})

    @gen = 0
  end
    
  def next
    init unless @fairy

    @gen += 1
    puts "GEN: #{@gen}"

    f1 = @fairy.input(@va).basic_mgroup_by(%{|v| @Pool.offsets.collect{|o| v + o}},
		      :BEGIN=>%{require "matrix"})
    @va = f1.seg_map(%{|i, b| 
      lives = i.to_a
puts "KEY: \#{i.key}"
puts "Lives: \#{lives.inspect}"
      if lives.include?(i.key) && (lives.size == 3 or lives.size == 4)
puts "A"
        b.call i.key
      elsif lives.size == 3
puts "B"
        b.call i.key
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

