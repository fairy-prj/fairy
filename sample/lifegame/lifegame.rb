
require "geometry"

#--
#
#      x  @neighbors = [
#  +---->   [.....]
#  |        [.....]
# y|        [.....]
#  V        [.....]]
#
# ライフゲーム本体
class LifeGame
  DefaultCompetitionArea =  [
    Geometry[-1, -1], Geometry[-1, 0], Geometry[-1, 1], 
    Geometry[0, -1],                   Geometry[0, 1], 
    Geometry[1, -1],  Geometry[1, 0],  Geometry[1, 1]
  ]

  InitialPositionOffset = [
             [-1, 0], [-1, 1],
    [0, -1], [0, 0],
             [1, 0]
  ]

  # 初期化
  def initialize(width=80, height=23)
    @width = width
    @height = height
    @lives = {}

    #(A) @neighborsの初期化
    @neighbors = Array.new(height)
    for y in 0..height - 1
      @neighbors[y] = a = Array.new(width)

      if y == 0
	competition_area = DefaultCompetitionArea.find_all{|geom| geom.y >= 0}
      elsif y == height - 1
	competition_area = DefaultCompetitionArea.find_all{|geom| geom.y <= 0}
      else
	competition_area = DefaultCompetitionArea
      end
      
      a[0] = competition_area.find_all{|geom| geom.x >= 0}
      for x in 1.. width - 2
	a[x] = competition_area
      end
      a[width - 1] = competition_area.find_all{|geom| geom.x <= 0}
    end

    #(B) 最初の生物の設定
    center = Geometry[height / 2, width / 2]
    for po in InitialPositionOffset
      born(center + po)
    end
  end

  # 生きているか?
  def live?(geom)
    @lives[geom]
  end

  # 生まれる
  def born(geom)
    @lives[geom] = true
  end

  # 殺す
  def kill(geom)
    @lives.delete(geom)
  end

  # 生きている生物全部のイテレータ
  def each_life
    @lives.each_key {|geom|
      yield geom
    }
  end

  # 次世代の生成
  def nextgen
    n = {}
    # (C) その座標の周りに生存する生物の数
    @lives.each_key {|geom|
      n[geom] ||= 0
      @neighbors[geom.y][geom.x].each {|pos|
	n[geom+pos] ||= 0           # n[geom+pos]に値が設定していなければ0を代入
	n[geom+pos] += 1
      }
    }
    # (D) その座標における生存条件のチェック
    n.each {|geom, count|
      if count == 3 || @lives[geom] && count == 2
	@lives[geom] = true
      else
	@lives.delete(geom)
      end
    }
  end

  # 文字列化
  def to_s
    s = ' ' * (@width * @height)
    each_life {|geom| s[geom.y * @width + geom.x, 1]='*'}
    s
  end
end

if __FILE__ == $0
  g = LifeGame.new
  loop {
    print g.to_s, "\n"
    break unless gets
    g.nextgen
  }
end
