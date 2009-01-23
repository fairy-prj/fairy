# 座標クラス
class Geometry

  #座標[y,x]の生成
  def Geometry.[](y,x)
    new(y, x)
  end

  # 初期化
  def initialize(y, x)
    @y = y
    @x = x
  end
  
  # x, yのアクセサ
  attr_accessor :x
  attr_accessor :y

  # 加法
  def +(other)
    case other
    when Geometry				    # otherがGeometryか?
      Geometry[@y + other.y, @x + other.x]
    when Array					    # otherがArrayか?
      Geometry[@y + other[0], @x + other[1]]
    else
      raise TypeError, 
	"wrong argument type #{other.type} (expected Geometry or Array)"
    end
  end

  # 減法
  def -(other)
    case other
    when Geometry				    # otherがGeometryか?
      Geometry[@y - other.y, @x - other.x]
    when Array					    # otherがArrayか?
      Geometry[@y - other[0], @x - other[1]]
    else
      raise TypeError, 
	"wrong argument type #{other.type} (expected Geometry or Array)"
    end
  end

  # 比較
  def ==(other)
    self.class == other.class and @x == other.x and @y == other.y
  end

  # ハッシュ関数
  def hash
    @x.hash ^ @y.hash
  end

  # ハッシュ比較関数
  alias eql? ==

  # 文字列化
  def to_s
    format("%d@%d", @y, @x)
  end

  # インスペクト
  def inspect
    format("#<%d@%d>", @y, @x)
  end
end


