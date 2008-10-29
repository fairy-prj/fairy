# ��ɸ���饹
class Geometry

  #��ɸ[y,x]������
  def Geometry.[](y,x)
    new(y, x)
  end

  # �����
  def initialize(y, x)
    @y = y
    @x = x
  end
  
  # x, y�Υ�������
  attr_accessor :x
  attr_accessor :y

  # ��ˡ
  def +(other)
    case other
    when Geometry				    # other��Geometry��?
      Geometry[@y + other.y, @x + other.x]
    when Array					    # other��Array��?
      Geometry[@y + other[0], @x + other[1]]
    else
      raise TypeError, 
	"wrong argument type #{other.type} (expected Geometry or Array)"
    end
  end

  # ��ˡ
  def -(other)
    case other
    when Geometry				    # other��Geometry��?
      Geometry[@y - other.y, @x - other.x]
    when Array					    # other��Array��?
      Geometry[@y - other[0], @x - other[1]]
    else
      raise TypeError, 
	"wrong argument type #{other.type} (expected Geometry or Array)"
    end
  end

  # ���
  def ==(other)
    self.class == other.class and @x == other.x and @y == other.y
  end

  # �ϥå���ؿ�
  def hash
    @x.hash ^ @y.hash
  end

  # �ϥå�����Ӵؿ�
  alias eql? ==

  # ʸ����
  def to_s
    format("%d@%d", @y, @x)
  end

  # ���󥹥ڥ���
  def inspect
    format("#<%d@%d>", @y, @x)
  end
end


