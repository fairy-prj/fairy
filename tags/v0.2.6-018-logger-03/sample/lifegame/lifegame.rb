
require "geometry"

#--
#
#      x  @neighbors = [
#  +---->   [.....]
#  |        [.....]
# y|        [.....]
#  V        [.....]]
#
# �饤�ե���������
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

  # �����
  def initialize(width=80, height=23)
    @width = width
    @height = height
    @lives = {}

    #(A) @neighbors�ν����
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

    #(B) �ǽ����ʪ������
    center = Geometry[height / 2, width / 2]
    for po in InitialPositionOffset
      born(center + po)
    end
  end

  # �����Ƥ��뤫?
  def live?(geom)
    @lives[geom]
  end

  # ���ޤ��
  def born(geom)
    @lives[geom] = true
  end

  # ����
  def kill(geom)
    @lives.delete(geom)
  end

  # �����Ƥ�����ʪ�����Υ��ƥ졼��
  def each_life
    @lives.each_key {|geom|
      yield geom
    }
  end

  # �����������
  def nextgen
    n = {}
    # (C) ���κ�ɸ�μ������¸������ʪ�ο�
    @lives.each_key {|geom|
      n[geom] ||= 0
      @neighbors[geom.y][geom.x].each {|pos|
	n[geom+pos] ||= 0           # n[geom+pos]���ͤ����ꤷ�Ƥ��ʤ����0������
	n[geom+pos] += 1
      }
    }
    # (D) ���κ�ɸ�ˤ�������¸���Υ����å�
    n.each {|geom, count|
      if count == 3 || @lives[geom] && count == 2
	@lives[geom] = true
      else
	@lives.delete(geom)
      end
    }
  end

  # ʸ����
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
