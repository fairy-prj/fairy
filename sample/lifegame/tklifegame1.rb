
require "tk"
require "lifegame"

# Tk�ǥ饤�ե���������
class TkLifeGame
  include Tk

  def initialize(width=80, height=80, rectsize=6)
    @lifegame = LifeGame.new(width, height)
    @rectsize = rectsize

    # �ᥤ���Window����
    @canvas = TkCanvas.new(nil,
			   'width'=>(width - 1) * rectsize,
			   'height'=>(height - 1) * rectsize,
			   'borderwidth'=>1,
			   'relief'=>'sunken')

    # [next]�ܥ�������
    @nextbutton = TkButton.new(nil, 'text'=>'next')

    # [go/stop]�ܥ�������
    @gobutton = TkButton.new(nil, 'text'=>'go')

    # [quit]�ܥ�������
    @quitbutton = TkButton.new(nil, 'text'=>'quit')
    @canvas.pack
    @nextbutton.pack('side'=>'left')
    @gobutton.pack('side'=>'left')
    @quitbutton.pack('side'=>'right')

    @prevgrid = {}
    @rectangles = {}
  end

  # �¹�
  def run
    display
    mainloop
  end

  # ɽ��
  def display
    nextgrid = {}
    @lifegame.each_life {|geom|
      if @prevgrid[geom]
	@prevgrid[geom] = nil
      else
	setrect(geom)
      end
      nextgrid[geom] = true
    }
    @prevgrid.each_key {|geom|
      resetrect(geom)
    }
    @prevgrid = nextgrid
  end

  # ����ɽ��
  def setrect(geom)
    @rectangles[geom] = TkcRectangle.new(@canvas,
				      geom.x * @rectsize,
				      geom.y * @rectsize,
				      geom.x * @rectsize + @rectsize - 2,
				      geom.y * @rectsize + @rectsize - 2,
				      'fill'=>'black')
  end

  # ���ξõ�
  def resetrect(geom)
    @rectangles[geom].destroy
    @rectangles[geom] = nil
  end
end

#g = LifeGame.new
g = TkLifeGame.new
g.run

