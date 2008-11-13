
require "tk"
require "sample/lifegame/lifegamef"

class TkLifeGameView < TkCanvas

  def initialize(parent, opts)
    super
    @prevgrid = {}
    @rectangles = {}
    @model = opts['model']
    @rectsize = opts['rectsize']
  end

  attr_accessor :model
  attr_accessor :rectsize
  
  # ɽ��
  def display
    offset = 
    nextgrid = {}
puts "X:0"
    @model.each_life {|geom|
p geom
      
      
      if @prevgrid[geom]
puts "X:1"
	@prevgrid.delete(geom)
      else
puts "X:2"
	setrect(geom)
      end
puts "X:3"
      nextgrid[geom] = true
    }
puts "X:4"
    @prevgrid.each_key {|geom|
puts "X:5"
      resetrect(geom)
    }
puts "X:6"
    @prevgrid = nextgrid
  end

  # ����ɽ��
  def setrect(geom)
    @rectangles[geom] = TkcRectangle.new(self,
					 geom[1] * @rectsize,
					 geom[0] * @rectsize,
					 geom[1] * @rectsize + @rectsize - 2,
					 geom[0] * @rectsize + @rectsize - 2,
					 'fill'=>'black')
  end

  # ���ξõ�
  def resetrect(geom)
    @rectangles[geom].destroy
    @rectangles.delete(geom)
  end
  
end

# Tk�ǥ饤�ե���������
class TkLifeGame
  include Tk
  def initialize(width=80, height=80, rectsize=6)
    @model = LifeGameF.new(width, height)
    @view = TkLifeGameView.new(nil,
			       'width' => (width - 1) * rectsize,
			       'height' => (height - 1) * rectsize,
			       'borderwidth' => 1,
			       'relief' => 'sunken')
    @view.model = @model
    @view.rectsize = rectsize

    # [next]�ܥ�������
    @nextbutton = TkButton.new(nil,
			       'text' => 'next',
			       'command' => proc{@model.nextgen; @view.display})
    # [go/stop]�ܥ�������
    @gobutton = TkButton.new(nil,
			     'text' => 'go',
			     'command' => proc{
			       @goflag = !@goflag
			       if @goflag
				 @gobutton.text 'stop'
				 go
			       else
				 @gobutton.text 'go'
			       end
			     })

    # [quit]�ܥ�������
    @quitbutton = TkButton.new(nil,
			       'text' => 'quit',
			       'command' => proc {exit})
    @view.pack
    @nextbutton.pack('side'=>'left')
    @gobutton.pack('side'=>'left')
    @quitbutton.pack('side'=>'right')

    # �ޥ����ܥ���򲡤������ν���
    @view.bind '1', proc {|x, y|
      geom = Geometry[y / rectsize, x / rectsize]
      if @model.live?(geom)
	@model.kill(geom)
      else
	@model.born(geom)
      end
      @view.display
      update
    }, '%x %y'

    @after = TkAfter.new
    @after.set_start_proc(0, proc {go})
  end

  # �ᥤ��롼��
  def go
    @model.nextgen
    @view.display
    update
    if @goflag
      @after.restart
    end
  end

  # �¹�
  def run
    @view.display
    mainloop
  end
end

#g = LifeGame.new
g = TkLifeGame.new
g.run

