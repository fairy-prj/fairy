
require "tk"

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

  end
end

g = TkLifeGame.new
g.mainloop
