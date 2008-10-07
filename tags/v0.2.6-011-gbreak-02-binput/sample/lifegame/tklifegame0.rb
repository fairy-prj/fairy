
require "tk"

# Tk版ライフゲーム本体
class TkLifeGame
  include Tk

  def initialize(width=80, height=80, rectsize=6)
    @lifegame = LifeGame.new(width, height)
    @rectsize = rectsize

    # メインのWindow生成
    @canvas = TkCanvas.new(nil,
			   'width'=>(width - 1) * rectsize,
			   'height'=>(height - 1) * rectsize,
			   'borderwidth'=>1,
			   'relief'=>'sunken')

    # [next]ボタン生成
    @nextbutton = TkButton.new(nil, 'text'=>'next')

    # [go/stop]ボタン生成
    @gobutton = TkButton.new(nil, 'text'=>'go')

    # [quit]ボタン生成
    @quitbutton = TkButton.new(nil, 'text'=>'quit')
    @canvas.pack
    @nextbutton.pack('side'=>'left')
    @gobutton.pack('side'=>'left')
    @quitbutton.pack('side'=>'right')

  end
end

g = TkLifeGame.new
g.mainloop
