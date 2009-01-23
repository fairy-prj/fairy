
require "tk"
require "lifegame"

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

    @prevgrid = {}
    @rectangles = {}
  end

  # 実行
  def run
    display
    mainloop
  end

  # 表示
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

  # 点の表示
  def setrect(geom)
    @rectangles[geom] = TkcRectangle.new(@canvas,
				      geom.x * @rectsize,
				      geom.y * @rectsize,
				      geom.x * @rectsize + @rectsize - 2,
				      geom.y * @rectsize + @rectsize - 2,
				      'fill'=>'black')
  end

  # 点の消去
  def resetrect(geom)
    @rectangles[geom].destroy
    @rectangles[geom] = nil
  end
end

#g = LifeGame.new
g = TkLifeGame.new
g.run

