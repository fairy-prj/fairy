
require "tk"
require "lifegame"

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
  
  # 表示
  def display
    nextgrid = {}
    @model.each_life {|geom|
      if @prevgrid[geom]
	@prevgrid.delete(geom)
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
    @rectangles[geom] = TkcRectangle.new(self,
					 geom.x * @rectsize,
					 geom.y * @rectsize,
					 geom.x * @rectsize + @rectsize - 2,
					 geom.y * @rectsize + @rectsize - 2,
					 'fill'=>'black')
  end

  # 点の消去
  def resetrect(geom)
    @rectangles[geom].destroy
    @rectangles.delete(geom)
  end
  
end

# Tk版ライフゲーム本体
class TkLifeGame
  include Tk
  def initialize(width=80, height=80, rectsize=6)
    @model = LifeGame.new(width, height)
    @view = TkLifeGameView.new(nil,
			       'width' => (width - 1) * rectsize,
			       'height' => (height - 1) * rectsize,
			       'borderwidth' => 1,
			       'relief' => 'sunken')
    @view.model = @model
    @view.rectsize = rectsize

    # [next]ボタン生成
    @nextbutton = TkButton.new(nil,
			       'text' => 'next',
			       'command' => proc{@model.nextgen; @view.display})
    # [go/stop]ボタン生成
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

    # [quit]ボタン生成
    @quitbutton = TkButton.new(nil,
			       'text' => 'quit',
			       'command' => proc {exit})
    @view.pack
    @nextbutton.pack('side'=>'left')
    @gobutton.pack('side'=>'left')
    @quitbutton.pack('side'=>'right')

    # マウスボタンを押した時の処理
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

  # メインループ
  def go
    @model.nextgen
    @view.display
    update
    if @goflag
      @after.restart
    end
  end

  # 実行
  def run
    @view.display
    mainloop
  end
end

#g = LifeGame.new
g = TkLifeGame.new
g.run

