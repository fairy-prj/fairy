#!/usr/bin/env ruby
#
# Log Analysis Data Visualizer for fairy
# 
# This tool processes a output of log-analysis.rb 
# and outputs a PNG image.
#
# by Hajime Masuda (Rakuten, Inc.)
#
# encoding: UTF-8

require 'time'
#require 'pp'

require 'rubygems'
require 'cairo'


module FairyPerformanceGraph

  class Base
    attr_reader :parent, :margin_top, :margin_button, :margin_between_elems, :margin_left, :margin_right

    def initialize(parent)
      @parent = parent

      @margin_top           = 0
      @margin_button        = 0
      @margin_between_elems = 0
      @margin_left          = 0
      @margin_right         = 0
    end

    def height
      init_val = @margin_top + @margin_button - @margin_between_elems
      @contents.inject(init_val){|result,elem|
        result += elem.height + @margin_between_elems
        result
      }
    end
  end

  class Session < Base
    attr_accessor :nodes, :scale, :start_at, :elapsed

    def initialize(width=1200)
      super(nil)

      @contents = []

      @margin_top           = 25
      @margin_button        = 5
      @margin_between_elems = 5
      @margin_left          = 5
      @margin_right         = 5

      @width = width
      @bgcolor = "WHITE"
      @fgcolor = "BLACK"
      @rounded_r = 10
      @font_size = 18.0

      @cache = {}
    end

    def index(name)
      return @cache[name] if @cache[name]

      idx = @contents.index{|node| node.name == name}
      @cache[name] = idx if idx

      return idx
    end
    
    def nodes
      @contents
    end

    def max_child_width
      @width - (@margin_left + @margin_right)
    end

    def draw(write_to)
      @start_at = Time.now
      @end_at = Time.at(0)
      
      @contents.each{|node|
        node.processors.each{|processor|
          processor.filters.each{|filter|
            @start_at = filter.start_at if @start_at > filter.start_at
            @end_at = filter.end_at if @end_at < filter.end_at
          }
        }
      }
      
      max_filter_width = @contents[0].processors[0].filters[0].max_width
      @scale = max_filter_width.to_f / (@end_at - @start_at)

      @elapsed = @end_at - @start_at
      
      format = Cairo::FORMAT_ARGB32
      surface = Cairo::ImageSurface.new(format, @width, (h = height))
      @context = Cairo::Context.new(surface)

      # draw background
      @context.set_source_color(@bgcolor)
      @context.rectangle(0, 0, @width, h)
      @context.fill

      # draw nodes
      height_total = 0
      w = @width - (@margin_left + @margin_right)
      @contents.each_with_index{|node,i|
        off_y = @margin_top + (i * @margin_between_elems) + height_total
        height_total += node.draw(@context, @margin_left, off_y, w)
      }


      # draw time lines
      ml = @margin_left + @contents[0].margin_left + @contents[0].processors[0].margin_left
      interval = max_filter_width / 20
      y = h - @margin_button
      @context.set_source_color("DARK_KHAKI")
      @context.set_line_width(0.5)
      @context.set_dash([2,2])
      21.times{|i|
        @context.move_to(ml, @margin_top)
        @context.line_to(ml, y)
        @context.stroke
        ml += interval
      }
      @context.set_dash(nil)

      # draw text
      @context.set_source_color(@fgcolor)
      @context.move_to(5, @font_size)
      @context.set_font_size(@font_size)
      @context.show_text("START:#{@start_at} -- END:#{@end_at} (#{@elapsed.to_i} sec.)")

      # output image
      surface.write_to_png(write_to)
    end
  end

  class Node < Base
    attr_reader :name, :line_color, :bgcolor

    def initialize(parent, name)
      super(parent)

      @name = name
      @contents = []

      @margin_left = 80 

      @bgcolor    = "#FFFFDD"
      @fgcolor    = "BLACK"
      @rounded_r  = 10
      @font_size  = 14.0
      @line_width = 0.5
    end

    def processors
      @contents
    end

    def max_child_width
      @parent.max_child_width - (@margin_left + @margin_right)
    end

    def draw(context, x, y, width)
      context.set_source_color(@bgcolor)
      context.rounded_rectangle(x, y, width, (h = height), @rounded_r, @rounded_r)
      context.fill_preserve
      context.set_source_color(@fgcolor)
      context.set_line_width(@line_width)
      context.stroke

      context.move_to((x + 5), (y + @font_size + 5))
      context.set_font_size(@font_size)
      context.show_text(@name)

      # draw processors
      height_total = 0
      off_x = x + @margin_left
      w = width - @margin_left
      @contents.each_with_index{|processor,i|
        off_y = y + @margin_top + (i * @margin_between_elems) + height_total
        height_total += processor.draw(context, off_x, off_y, w)
      }

      h
    end
  end

  class Processor < Base
    attr_reader :id

    def initialize(parent, id)
      super(parent)

      @id = id
      @contents = []

      @margin_top           = 10
      @margin_button        = 10
      @margin_between_elems = 5
      @margin_left          = 40
      @margin_right         = 5

      @fgcolor    = "DARK_BLUE"
      @line_width = 0.75
      @dash       = [5,2]
      @font_size  = 14.0
    end

    def filters
      @contents
    end

    def max_child_width
      @parent.max_child_width - (@margin_left + @margin_right)
    end

    def draw(context, x, y, width)
      context.set_source_color(@fgcolor)
      context.set_font_size(@font_size)
      context.move_to((x + 5), (y + @font_size + 5))
      context.show_text("P#"+id.to_s)

      @contents.sort!{|a,b| a.start_at - b.start_at}

      # draw filters
      height_total = 0
      off_x = x + @margin_left
      @contents.each_with_index{|filter,i|
        off_y = y + @margin_top + (i * @margin_between_elems) + height_total
        height_total += filter.draw(context, off_x, off_y)
      }

      if @id == (@parent.processors.size - 1)
        h = height
      else
        context.set_source_color(@fgcolor)
        context.set_line_width(@line_width)
        context.set_dash(@dash)
        context.move_to(x, (y + (h = height)))
        context.line_to((x + width), (y + h))
        context.stroke
        context.set_dash(nil)
      end

      h
    end
  end

  class Filter < Base
    IMPORT     = 0
    PROCESSING = 1
    EXPORT     = 2

    TYPE_NAME2IDX = {
      "IMPORT"      => IMPORT, 
      "PROCESSING"  => PROCESSING,
      "EXPORT"      => EXPORT
    }

    PTN_NAME = /\A(?:\w+::)*\w+\[((\d+)-\d+)(\[(\d+:\d+)\])?\]\z/

    attr_reader :name, :type, :start_at, :end_at, :elapsed, :job_id, :task_id, :key_info
    attr_accessor :elapsed_for_store

    def initialize(parent, name, type, start_at, end_at, elapsed)
      super(parent)

      @name     = name
      @type     = TYPE_NAME2IDX[type]
      @start_at = Time.parse(start_at)
      @end_at   = Time.parse(end_at)
      @elapsed  = elapsed

      @fgcolor   = "BLACK"
      @font_size = 8

      @job_id, @task_id, @key_info = self.class.parse_name(@name)
    end

    def self.parse_name(name)
      if m = name.match(PTN_NAME)
        m[1..3]
      else
        []
      end
    end

    def height
      10
    end
    
    def max_width
      @parent.max_child_width
    end

    def draw(context, x, y)
      case @type
      when IMPORT
        context.set_source_color("LIGHT_BLUE")
      when PROCESSING
        context.set_source_color("PINK")
      when EXPORT
        context.set_source_color("LIGHT_GREEN")
      end

      scale = @parent.parent.parent.scale
      global_start_at = @parent.parent.parent.start_at

      width = ((@end_at - @start_at) * scale).to_i
      off_x = x + ((@start_at - global_start_at) * scale).to_i

      context.rectangle(off_x, y, width, (h = height))
      context.fill

      if @type == IMPORT
        context.set_source_color("CORNFLOWER_BLUE")
        width_store = (@elapsed_for_store * scale).to_i
        off_x_store = off_x + width - width_store
        context.rectangle(off_x_store, y, width_store, h)
        context.fill
      end

      context.set_source_color(@fgcolor)
      context.move_to((off_x + 2), (y + @font_size))
      context.set_font_size(@font_size)
      context.show_text(@name)

      global_elaped = @parent.parent.parent.elapsed
      percentage = (@elapsed / global_elaped) * 100

      context.set_source_color("RED")
      context.move_to(off_x + 2 + ((@name.size * @font_size) * 0.7).to_i, (y + @font_size))
      context.set_font_size(@font_size + 2.0)
      context.show_text("%.1f sec. (%.1f%%)" % [@elapsed, percentage])


      h
    end
  end

  class LogParser
    PTN_SEPARATOR    = /, */
    PTN_FIRST_RECORD = /\A([^ ]+) \[P\]#(\d+) (.+)\z/

    attr_reader :io

    def initialize(log)
      @io = File.open(log, "r")
    end

    def each_log
      @io.each_with_index{|ln,i|
        ln.chomp!
        ary = ln.split(PTN_SEPARATOR)
        m = ary[0].match(PTN_FIRST_RECORD)

        line_no       = i+1
        host_name     = m[1]
        processor_id  = m[2].to_i
        filter_name   = m[3]
        type          = ary[1]
        start_at      = ary[2]
        end_at        = ary[3]
        elasped       = ary[4].to_f

        yield(line_no, host_name, processor_id, filter_name, type, start_at, end_at, elasped)
      }
    end
  end

  #
  # main
  #
  def run(input_from, output_to, width=nil)
    if width
      graph = Session.new(width)
    else
      graph = Session.new
    end

    log = LogParser.new(input_from)
    log.each_log{|line_no, host_name, processor_id, filter_name, type, start_at, end_at, elapsed|

      if type == "STORE"
        node = graph.nodes.select{|node| node.name == host_name}[0] or next
        processor = node.processors[processor_id] or next
        filter = processor.filters.select{|filter| (filter.type == Filter::IMPORT) && (filter.job_id == Filter.parse_name(filter_name)[0])}[0] or next
        filter.elapsed_for_store = elapsed
        #$stderr.puts("set filter.elapsed_for_store (#{filter.name})")
        next
      end

      if idx = graph.index(host_name)
          node = graph.nodes[idx]
      else
          node = Node.new(graph, host_name)
          graph.nodes << node
      end

      unless processor = node.processors[processor_id]
        processor = Processor.new(node, processor_id)
        node.processors << processor
      end

      processor.filters << Filter.new(processor, filter_name, type, start_at, end_at, elapsed)
    }
    #pp graph

    graph.draw(output_to)
  end
  module_function :run

end


#
# boot strap
#
unless ARGV.size == 2 || ARGV.size == 3
  $stderr.puts "Usage: #{File.basename($0)} INPUT_FROM OUTPUT_TO [IMAGE_WIDTH]"
  exit(1)
end

INPUT_FROM  = ARGV.shift
OUTPUT_TO   = ARGV.shift
WIDTH       = ARGV.shift

if WIDTH
  FairyPerformanceGraph.run(INPUT_FROM, OUTPUT_TO, WIDTH.to_i)
else
  FairyPerformanceGraph.run(INPUT_FROM, OUTPUT_TO)
end

exit(0)


