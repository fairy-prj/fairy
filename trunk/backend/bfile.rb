
module Fairy
  class BFile
    def BFile.open(controller, descripter)
      bfile = BFile.new(controller)
      bfile.open(descripter)
      bfile
    end

    def initialize(controller)
      @controller = controller
    end

    def open(descripter)
      # ��ꤢ����
      @node_files = descripter.map{|d| File.open(d)}
    end

  end
end
