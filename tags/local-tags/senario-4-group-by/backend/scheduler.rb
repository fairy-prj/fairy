
module Fairy
  class Scheduler

    def initialize(controller)
      # ��ꤢ����
      @controller = controller
      #@processors = [Processor.new]
    end
      

    def schedule(atom)
      # ��ꤢ����.
      # ������, atom�ˤϥΡ��ɻ��꤬���Ǥˤ���Ȥ��Ƥ���.
      # ���ꤵ�줿�Ρ��ɤ˥������塼�뤹��.
      @processors[0].exec atom
    end
    
  end
end
