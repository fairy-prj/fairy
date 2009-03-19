# encoding: UTF-8

require "fairy/job/job"
require "fairy/share/vfile"

module Fairy
  class Exec < Job
    module Interface

      # Usage:
      # fairy.exec(vnode-spec)....
      #
      def exec(vnode_spec, opts={})
	Exec.exec(self, opts, vnode_spec)
      end
    end
    Fairy::def_fairy_interface Interface

    def Exec.exec(fairy, opts, vnode_spec)
      exec = new(fairy, opts)
      exec.start(vnode_spec)
      exec
    end


    def backend_class_name
      "BExec"
    end

    def start(vnode_spec)
      @vnode_spec = vnode_spec
      case @vnode_spec
      when Array
	vf = VFile.real_files(@vnode_spec)
      when VFile
	vf = @vnode_spec
      when String
	if VFile.vfile?(@vnode_spec)
	  vf = VFile.vfile(@vnode_spec)
	else
	  vf = VFile.real_files([@vnode_spec])
	end
      else
	raise "指定が間違っています"
      end

      backend.start(vf)
    end

  end
end
