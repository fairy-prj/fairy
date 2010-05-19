
module Fairy
  class WC<Job
    module Interface
      def wc(from_desc, to_desc, opts = {})
	wc = WC.open(self, opts, from_desc)
	wc.post_wc(to_desc, opts)
      end
    end
    Fairy::def_fairy_interface Interface

    @backend_class = nil

    def self.open(fairy, opts, from_desc)
      wc = new(fairy, opts)
      wc.open(from_desc)
      wc
    end

    def initialize(fairy, opts=nil)
      super
    end

    def backend_class_name
      "BWC"
    end

    def open(from_desc)
      @descripter = from_desc

      case from_desc
      when Array
	vf = VFile.real_files(from_desc)
      when VFile
	vf = from_desc
      when String
	if VFile.vfile?(from_desc)
	  vf = VFile.vfile(from_desc)
	else
	  vf = VFile.real_files([from_desc])
	end
      else
	ERR::Raise ERR::IllegalVFile
      end
      backend.open(vf)
      self
    end

    class PostFilter<Filter
      module Interface
	def post_wc(to_desc, opts = nil)
	  post_wc = PostFilter.new(@fairy, opts)
	  post_wc.output(to_desc)
	  post_wc.input = self
	  post_wc
	end
	Fairy::def_job_interface Interface
      end

      def initialize(fairy, opts = nil)
	super
	@to_desc = nil
      end

      def backend_class_name
	"BWC::BPostFilter"
      end

      def output(vfn)
	@descripter = vfn
	@vfile = VFile.new
	@vfile.vfile_name = vfn
	backend.output(@vfile)
      end

      def input=(job)
	@input = job
	backend.input=job.backend

	backend.wait_all_output_finished
	@vfile.create_vfile
      end

    end
  end
end


