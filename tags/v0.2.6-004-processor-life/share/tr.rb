require "tracer"
Tracer.add_filter do |event, file, line, id, binding, klass|
  file =~ /^\./ && file !~ /deep-connect/
end

Tracer.display_process_id = true
Tracer.display_thread_id = true
Tracer.display_c_call = false
	  
Tracer.on

