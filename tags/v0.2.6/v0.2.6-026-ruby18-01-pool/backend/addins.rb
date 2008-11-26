

backend_dir = File.basename(File.dirname(__FILE__))
for backend in Dir.glob("#{backend_dir}/*.rb")
#  next if backend == __FILE__

#  puts "require #{backend}"
  require backend
end


# require "backend/bfile"
# require "backend/b-local-file-input"
# require "backend/b-input-iota"
# require "backend/b-there"

# require "backend/b-file-output"
# require "backend/b-local-file-output"
# require "backend/bhere"

# require "backend/b-each-element-mapper"
# require "backend/b-each-substream-mapper"
# require "backend/b-each-element-selector"
# require "backend/b-group-by"
# require "backend/b-zipper"
# require "backend/b-splitter"
# require "backend/b-shuffle"
# require "backend/b-barrier"


