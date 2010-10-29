
require "rubygems"

FAIRY_VER = `ruby -Ilib -e 'require "fairy/version"; print Fairy::Version'`

Gem::Specification.new do |s|
  s.name = "fairy"
  s.authors = "Rakuten, Inc."
  s.email = "hajime.masuda@mail.rakuten.co.jp"
  s.platform = Gem::Platform::RUBY
  s.summary = "fairy is a framework for destributed processing in Ruby, originaly desined at Rakuten institute of technology with Yukihiro Matsumoto, the founder of Ruby."
  s.rubyforge_project = s.name
  s.homepage = "http://code.google.com/p/fairy-prj/"
  s.version = FAIRY_VER.sub(/.*([0-9]+\.[0-9]+\.[0-9]+).*/, "\\1").chomp
  s.require_path = "lib"
  s.test_file = "spec/run_all.sh"
  s.executables = ["fairy", "fairy-cat", "fairy-cp", "fairy-rm"]
  s.default_executable = "fairy"

  s.files = ["Makefile", "README", "fairy.gemspec", "lib/fairy.rb" ]
  s.files.concat Dir.glob("lib/fairy/**/*.rb")
  s.files.concat Dir.glob("lib/fairy/template/*.tmpl")
  s.files.concat ["etc/fairy.conf.tmpl"]
  s.files.concat Dir.glob("bin/{#{s.executables.join(",")}}")
  s.files.concat Dir.glob("bin/subcmd/*[a-z]")
  s.files.concat Dir.glob("doc/*.{rd,html}")
  s.files.concat Dir.glob("spec/{README,*.rb,rub_all.sh}")
  s.files.concat Dir.glob("sample/*.rb")
  s.files.concat ["test/testc.rb"]
  s.files.concat Dir.glob("tools/**/*[a-z]")

  s.add_dependency("DeepConnect", ">= 0.4.06")
  s.add_dependency("fiber-mon", ">= 0.1.0")
  
  s.description = <<EOF
fairy is a framework for destributed processing in Ruby, originaly
desined at Rakuten institute of technology with Yukihiro Matsumoto,
the founder of Ruby.

Although fairy was inspired by MapReduce? model, a well-known
programming model for desributed processing, it's more flexible and
suitable for wider use. That's due to fairy's programming model,
called filter IF, and various buit-in filters.

fairy is implemented in Ruby and inherits high productivity and
simplicity from Ruby. It's API is quite similar to Ruby. Therefore
most programmers who know Ruby can easily understand and use it.
EOF
end

# Editor settings
# - Emacs -
# local variables:
# mode: Ruby
# end:
