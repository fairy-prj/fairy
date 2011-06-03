
require "rubygems"

v = `ruby -Ilib -e 'require "fairy/version"; print Fairy::Version'`
v, p = v.scan(/^([0-9]+\.[0-9]+\.[0-9]+)-([0-9]+)/).first
if p.to_i > 1
  v += "."+p
end

Gem::Specification.new do |s|
  s.name = "fairy"
  s.authors = "Rakuten, Inc."
  s.email = "hajime.masuda@mail.rakuten.co.jp"
  s.platform = Gem::Platform::RUBY
  s.summary = "fairy is a framework for distributed processing in Ruby, originally designed at Rakuten Institute of Technology with Yukihiro Matsumoto, the founder of Ruby."
  s.rubyforge_project = s.name
  s.homepage = "http://code.google.com/p/fairy-prj/"
  s.version = v
  s.require_path = "lib"
  s.test_file = "spec/run_all.sh"
  s.executables = ["fairy", "fairy-cat", "fairy-cp", "fairy-rm"]
  s.default_executable = "fairy"

  s.files = ["Makefile", "README", "LICENSE", "fairy.gemspec", "lib/fairy.rb" ]
  s.files.concat Dir.glob("lib/fairy/**/*.rb")
  s.files.concat Dir.glob("lib/fairy/template/*.tmpl")
  s.files.concat ["etc/fairy.conf.tmpl"]
  s.files.concat Dir.glob("bin/{#{s.executables.grep(/.*[a-z]$/).join(",")}}")
  s.files.concat Dir.glob("bin/subcmd/*[A-Za-z]")
  s.files.concat Dir.glob("ext/{*.rb,*.c,*.h}")
  s.files.concat Dir.glob("doc/*.{rd,html}")
  s.files.concat Dir.glob("spec/{README,*.rb,run_all.sh}")
  s.files.concat Dir.glob("sample/*.rb")
  s.files.concat ["test/testc.rb"]
  s.files.concat Dir.glob("tools/**/*[a-z]")

  s.add_dependency("xthread", ">= 0.1.4.001")
  s.add_dependency("fiber-mon", ">= 0.2.1")
  s.add_dependency("DeepConnect", ">= 0.4.06")

  s.extensions = ["ext/extconf.rb"]
  
  s.description = <<EOF
fairy is a framework for distributed processing in Ruby, originally
designed at Rakuten Institute of Technology with Yukihiro Matsumoto,
the founder of Ruby.

Although fairy was inspired by MapReduce model, a well-known
programming model for distributed processing, it's more flexible and
suitable for wider use. That's due to fairy's programming model,
called filter IF, and various built-in filters.

fairy is implemented in Ruby and inherits its high productivity and
simplicity. fairy's API is quite similar to Ruby. Therefore most
programmers who know Ruby can easily understand and use it.
EOF
end

# Editor settings
# - Emacs -
# local variables:
# mode: Ruby
# end:
