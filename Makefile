
CONF = etc/fairy.conf

BINSRCS = \
	bin/master \
	bin/node \
	bin/controller \
	bin/processor \
	\
	bin/fairy \
	bin/fairy-cp \
	bin/fairy-cat \
	bin/fairy-rm \
	bin/inspector


TEMPLS = $(wildcard lib/fairy/template/*.tmpl)

DEEPCONNECTSRCS = $(wildcard deep-connect/*.rb)

SRCS = Makefile ChangeLog TODO \
	$(CONF) \
	$(BINSRCS) \
	$(TMPLS) \
	$(wildcard lib/*.rb \
		   lib/fairy/*.rb \
		   lib/fairy/job/*.rb \
		   lib/fairy/backend/*.rb \
		   lib/fairy/node/*.rb \
		   lib/fairy/share/*.rb \
		   sample/*.rb \
		   sample/*/*.rb \
		   sample/*/*/*.rb \
		   test/*.rb)

TS = TimeStamps

FAIRYSHELL = bin/fairy
FAIRYSHELL_TEMPL = \
	lib/fairy/template/fairy-HEAD.templ \
	lib/fairy/share/conf.rb \
	lib/fairy/share/base-app.rb \
	lib/fairy/template/fairy-BODY.templ

bin/fairy: $(FAIRYSHELL_TEMPL)
	echo "Make $@"
	chmod +w $@
	cat $(FAIRYSHELL_TEMPL) > $@
	chmod +x,-w $@
#
test-clean:
	rm -fr test/Repos

# git
commit:	$(TS)/commit

$(TS)/commit: $(SRCS)
	git commit -a
	touch $(TS)/commit

diff:
	git diff

tag-%:
	echo "Make tag $*"
	tools/git-tag $*

push:	doc/programming-interface.html bin/fairy
	git push origin

push-dev:
	git push origin refs/heads/dev

push-tags:	
	git push --tags origin

doc/programming-interface.html: doc/programming-interface.rd
	tools/rd2html

# gem
gem:
	gem build fairy.gemspec

# tar archives
TGZ_FILES = $(SRCS) $(DEEPCONNECTSRCS)


SNAPSHOT = Snapshot

VERSION = $(shell ruby -r lib/fairy/version.rb -e "puts Fairy::Version")

PACKAGE_NAME = fairy

TAR_NAME = $(PACKAGE_NAME)-$(VERSION).tgz

tgz: $(SNAPSHOT)/$(TAR_NAME)

$(SNAPSHOT)/$(TAR_NAME): $(TGZ_FILES)
	@if [ ! -e $(SNAPSHOT) ]; then \
	    mkdir $(SNAPSHOT); \
	fi
	@echo "make $(TAR_NAME) in $(SNAPSHOT)"
	@tar zcf $(SNAPSHOT)/$(TAR_NAME) $(TGZ_FILES)
	@echo "copy $(TAR_NAME) to /tmp/Downloads"
	@cp -p $(SNAPSHOT)/$(TAR_NAME) /tmp/Downloads

