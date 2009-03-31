
CONF = etc/fairy.conf

BINSRCS = \
	bin/master \
	bin/node \
	bin/controller \
	bin/processor

DEEPCONNECTSRCS = $(wildcard deep-connect/*.rb)

SRCS = Makefile ChangeLog TODO \
	$(CONF) \
	$(BINSRCS) \
	$(wildcard *.rb \
		   front/*.rb \
		   job/*.rb \
		   backend/*.rb \
		   node/*.rb \
		   share/*.rb \
		   sample/*.rb \
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

push:	
	git push ssh://git-keiju@www.sota.me/var/www/html/fairy/fairy.git

push-tags:	
	git push --tags ssh://git-keiju@www.sota.me/var/www/html/fairy/fairy.git


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

