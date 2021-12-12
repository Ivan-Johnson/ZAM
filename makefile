#!/usr/bin/make -f
INSTALL = /usr/bin/install -D
INSTALLDATA = /usr/bin/install -Dm 644

#https://www.gnu.org/software/make/manual/html_node/Directory-Variables.html
prefix ?= /usr/local
exec_prefix ?= $(prefix)
bindir ?= $(exec_prefix)/bin
sbindir ?= $(exec_prefix)/sbin
libexecdir ?= $(exec_prefix)/libexec
datarootdir ?= $(prefix)/share
datadir ?= $(datarootdir)
sysconfdir ?= $(prefix)/etc
sharedstatedir ?= $(prefix)/com
localstatedir ?= $(prefix)/var
runstatedir ?= $(localstatedir)/run
libdir ?= $(exec_prefix)/lib

#TODO: I made up these variable names/definitions; is there a standard I should follow?
systemduserdir ?= $(libdir)/sysusers.d
systemdservicedir ?= $(libdir)/systemd/system

mandir ?= $(datarootdir)/man
man1dir ?= $(mandir)/man1
man2dir ?= $(mandir)/man2
man3dir ?= $(mandir)/man3
man4dir ?= $(mandir)/man4
man5dir ?= $(mandir)/man5
man6dir ?= $(mandir)/man6
man7dir ?= $(mandir)/man7
man8dir ?= $(mandir)/man8

man1ext ?= .1
man2ext ?= .2
man3ext ?= .3
man4ext ?= .4
man5ext ?= .5
man6ext ?= .6
man7ext ?= .7
man8ext ?= .8

#https://www.gnu.org/software/make/manual/html_node/DESTDIR.html
#destdir ?=

utildir ?= utilities

datadir_zam ?= $(datadir)/zam

all:
	@echo '"make all" is a NOP'

.PHONY: install
install_utils:
	@echo Installing ZAM
	$(INSTALLDATA) $(utildir)/"example config.json" $(DESTDIR)$(datadir_zam)/"example config.json"

	$(INSTALLDATA) $(utildir)/user.conf $(DESTDIR)$(systemduserdir)/zam.conf
	$(INSTALLDATA) $(utildir)/systemd.service $(DESTDIR)$(systemdservicedir)/zam.service
