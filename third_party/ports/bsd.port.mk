# bsd.port.mk - FreeBSD-style ports infrastructure for third-party libraries
#
# Usage: In your package's Makefile, define:
#   PORTNAME=    package name
#   DISTNAME=    archive name (may differ from PORTNAME)
#   VERSION=     version number
#   DISTURL=     download URL
#   SRCDIR=      source directory after extraction
#   BUILD_COMMANDS= shell commands to run during build phase
#
# Then include this file:
#   .include "${.CURDIR}/../bsd.port.mk"

# Ports directory
PORTSDIR?=	${CURDIR}/..

# Directories
DISTDIR?=	${PORTSDIR}/distfiles
WRKDIR?=	${PORTSDIR}/work/${PORTNAME}
DESTDIR?=	${PORTSDIR}/destdir/${PORTNAME}

# Number of parallel jobs
MAKE_JOBS?=	$(shell nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)

# Default targets
all: update

fetch:
	@echo "Fetching ${PORTNAME} ${VERSION}..."
	@python3 ${PORTSDIR}/pkg ${PORTNAME} fetch

extract:
	@echo "Extracting ${PORTNAME}..."
	@python3 ${PORTSDIR}/pkg ${PORTNAME} extract

patch:
	@echo "Applying patches..."
	@python3 ${PORTSDIR}/pkg ${PORTNAME} patch

configure:
	@echo "Configuring..."

build:
	@echo "Building ${PORTNAME}..."
	@python3 ${PORTSDIR}/pkg ${PORTNAME} build

install:
	@echo "Installing ${PORTNAME}..."
	@python3 ${PORTSDIR}/pkg ${PORTNAME} install

update:
	@echo "Updating ${PORTNAME}..."
	@python3 ${PORTSDIR}/pkg ${PORTNAME} update

clean:
	@echo "Cleaning..."
	@rm -rf ${WRKDIR}
	@rm -rf ${DESTDIR}

distclean:
	@echo "Cleaning distfiles..."
	@rm -f ${DISTDIR}/${DISTNAME}-${VERSION}.*

reinstall: install

.PHONY: all fetch extract patch configure build install update clean distclean reinstall
