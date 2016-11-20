# Makefile for a go project
#
# Author: Jon Eisen
# 	site: joneisen.me
# 	
# Targets:
# 	all: Builds the code
# 	build: Builds the code
# 	fmt: Formats the source files
# 	clean: cleans the code
# 	install: Installs the code to the GOPATH
# 	iref: Installs referenced projects
#	test: Runs the tests
#	
#  Blog post on it: http://joneisen.me/post/25503842796
#

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOGET=$(GOCMD) get
GOCLEAN=$(GOCMD) clean
GOINSTALL=$(GOCMD) install
GOTEST=$(GOCMD) test
GODEP=$(GOTEST) -i
GOFMT=gofmt -w

# Package lists
INT_LIST := ""# myinterface another/impl	#<-- Interface directories
#IMPL_LIST := myimplementation another/pkg/impl	#<-- Implementation directories
#CMD_LIST := mycmd1 another/pkg/mycmd1	#<-- Command directories

# List building
ALL_LIST = $(INT_LIST) $(IMPL_LIST) $(CMD_LIST)

BUILD_LIST = $(foreach int, $(ALL_LIST), $(int)_build)
CLEAN_LIST = $(foreach int, $(ALL_LIST), $(int)_clean)
INSTALL_LIST = $(foreach int, $(ALL_LIST), $(int)_install)
TEST_LIST = $(foreach int, $(ALL_LIST), $(int)_test)

# All are .PHONY for now because dependencyness is hard
.PHONY: $(CLEAN_LIST) $(TEST_LIST) $(FMT_LIST) $(INSTALL_LIST) $(BUILD_LIST) $(IREF_LIST)

all: build
build: $(BUILD_LIST)
clean: $(CLEAN_LIST)
install: $(INSTALL_LIST)
test: $(TEST_LIST)

$(BUILD_LIST):
	$(GOGET)
	$(GOBUILD)
$(CLEAN_LIST):
	$(GOCLEAN)
$(INSTALL_LIST):
	$(GOINSTALL)
$(TEST_LIST):
	$(GOTEST)
