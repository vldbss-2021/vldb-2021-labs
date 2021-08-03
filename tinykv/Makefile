SHELL := /bin/bash
PROJECT=tinykv
GOPATH ?= $(shell go env GOPATH)

# Ensure GOPATH is set before running build process.
ifeq "$(GOPATH)" ""
  $(error Please set the environment variable GOPATH before running `make`)
endif

GO                  := GO111MODULE=on go
GOBUILD             := $(GO) build $(BUILD_FLAG) -tags codes
GOTEST              := $(GO) test -v --count=1 --parallel=1 -p=1

TEST_LDFLAGS        := ""

PACKAGE_LIST        := go list ./...| grep -vE "cmd"
PACKAGES            := $$($(PACKAGE_LIST))

# Targets
.PHONY: clean test proto kv scheduler dev

default: kv scheduler

dev: default test

test:
	@echo "Running tests in native mode."
	@export TZ='Asia/Shanghai'; \
	LOG_LEVEL=fatal $(GOTEST) -timeout 20m $(PACKAGES)

CURDIR := $(shell pwd)
export PATH := $(CURDIR)/bin/:$(PATH)
proto:
	mkdir -p $(CURDIR)/bin
	(cd proto && ./generate_go.sh)
	GO111MODULE=on go build ./proto/pkg/...

kv:
	$(GOBUILD) -o bin/tinykv-server kv/main.go

scheduler:
	$(GOBUILD) -o bin/tinyscheduler-server scheduler/main.go

deploy-cluster:
	$(GOBUILD) -o bin/cluster deploy/main.go

ci: default test
	@echo "Checking formatting"
	@test -z "$$(gofmt -s -l $$(find . -name '*.go' -type f -print) | tee /dev/stderr)"
	@echo "Running Go vet"
	@go vet ./...

format:
	@gofmt -s -w `find . -name '*.go' -type f ! -path '*/_tools/*' -print`

project1:
	$(GOTEST) ./kv/server -run 1 

project2: project2a project2b project2c

project2a:
	$(GOTEST) ./raft -run 2A

project2aa:
	$(GOTEST) ./raft -run 2AA

project2ab:
	$(GOTEST) ./raft -run 2AB

project2ac:
	$(GOTEST) ./raft -run 2AC

project2b:
	$(GOTEST) ./kv/test_raftstore -run 2B

project2c:
	$(GOTEST) ./raft ./kv/test_raftstore -run 2C

project3: project3a project3b project3c

project3a:
	$(GOTEST) ./raft -run 3A

project3b:
	$(GOTEST) ./kv/test_raftstore -run 3B

project3c:
	$(GOTEST) ./scheduler/server ./scheduler/server/schedulers -check.f="3C"

project4: project4a project4b project4c

project4a:
	$(GOTEST) ./kv/transaction/... -run 4A

project4b:
	$(GOTEST) ./kv/transaction/... -run 4B

project4c:
	$(GOTEST) ./kv/transaction/... -run 4C

clean:
	rm /tmp/test-raftstore* -rf

###
# The lab1 related tests. It's better to pass the part "a" tests and then try to
# run the part "b" tests, as the part "a" tests are common path test without
# failures or fault injections.
lab1P0:
	$(GOTEST) ./kv/server -run 1

# P1 is the basic test for the raft store.
lab1P1a:
	$(GOTEST) ./kv/test_raftstore -run TestBasic2BLab1P1a

lab1P1b:
	$(GOTEST) ./kv/test_raftstore -run Lab1P1b

# P2 is to test the persistency of the raft store that states will not get lost
# after restarts.
lab1P2a:
	$(GOTEST) ./kv/test_raftstore -run TestPersistOneClient2BLab1P2a

lab1P2b:
	$(GOTEST) ./kv/test_raftstore -run Lab1P2b

# P3 is to test the snapshot utility of the raft store.	Including the snapshot
# generation, communication and apply, also the recovery from the snapshot.
lab1P3a:
	$(GOTEST) ./kv/test_raftstore -run TestOneSnapshot2BLab1P3a

lab1P3b:
	$(GOTEST) ./kv/test_raftstore -run Lab1P3b

# P4 is to test the configuration changes including leader transfer, add peer and
# remove peer.
lab1P4a:
	$(GOTEST) ./kv/test_raftstore -run Lab1P4a

lab1P4b:
	$(GOTEST) ./kv/test_raftstore -run Lab1P4b

###
# The lab2 related tests.
lab2P1:
	$(GOTEST) ./kv/transaction/commands -run Lab2P1

lab2P2:
	$(GOTEST) ./kv/transaction/commands -run Lab2P2

lab2P3:
	$(GOTEST) ./kv/transaction/commands -run Lab2P3

lab2P4:
	$(GOTEST) ./kv/transaction/... -run 4
