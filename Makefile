GOCMD=go
GOBUILD=$(GOCMD) build
BINARY_NAME=gofiledb.out

all: clean build test
build:
	$(GOBUILD) -o $(BINARY_NAME)
run: build
	./$(BINARYNAME)
clean:
	rm -f $(BINARY_NAME)
test:
	go test -vv

