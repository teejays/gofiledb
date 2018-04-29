GOCMD=go
GOBUILD=$(GOCMD) build
BINARY_NAME=gofiledb.out

all: clean build
build:
	$(GOBUILD) -o $(BINARY_NAME)
run: build
	./$(BINARYNAME)
clean:
	rm -f $(BINARY_NAME)

