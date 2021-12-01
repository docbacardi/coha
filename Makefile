.PHONY: all clean

VERSION="0.0.1"


all: coha package

clean:
	rm -rf coha

coha: coha.go
	env GO111MODULE=on go build coha.go

package: coha coha.yaml.template
	tar cfvz coha-${VERSION}.tar.gz coha coha.yaml.template
