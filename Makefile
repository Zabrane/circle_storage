ERL=$(shell which erl)
REBAR=./rebar
APP=iplib
PA=./ebin ./deps/*/ebin

.PHONY: deps

all: deps compile

compile:
	    @$(REBAR) compile
deps:
	    @$(REBAR) get-deps
clean:
	    @$(REBAR) clean
distclean: clean
	    @$(REBAR) delete-deps
generate:
	    @$(REBAR) generate
