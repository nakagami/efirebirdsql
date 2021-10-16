REBAR = rebar3

all: compile

compile:
	@$(REBAR) update
	@$(REBAR) upgrade
	@$(REBAR) compile

clean:
	@$(REBAR) clean
	@rm -rf ebin/*
	@rm -f build.plt
	@rm -f rebar.lock

test: compile
	@$(REBAR) eunit

dialyzer: build.plt compile
	dialyzer --plt $< ebin

build.plt:
	dialyzer -q --build_plt --apps erts kernel stdlib crypto --output_plt $@

