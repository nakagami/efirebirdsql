REBAR = rebar3

all: compile

compile:
	@$(REBAR) update
	@$(REBAR) upgrade
	@$(REBAR) compile

clean:
	@$(REBAR) clean
	@rm -f build.plt
	@rm -f rebar.lock

test: compile
	@$(REBAR) eunit

dialyzer: build.plt compile
	dialyzer --plt $< _build/default/lib/efirebirdsql/ebin/

build.plt:
	dialyzer -q --build_plt --apps erts kernel stdlib crypto --output_plt $@

publish:
	@$(REBAR) hex publish
