%%% The MIT License (MIT)
%%% Copyright (c) 2019 Hajime Nakagami<nakagami@gmail.com>

-module(efirebirdsql_protocol_tests).

-compile([export_all]).

-include_lib("eunit/include/eunit.hrl").
-include("efirebirdsql.hrl").

protocol_test() ->
    {ok, C} = efirebirdsql_protocol:connect(
        "localhost", "sysdba", "masterkey", "/tmp/erlang_test.fdb",
        [{createdb, true}, {auth_plugin, "Srp"}]).


