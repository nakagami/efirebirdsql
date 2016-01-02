%%% The MIT License (MIT)
%%% Copyright (c) 2015 Hajime Nakagami<nakagami@gmail.com>

-module(efirebirdsql_tests).

-compile([export_all]).

-include_lib("eunit/include/eunit.hrl").
-include("efirebirdsql.hrl").


connect_test() ->
    %% connect to bad database
    {error, _} = efirebirdsql:connect("localhost", "sysdba", "masterkey", "something_wrong_database"),
    %% crete new database
    {ok, C} = efirebirdsql:connect("localhost", "sysdba", "masterkey", "/tmp/erlang_test.fdb", [{createdb, true}]),
    ok = efirebirdsql:commit(C),
    ok = efirebirdsql:close(C).
