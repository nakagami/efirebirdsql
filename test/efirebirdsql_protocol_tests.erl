%%% The MIT License (MIT)
%%% Copyright (c) 2019 Hajime Nakagami<nakagami@gmail.com>

-module(efirebirdsql_protocol_tests).

-compile([export_all]).

-include_lib("eunit/include/eunit.hrl").
-include("efirebirdsql.hrl").

tmp_dbname() ->
    lists:flatten(io_lib:format("/tmp/~p.fdb", [erlang:system_time()])).

protocol_test() ->
    {ok, C1} = efirebirdsql_protocol:connect(
        "localhost", "sysdba", "masterkey", "/tmp/erlang_protocol_test.fdb",
        [{createdb, true}, {auth_plugin, "Srp"}]),
    {ok, C2} = efirebirdsql_protocol:begin_transaction(true, C1),
    {ok, C3, Stmt} = efirebirdsql_protocol:allocate_statement(C2),

    {ok, C4, Stmt2} = efirebirdsql_protocol:prepare_statement(
        <<"SELECT * FROM RDB$RELATIONS WHERE RDB$SYSTEM_FLAG=?">>, C3, Stmt),
    {ok, C5, Stmt3} = efirebirdsql_protocol:execute(C4, Stmt2, [1]),
    _Description = efirebirdsql_protocol:description(Stmt3),
    {ok, Rows, C6} = efirebirdsql_protocol:fetchall(C5, Stmt3),
%    io:format("Rows=~p~n", [Rows]),

    {ok, C7} = efirebirdsql_protocol:rollback(C6),
    {ok, _} = efirebirdsql_protocol:close(C7).

