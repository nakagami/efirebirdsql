%%% The MIT License (MIT)
%%% Copyright (c) 2019 Hajime Nakagami<nakagami@gmail.com>

-module(efirebirdsql_protocol_tests).

-include_lib("eunit/include/eunit.hrl").
-include("efirebirdsql.hrl").

tmp_dbname() ->
    lists:flatten(io_lib:format("/tmp/~p.fdb", [erlang:system_time()])).

protocol_test() ->
    {ok, C1} = efirebirdsql_protocol:connect(
        "localhost", "sysdba", "masterkey", tmp_dbname(),
        [{createdb, true}, {auth_plugin, "Srp"}]),
    {ok, C2, Stmt} = efirebirdsql_protocol:allocate_statement(C1),

    {ok, C3, Stmt2} = efirebirdsql_protocol:prepare_statement(
        <<"SELECT rdb$relation_name, rdb$owner_name FROM rdb$relations WHERE rdb$system_flag=?">>, C2, Stmt),
    {ok, C4, Stmt3} = efirebirdsql_protocol:execute(C3, Stmt2, [1]),
    _Description = efirebirdsql_protocol:description(Stmt3),
    {ok, _Rows, C5} = efirebirdsql_protocol:fetchall(C4, Stmt3),
    ?assertEqual(
        efirebirdsql_protocol:columns(Stmt3),

        [{<<"RDB$RELATION_NAME">>,text,0,252,true},
         {<<"RDB$OWNER_NAME">>,text,0,252,true}]
    ),
    {ok, C6} = efirebirdsql_protocol:rollback(C5),
    {ok, _} = efirebirdsql_protocol:close(C6).

connect_test(_DbName, 0) ->
    ok;
connect_test(DbName, Count) ->
    {ok, C} = efirebirdsql_protocol:connect(
        "localhost", "sysdba", "masterkey", DbName,
        [{auth_plugin, "Srp"}]),
    efirebirdsql_protocol:close(C),
    connect_test(DbName, Count-1).
connect_test() ->
    DbName = tmp_dbname(),
    {ok, C} = efirebirdsql_protocol:connect(
        "localhost", "sysdba", "masterkey", DbName,
        [{createdb, true}, {auth_plugin, "Srp"}]),
    efirebirdsql_protocol:close(C),
    connect_test(DbName, 100).
