%%% The MIT License (MIT)
%%% Copyright (c) 2019 Hajime Nakagami<nakagami@gmail.com>

-module(efirebirdsql_protocol_tests).

-include_lib("eunit/include/eunit.hrl").
-include("efirebirdsql.hrl").

tmp_dbname() ->
    lists:flatten(io_lib:format("/tmp/~p.fdb", [erlang:system_time()])).

protocol_test() ->
    {ok, C1} = efirebirdsql_protocol:connect(
        "localhost", os:getenv("ISC_USER", "sysdba"), os:getenv("ISC_PASSWORD","masterkey"), tmp_dbname(),
        [{createdb, true}, {auth_plugin, "Srp"}]),
    ?assertEqual(C1#conn.auto_commit, true),

    {ok, C2, Stmt} = efirebirdsql_protocol:allocate_statement(C1),

    {ok, C3, Stmt2} = efirebirdsql_protocol:prepare_statement(
        <<"SELECT rdb$relation_name, rdb$owner_name FROM rdb$relations WHERE rdb$system_flag=?">>, C2, Stmt),
    {ok, C4, Stmt3} = efirebirdsql_protocol:execute(C3, Stmt2, [1]),
    _Description = efirebirdsql_protocol:description(Stmt3),

    {ok, C5, Stmt4} = efirebirdsql_protocol:prepare_statement(
        <<"SELECT rdb$relation_name, rdb$owner_name FROM rdb$relations WHERE rdb$system_flag=?">>, C4, Stmt3),
    {ok, C6, Stmt5} = efirebirdsql_protocol:execute(C5, Stmt4, [1]),

    {ok, _Rows, C7, Stmt6} = efirebirdsql_protocol:fetchall(C6, Stmt5),
    ?assertEqual(
        efirebirdsql_protocol:columns(Stmt6),

        [{<<"RDB$RELATION_NAME">>,text,0,252,true},
         {<<"RDB$OWNER_NAME">>,text,0,252,true}]
    ),

    {ok, C8, Stmt7} = efirebirdsql_protocol:prepare_statement(<<"
        CREATE TABLE foo (
            a INTEGER NOT NULL,
            b VARCHAR(30) NOT NULL UNIQUE,
            c VARCHAR(1024),
            d DECIMAL(16,3) DEFAULT -0.123,
            e DATE DEFAULT '1967-08-11',
            f TIMESTAMP DEFAULT '1967-08-11 23:45:01',
            g TIME DEFAULT '23:45:01',
            h BLOB SUB_TYPE 1,
            i DOUBLE PRECISION DEFAULT 1.0,
            j FLOAT DEFAULT 2.0,
            PRIMARY KEY (a),
            CONSTRAINT CHECK_A CHECK (a <> 0)
        )">>, C7, Stmt6),
    {ok, C9, Stmt8} = efirebirdsql_protocol:execute(C8, Stmt7, []),

    {ok, nil, C10, Stmt9} = efirebirdsql_protocol:fetchall(C9, Stmt8),

    {ok, C11, Stmt10} = efirebirdsql_protocol:prepare_statement(
        <<"INSERT INTO foo(a, b) VALUES(?, ?)">>, C10, Stmt9),
    {ok, C12, Stmt11} = efirebirdsql_protocol:execute(C11, Stmt10, [1, "b"]),
    ?assertEqual(Stmt11#stmt.rows, nil),
    {ok, C13, 1} = efirebirdsql_protocol:rowcount(C12, Stmt11),

    {ok, C14, Stmt12} = efirebirdsql_protocol:prepare_statement(
        <<"SELECT * FROM foo WHERE f = ?">>, C13, Stmt11),
    {ok, C15, Stmt13} = efirebirdsql_protocol:execute(C14, Stmt12, [{{1967, 8, 11}, {23, 45, 1, 0}}]),
    {ok, Rows, C16, Stmt14} = efirebirdsql_protocol:fetchall(C15, Stmt13),
    ?assertEqual(length(Rows),  1),
    {ok, C17, 1} = efirebirdsql_protocol:rowcount(C16, Stmt14),

    {ok, C18} = efirebirdsql_protocol:rollback(C17),
    {ok, _} = efirebirdsql_protocol:close(C18).


connect_test(_DbName, 0) ->
    ok;
connect_test(DbName, Count) ->
    {ok, C} = efirebirdsql_protocol:connect(
        "localhost", os:getenv("ISC_USER", "sysdba"), os:getenv("ISC_PASSWORD", "masterkey"), DbName,
        [{auth_plugin, "Srp"}]),
    efirebirdsql_protocol:close(C),
    connect_test(DbName, Count-1).
connect_test() ->
    DbName = tmp_dbname(),
    {ok, C} = efirebirdsql_protocol:connect(
        "localhost", os:getenv("ISC_USER", "sysdba"), os:getenv("ISC_PASSWORD", "masterkey"), DbName,
        [{createdb, true}, {auth_plugin, "Srp"}]),
    efirebirdsql_protocol:close(C),
    connect_test(DbName, 10).

connect_error_test() ->
    {error, ErrNo, Reason, _Conn} = efirebirdsql_protocol:connect("localhost", os:getenv("ISC_USER", "sysdba"), os:getenv("ISC_PASSWORD", "masterkey"), "something_wrong_database", []),
    ?assertEqual(ErrNo, 335544734),
    ?assertEqual(Reason, <<"I/O error during 'open' operation for file 'something_wrong_database'\nError while trying to open file\nNo such file or directory">>).
