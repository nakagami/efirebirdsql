%%% The MIT License (MIT)
%%% Copyright (c) 2019 Hajime Nakagami<nakagami@gmail.com>

-module(efirebirdsql_protocol_tests).

-include_lib("eunit/include/eunit.hrl").
-include("efirebirdsql.hrl").

tmp_dbname() ->
    lists:flatten(io_lib:format("/tmp/~p.fdb", [erlang:system_time()])).

protocol_test() ->
    {ok, Conn} = efirebirdsql_protocol:connect(
        "localhost", os:getenv("ISC_USER", "sysdba"), os:getenv("ISC_PASSWORD","masterkey"), tmp_dbname(),
        [{createdb, true}, {auth_plugin, "Srp"}]),
    ?assertEqual(Conn#conn.auto_commit, true),

    {ok, C} = efirebirdsql_protocol:begin_transaction(true, Conn),
    {ok, Stmt} = efirebirdsql_protocol:allocate_statement(C),

    {ok, Stmt2} = efirebirdsql_protocol:prepare_statement(
        <<"SELECT rdb$relation_name, rdb$owner_name FROM rdb$relations WHERE rdb$system_flag=?">>, C, Stmt),
    {ok, Stmt3} = efirebirdsql_protocol:execute(C, Stmt2, [1]),
    _Description = efirebirdsql_protocol:description(Stmt3),

    {ok, Stmt4} = efirebirdsql_protocol:prepare_statement(
        <<"SELECT RDB$RELATION_ID, RDB$EXTERNAL_FILE FROM rdb$relations WHERE rdb$system_flag=?">>, C, Stmt3),
    {ok, Stmt5} = efirebirdsql_protocol:execute(C, Stmt4, [1]),

    {ok, _Rows, Stmt6} = efirebirdsql_protocol:fetchall(C, Stmt5),
    ?assertEqual(
        efirebirdsql_protocol:columns(Stmt6),

        [{<<"RDB$RELATION_ID">>,short,0,2,true},
         {<<"RDB$EXTERNAL_FILE">>,varying,0,255,true}]
    ),

    {ok, Stmt7} = efirebirdsql_protocol:prepare_statement(<<"
        CREATE TABLE foo (
            a INTEGER NOT NULL,
            b VARCHAR(30) NOT NULL UNIQUE,
            c VARCHAR(1024),
            d DECIMAL(16,3) DEFAULT -0.123,
            e DATE DEFAULT '1967-08-11',
            f TIMESTAMP DEFAULT '1967-08-11 23:45:01',
            g TIME DEFAULT '23:45:01',
            h0 BLOB SUB_TYPE 0,
            h1 BLOB SUB_TYPE 1,
            i DOUBLE PRECISION DEFAULT 1.0,
            j FLOAT DEFAULT 2.0,
            PRIMARY KEY (a),
            CONSTRAINT CHECK_A CHECK (a <> 0)
        )">>, C, Stmt6),
    {ok, Stmt8} = efirebirdsql_protocol:execute(C, Stmt7, []),

    {ok, nil, Stmt9} = efirebirdsql_protocol:fetchall(C, Stmt8),

    {ok, Stmt10} = efirebirdsql_protocol:prepare_statement(
        <<"INSERT INTO foo(a, b) VALUES(?, ?)">>, C, Stmt9),
    {ok, Stmt11} = efirebirdsql_protocol:execute(C, Stmt10, [1, "b"]),
    ?assertEqual(Stmt11#stmt.rows, nil),
    {ok, 1} = efirebirdsql_protocol:rowcount(C, Stmt11),

    {ok, Stmt12} = efirebirdsql_protocol:prepare_statement(
        <<"SELECT * FROM foo WHERE f = ?">>, C, Stmt11),
    {ok, Stmt13} = efirebirdsql_protocol:execute(C, Stmt12, [{{1967, 8, 11}, {23, 45, 1, 0}}]),
    {ok, Rows, Stmt14} = efirebirdsql_protocol:fetchall(C, Stmt13),
    ?assertEqual(length(Rows),  1),
    {ok, 1} = efirebirdsql_protocol:rowcount(C, Stmt14),

    {ok, Stmt15} = efirebirdsql_protocol:prepare_statement(
        <<"UPDATE foo SET b=? WHERE a=?">>, C, Stmt14),
    {ok, Stmt16} = efirebirdsql_protocol:execute(C, Stmt15, ["c", 1]),
    {ok, 1} = efirebirdsql_protocol:rowcount(C, Stmt16),

    ok = efirebirdsql_protocol:rollback_retaining(C),
    {ok, _} = efirebirdsql_protocol:close(C).


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
