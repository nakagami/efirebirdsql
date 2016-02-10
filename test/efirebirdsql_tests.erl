%%% The MIT License (MIT)
%%% Copyright (c) 2015 Hajime Nakagami<nakagami@gmail.com>

-module(efirebirdsql_tests).

-compile([export_all]).

-include_lib("eunit/include/eunit.hrl").
-include("efirebirdsql.hrl").

create_testdb() ->
    %% crete new database
    {ok, C} = efirebirdsql:connect(
        "localhost", "sysdba", "masterkey", "/tmp/erlang_test.fdb",
        [{createdb, true}]),
    ok = efirebirdsql:execute(C, <<"
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
        )
    ">>),
    ok = efirebirdsql:execute(C, <<"insert into foo(a, b, c, h) values (1, 'b', 'c','blob')">>),
    ok = efirebirdsql:execute(C, <<"insert into foo(a, b, c, h) values (2, 'B', 'C','BLOB')">>),

    ok = efirebirdsql:execute(C, <<"
            CREATE PROCEDURE foo_proc ()
              AS
              BEGIN
              END
    ">>),
    ok = efirebirdsql:execute(C, <<"
            CREATE PROCEDURE bar_proc (param_a INTEGER, param_b VARCHAR(30))
              RETURNS (out1 INTEGER, out2 VARCHAR(30))
              AS
              BEGIN
                out1 = param_a;
                out2 = param_b;
              END
    ">>),
    efirebirdsql:close(C).

description() ->
    [{<<"A">>,long,0,4,false},
     {<<"B">>,varying,0,120,false},
     {<<"C">>,varying,0,4096,true},
     {<<"D">>,int64,-3,8,true},
     {<<"E">>,date,0,4,true},
     {<<"F">>,timestamp,0,8,true},
     {<<"G">>,time,0,4,true},
     {<<"H">>,blob,4,8,true},
     {<<"I">>,double,0,8,true},
     {<<"J">>,float,0,4,true}].

alias_description() ->
    [{<<"ALIAS_NAME">>,long,0,4,false}].

result1() ->
    [{<<"A">>,1},
     {<<"B">>,<<"b">>},
     {<<"C">>,<<"c">>},
     {<<"D">>,"-0.123"},
     {<<"E">>,{1967,8,11}},
     {<<"F">>,{{1967,8,11},{23,45,1,0}}},
     {<<"G">>,{23,45,1,0}},
     {<<"H">>,<<"blob">>},
     {<<"I">>,1.0},
     {<<"J">>,2.0}].

result2() ->
    [{<<"A">>,2},
     {<<"B">>,<<"B">>},
     {<<"C">>,<<"C">>},
     {<<"D">>,"-0.123"},
     {<<"E">>,{1967,8,11}},
     {<<"F">>,{{1967,8,11},{23,45,1,0}}},
     {<<"G">>,{23,45,1,0}},
     {<<"H">>,<<"BLOB">>},
     {<<"I">>,1.0},
     {<<"J">>,2.0}].

basic_test() ->
    create_testdb(),

    %% connect to bad database
    {error, ErrMsg} = efirebirdsql:connect(
        "localhost", "sysdba", "masterkey", "something_wrong_database", []),
    ?assertEqual(ErrMsg, <<"I/O error during 'open' operation for file 'something_wrong_database'\nError while trying to open file\nNo such file or directory">>),

    {ok, C} = efirebirdsql:connect(
        "localhost", "sysdba", "masterkey", "/tmp/erlang_test.fdb", []),

    %% execute bad query
    {error, ErrMsg2} = efirebirdsql:prepare(C, <<"bad query statement">>),
    {error, ErrMsg2} = efirebirdsql:execute(C, <<"bad query statement">>),

    %% field name
    ok = efirebirdsql:execute(C, <<"select a alias_name from foo">>),
    ?assertEqual(efirebirdsql:description(C), alias_description()),

    %% fetchall
    ok = efirebirdsql:execute(C, <<"select * from foo order by a">>),
    ?assertEqual(efirebirdsql:description(C), description()),
    {ok, ResultAll} = efirebirdsql:fetchall(C),
    ?assertEqual(ResultAll,  [result1(), result2()]),

    %% fetch one by one
    ok = efirebirdsql:execute(C, <<"select * from foo order by a">>),
    {ok, ResultOne} = efirebirdsql:fetchone(C),
    ?assertEqual(ResultOne, result1()),
    {ok, ResultTwo} = efirebirdsql:fetchone(C),
    ?assertEqual(ResultTwo, result2()),

    %% query with parameter
    ok = efirebirdsql:execute(C, <<"select * from foo where a=?">>, [1]),
    {ok, ResultA1} = efirebirdsql:fetchall(C),
    ?assertEqual(ResultA1, [result1()]),
    ok = efirebirdsql:execute(C, <<"select * from foo where b=?">>, [<<"B">>]),
    {ok, ResultB2} = efirebirdsql:fetchall(C),
    ?assertEqual(ResultB2, [result2()]),

    ok = efirebirdsql:close(C),

    %% commit and rollback
    {ok, C2} = efirebirdsql:connect(
        "localhost", "sysdba", "masterkey", "/tmp/erlang_test.fdb",
        [{auto_commit, false}]),
    ok = efirebirdsql:execute(C2, <<"update foo set c='C'">>),
    ok = efirebirdsql:execute(C2, <<"select * from foo where c='C'">>),
    {ok, ResultAll2} = efirebirdsql:fetchall(C2),
    ?assertEqual(length(ResultAll2),  2),
    ok = efirebirdsql:rollback(C2),
    ok = efirebirdsql:execute(C2, <<"select * from foo where c='C'">>),
    {ok, ResultAll3} = efirebirdsql:fetchall(C2),
    ?assertEqual(length(ResultAll3),  1),

    %% prepare and execute parameterized query
    ok = efirebirdsql:prepare(C2, <<"select * from foo where c=?">>),
    ok = efirebirdsql:execute(C2, [<<"C">>]),
    {ok, ResultAll4} = efirebirdsql:fetchall(C2),
    ?assertEqual(length(ResultAll4),  1),
    ok = efirebirdsql:execute(C2, [<<"c">>]),
    {ok, ResultAll5} = efirebirdsql:fetchall(C2),
    ?assertEqual(length(ResultAll5),  1),
    ok = efirebirdsql:prepare(C2, <<"select * from foo">>),
    ok = efirebirdsql:execute(C2),
    {ok, ResultAll6} = efirebirdsql:fetchall(C2),
    ?assertEqual(length(ResultAll6),  2),

    %% insert .. returning
    ok = efirebirdsql:execute(C2, <<"insert into foo(a, b) values (3, 'c') returning a, b">>),
    {ok, ResultReturning} = efirebirdsql:fetchone(C2),
    ?assertEqual(ResultReturning,  [{<<"A">>,3}, {<<"B">>,<<"c">>}]),

    ok = efirebirdsql:execute(C2, <<"EXECUTE PROCEDURE foo_proc">>),
    ok = efirebirdsql:execute(C2, <<"EXECUTE PROCEDURE bar_proc(4, 'd')">>),
    {ok, ResultProcedure} = efirebirdsql:fetchone(C2),
    ?assertEqual(ResultProcedure,  [{<<"OUT1">>,4}, {<<"OUT2">>,<<"d">>}]),

    ok = efirebirdsql:commit(C2),
    ok = efirebirdsql:close(C2).
