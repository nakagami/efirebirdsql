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
    efirebirdsql:close(C).

description() ->
    [{column,<<"A">>,long,0,4,false},
     {column,<<"B">>,varying,0,120,false},
     {column,<<"C">>,varying,0,4096,true},
     {column,<<"D">>,int64,-3,8,true},
     {column,<<"E">>,date,0,4,true},
     {column,<<"F">>,timestamp,0,8,true},
     {column,<<"G">>,time,0,4,true},
     {column,<<"H">>,blob,4,8,true},
     {column,<<"I">>,double,0,8,true},
     {column,<<"J">>,float,0,4,true}].

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
    {error, _} = efirebirdsql:connect(
        "localhost", "sysdba", "masterkey", "something_wrong_database"),

    {ok, C} = efirebirdsql:connect(
        "localhost", "sysdba", "masterkey", "/tmp/erlang_test.fdb"),

    ok = efirebirdsql:execute(C, <<"select * from foo order by a">>),
    ?assertEqual(efirebirdsql:description(C), description()),
    {ok, ResultAll} = efirebirdsql:fetchall(C),
    ?assertEqual(ResultAll,  [result1(), result2()]),

    ok = efirebirdsql:execute(C, <<"select * from foo order by a">>),
    {ok, ResultOne} = efirebirdsql:fetchone(C),
    ?assertEqual(ResultOne, result1()),
    {ok, ResultTwo} = efirebirdsql:fetchone(C),
    ?assertEqual(ResultTwo, result2()),

    ok = efirebirdsql:commit(C),
    ok = efirebirdsql:close(C).
