%%% The MIT License (MIT)
%%% Copyright (c) 2015 Hajime Nakagami<nakagami@gmail.com>

-module(efirebirdsql_tests).

-compile([export_all]).

-include_lib("eunit/include/eunit.hrl").
-include("efirebirdsql.hrl").


connect_test() ->
    %% connect to bad database
    {error, _} = efirebirdsql:connect(
        "localhost", "sysdba", "masterkey", "something_wrong_database"),
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
    ok = efirebirdsql:execute(C, <<"select * from foo">>),
    ?assertEqual(length(efirebirdsql:description(C)), 10),
    {ok, R} = efirebirdsql:fetchall(C),
    ?assertEqual(length(R), 2),
    ok = efirebirdsql:commit(C),
    ok = efirebirdsql:close(C).
