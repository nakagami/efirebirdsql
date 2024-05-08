%%% The MIT License (MIT)
%%% Copyright (c) 2015-2019 Hajime Nakagami<nakagami@gmail.com>

-module(efirebirdsql_tests).

-include_lib("eunit/include/eunit.hrl").
-include("efirebirdsql.hrl").

tmp_dbname() ->
    lists:flatten(io_lib:format("/tmp/~p.fdb", [erlang:system_time()])).

get_major_version(Conn) ->
    ok = efirebirdsql:execute(Conn, <<"select cast(LEFT(rdb$get_context('SYSTEM', 'ENGINE_VERSION'), 1) as int) from rdb$database;">>),
    {ok, [{_, MajorVersion}]} = efirebirdsql:fetchone(Conn),
    MajorVersion.

create_test_db(DbName) ->
    %% crete new database
    {ok, C} = efirebirdsql:connect(
        "localhost", os:getenv("ISC_USER", "sysdba"), os:getenv("ISC_PASSWORD", "masterkey"), DbName,
        [{createdb, true}, {timezone, "Asia/Tokyo"}]),
    C.

create_test_tables(C) ->
    %% crete new database
    ok = efirebirdsql:execute(C, <<"
        CREATE TABLE foo (
            a INTEGER NOT NULL,
            b VARCHAR(30) NOT NULL UNIQUE,
            c CHAR(1024),
            d DECIMAL(16,3) DEFAULT -0.123,
            e DATE DEFAULT '1967-08-11',
            f TIMESTAMP DEFAULT '1967-08-11 23:45:01',
            g TIME DEFAULT '23:45:01',
            h0 BLOB SUB_TYPE 0,
            h1 BLOB SUB_TYPE 1,
            i DOUBLE PRECISION DEFAULT 1.0,
            j FLOAT DEFAULT 2.0,
            k BIGINT DEFAULT 123456789999999999,
            PRIMARY KEY (a),
            CONSTRAINT CHECK_A CHECK (a <> 0)
        )
    ">>),
    ok = efirebirdsql:execute(C, <<"insert into foo(a, b, c, h0, h1) values (?,?,?,?,?)">>, [1, <<"b">>, <<"c">>, nil, <<"blob">>]),
    ok = efirebirdsql:execute(C, <<"insert into foo(a, b, c, h1, k) values (?,?,?,?)">>, [2, <<"B">>, <<"C">>, <<"BLOB">>, 123456780000000000]),

    ok = efirebirdsql:execute(C, <<"
            CREATE PROCEDURE foo_proc
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
    ">>).

description() ->
    [{<<"A">>,long,0,4,false},
     {<<"B">>,varying,0,120,false},
     {<<"C">>,text,0,4096,true},
     {<<"D">>,int64,-3,8,true},
     {<<"E">>,date,0,4,true},
     {<<"F">>,timestamp,0,8,true},
     {<<"G">>,time,0,4,true},
     {<<"H0">>,blob,0,8,true},
     {<<"H1">>,blob,4,8,true},
     {<<"I">>,double,0,8,true},
     {<<"J">>,float,0,4,true},
     {<<"K">>,int64,0,8,true}].

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
     {<<"H0">>,nil},
     {<<"H1">>,<<"blob">>},
     {<<"I">>,1.0},
     {<<"J">>,2.0},
     {<<"K">>,123456789999999999}].

result2() ->
    [{<<"A">>,2},
     {<<"B">>,<<"B">>},
     {<<"C">>,<<"C">>},
     {<<"D">>,"-0.123"},
     {<<"E">>,{1967,8,11}},
     {<<"F">>,{{1967,8,11},{23,45,1,0}}},
     {<<"G">>,{23,45,1,0}},
     {<<"H0">>,nil},
     {<<"H1">>,<<"BLOB">>},
     {<<"I">>,1.0},
     {<<"J">>,2.0},
     {<<"K">>,123456780000000000}].

basic_test() ->
    %% connect to bad database
    {error, ErrMsg} = efirebirdsql:connect(
        "localhost", os:getenv("ISC_USER", "sysdba"), os:getenv("ISC_PASSWORD", "masterkey"), "something_wrong_database", []),
    ?assertNotEqual(ErrMsg, nil),
    DbName = tmp_dbname(),
    CreatedConn = create_test_db(DbName),
    create_test_tables(CreatedConn),
    efirebirdsql:close(CreatedConn),

    {ok, C} = efirebirdsql:connect(
        "localhost", os:getenv("ISC_USER", "sysdba"), os:getenv("ISC_PASSWORD", "masterkey"), DbName, []),
    ok = efirebirdsql:ping(C),

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

    %% prepare and multi execute
    ok = efirebirdsql:prepare(C, <<"select * from foo order by a">>),
    ok = efirebirdsql:execute(C, []),
    {ok, ResultAll} = efirebirdsql:fetchall(C),
    ok = efirebirdsql:execute(C, []),
    {ok, ResultAll} = efirebirdsql:fetchall(C),

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
        "localhost", os:getenv("ISC_USER", "sysdba"), os:getenv("ISC_PASSWORD", "masterkey"), DbName,
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

    %% fetch null value
    ok = efirebirdsql:execute(C2, <<"select a,c from foo where A=3">>),
    {ok, ResultNull} = efirebirdsql:fetchone(C2),
    ?assertEqual(ResultNull,  [{<<"A">>,3}, {<<"C">>,nil}]),

    %% procedure call
    ok = efirebirdsql:execute(C2, <<"EXECUTE PROCEDURE foo_proc">>),
    ok = efirebirdsql:execute(C2, <<"EXECUTE PROCEDURE bar_proc(4, 'd')">>),
    {ok, ResultProcedure} = efirebirdsql:fetchone(C2),
    ?assertEqual(ResultProcedure,  [{<<"OUT1">>,4}, {<<"OUT2">>,<<"d">>}]),

    %% Fetch null value issue #6
    {ok, C3} = efirebirdsql:connect(
        "localhost", os:getenv("ISC_USER", "sysdba"), os:getenv("ISC_PASSWORD", "masterkey"), tmp_dbname(),
        [{createdb, true}]),
    ok = efirebirdsql:execute(C3, <<"create table TestTable (ID int, testvalue int)">>),
    ok = efirebirdsql:execute(C3, <<"insert into TestTable (ID, testvalue) values (2, null)">>),
    ok = efirebirdsql:execute(C3, <<"select * from TestTable">>),
    {ok, ResultHasNull} = efirebirdsql:fetchall(C3),
    ?assertEqual(ResultHasNull,  [[{<<"ID">>,2}, {<<"TESTVALUE">>,nil}]]).

many_column_test() ->
    DbName = tmp_dbname(),
    C = create_test_db(DbName),
    ok = efirebirdsql:execute(C, <<"
        CREATE TABLE ABCDEFGHIJKLMNOPQRSTUV(
            A varchar(10),
            B varchar(10),
            C varchar(10),
            D varchar(10),
            E varchar(10),
            F varchar(10),
            G varchar(10),
            H varchar(10),
            I varchar(10),
            J varchar(10),
            K varchar(10),
            L varchar(10),
            M varchar(10),
            N varchar(10),
            O varchar(10),
            P varchar(10),
            Q varchar(10),
            R varchar(10),
            S varchar(10),
            T varchar(10),
            U varchar(10),
            V varchar(10),
            W varchar(10),
            X varchar(10),
            Y varchar(10),
            Z varchar(10),
            RFCEMPRESA varchar(20) NOT NULL,
            NOSUCURSAL integer NOT NULL,
            TIPO integer NOT NULL,
            SERIE varchar(5) NOT NULL,
            NODOCTO integer NOT NULL,
            LINEA integer NOT NULL,
            CODART varchar(20),
            NOMART varchar(80),
            CLAVEPRODSERV varchar(10),
            UNIDADCLAVE varchar(10),
            UNIDADNOMBRE varchar(80),
            CANT1 double precision,
            CATN2 double precision,
            PUNIT double precision,
            MONTO double precision,
            IMPTO1 double precision,
            IMPTO2 double precision,
            PIMPTO1 double precision,
            PIMPTO2 double precision,
            TIMPTO1 varchar(10),
            TIMPTO2 varchar(10),
            TFIMPTO1 varchar(10),
            TFIMPTO2 varchar(10),
            PDESCTO double precision,
            IDESCTO double precision
        )
    ">>),
    ok = efirebirdsql:execute(C, <<"
        SELECT * FROM ABCDEFGHIJKLMNOPQRSTUV
    ">>),
    ?assertEqual(length(efirebirdsql:description(C)), 51),
    {ok, Result} = efirebirdsql:fetchall(C),
    ?assertEqual(length(Result), 0).

fb3_test() ->
    DbName = tmp_dbname(),
    CreatedConn = create_test_db(DbName),
    FirebirdMajorVersion = get_major_version(CreatedConn),
    if
    FirebirdMajorVersion >= 3 ->
        create_test_tables(CreatedConn),
        efirebirdsql:close(CreatedConn),
        {ok, C} = efirebirdsql:connect(
            "localhost", os:getenv("ISC_USER", "sysdba"), os:getenv("ISC_PASSWORD", "masterkey"), DbName, []),
        ok = efirebirdsql:execute(C, <<"select True AS C from rdb$relations">>),
        ?assertEqual({ok, [{<<"C">>, true}]}, efirebirdsql:fetchone(C)),
        ok = efirebirdsql:execute(C, <<"select False AS C from rdb$relations">>),
        ?assertEqual({ok, [{<<"C">>, true}]}, efirebirdsql:fetchone(C)),

        ok = efirebirdsql:commit(C),
        ok = efirebirdsql:close(C);
    FirebirdMajorVersion < 3 ->
        ok
    end.


create_fb4_test_tables(C) ->
    %% crete new database
    ok = efirebirdsql:execute(C, <<"
        CREATE TABLE dec_test (
            d DECIMAL(20, 2),
            df64 DECFLOAT(16),
            df128 DECFLOAT(34),
            s varchar(32))
    ">>),
    ok = efirebirdsql:execute(C, <<"insert into dec_test(d, df64, df128, s) values (0.0, 0.0, 0.0, '0.0')">>),
    ok = efirebirdsql:execute(C, <<"insert into dec_test(d, df64, df128, s) values (1.0, 1.0, 1.0, '1.0')">>),
    ok = efirebirdsql:execute(C, <<"insert into dec_test(d, df64, df128, s) values (20.0, 20.0, 20.0, '20.0')">>),
    ok = efirebirdsql:execute(C, <<"insert into dec_test(d, df64, df128, s) values (-1.0, -1.0, -1.0, '-1.0')">>),
    ok = efirebirdsql:execute(C, <<"insert into dec_test(d, df64, df128, s) values (-20.0, -20.0, -20.0, '-20.0')">>),

    ok = efirebirdsql:execute(C, <<"
        CREATE TABLE tz_test (
            id INTEGER NOT NULL,
            t TIME WITH TIME ZONE DEFAULT '12:34:56',
            ts TIMESTAMP WITH TIME ZONE DEFAULT '1967-08-11 23:45:01',
            PRIMARY KEY (id)
        )
    ">>),
    ok = efirebirdsql:execute(C, <<"insert into tz_test (id) values (1)">>),
    ok = efirebirdsql:execute(C, <<"insert into tz_test (id, t, ts) values (2, '12:34:56 Asia/Seoul', '1967-08-11 23:45:01.0000 Asia/Seoul')">>),
    ok = efirebirdsql:execute(C, <<"insert into tz_test (id, t, ts) values (3, '03:34:56 UTC', '1967-08-11 14:45:01.0000 UTC')">>).

fb4_test() ->
    DbName = tmp_dbname(),
    CreatedConn = create_test_db(DbName),
    FirebirdMajorVersion = get_major_version(CreatedConn),
    if
    FirebirdMajorVersion >= 4 ->
        create_fb4_test_tables(CreatedConn),
        efirebirdsql:close(CreatedConn),
        {ok, C} = efirebirdsql:connect(
            "localhost", os:getenv("ISC_USER", "sysdba"), os:getenv("ISC_PASSWORD", "masterkey"), DbName, [{timezone, "Asia/Tokyo"}]),
        ok = efirebirdsql:execute(C, <<"select * from dec_test">>),
        {ok, ResultDecFloat} = efirebirdsql:fetchall(C),
        ?assertEqual([
            [{<<"D">>,"0.00"}, {<<"DF64">>,"0.0"}, {<<"DF128">>,"0.0"}, {<<"S">>, <<"0.0">>}],
            [{<<"D">>,"1.00"}, {<<"DF64">>,"1.0"}, {<<"DF128">>,"1.0"}, {<<"S">>, <<"1.0">>}],
            [{<<"D">>,"20.00"}, {<<"DF64">>,"20.0"}, {<<"DF128">>,"20.0"}, {<<"S">>, <<"20.0">>}],
            [{<<"D">>,"-1.00"}, {<<"DF64">>,"-1.0"}, {<<"DF128">>,"-1.0"}, {<<"S">>, <<"-1.0">>}],
            [{<<"D">>,"-20.00"}, {<<"DF64">>,"-20.0"}, {<<"DF128">>,"-20.0"}, {<<"S">>, <<"-20.0">>}]
        ], ResultDecFloat),

        ok = efirebirdsql:execute(C, <<"select * from tz_test">>),
        {ok, ResultTimeZone} = efirebirdsql:fetchall(C),
        ?assertEqual(ResultTimeZone, [
            [{<<"ID">>,1}, {<<"T">>,{{3,34,56,0},<<"GMT">>,<<"Asia/Tokyo">>}}, {<<"TS">>,{{1967,8,11},{14,45,1,0}, <<"GMT">>,<<"Asia/Tokyo">>}}],
            [{<<"ID">>,2}, {<<"T">>,{{3,34,56,0},<<"GMT">>,<<"Asia/Seoul">>}}, {<<"TS">>,{{1967,8,11},{14,45,1,0}, <<"GMT">>,<<"Asia/Seoul">>}}],
            [{<<"ID">>,3}, {<<"T">>,{{3,34,56,0},<<"GMT">>,<<"UTC">>}}, {<<"TS">>,{{1967,8,11},{14,45,1,0}, <<"GMT">>,<<"UTC">>}}]
        % ]),
        % ok = efirebirdsql:execute(C, <<"select * from tz_test where T=? and TS=?">>,
        %     [{{12,34,56, 0}, <<"Asia/Seoul">>}, {{1967,8,11},{23,45,1,0}, <<"Asia/Seoul">>}]),
        % {ok, ResultTimeZone2} = efirebirdsql:fetchall(C),
        % ?assertEqual(ResultTimeZone2, ResultTimeZone);
        ]);
    FirebirdMajorVersion < 4 ->
        ok
    end.
