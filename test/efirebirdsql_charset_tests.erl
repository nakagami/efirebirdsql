%%% The MIT License (MIT)
%%% Copyright (c) 2022 Hajime Nakagami<nakagami@gmail.com>

-module(efirebirdsql_charset_tests).

-include_lib("eunit/include/eunit.hrl").
-include("efirebirdsql.hrl").

default_charset_test() ->
    DbName = lists:flatten(io_lib:format("/tmp/~p.fdb", [erlang:system_time()])),
    {ok, C} = efirebirdsql:connect(
        "localhost", os:getenv("ISC_USER", "sysdba"), os:getenv("ISC_PASSWORD", "masterkey"), DbName,
        [{createdb, true}]),
    ok = efirebirdsql:execute(C, <<"select RDB$CHARACTER_SET_NAME from RDB$DATABASE">>),
    ?assertEqual({ok, [{<<"RDB$CHARACTER_SET_NAME">>, <<"UTF8">>}]}, efirebirdsql:fetchone(C)),

    ok = efirebirdsql:close(C),
    ok.

charset_test() ->
    DbName = lists:flatten(io_lib:format("/tmp/~p.fdb", [erlang:system_time()])),
    {ok, C} = efirebirdsql:connect(
        "localhost", os:getenv("ISC_USER", "sysdba"), os:getenv("ISC_PASSWORD", "masterkey"), DbName,
        [{createdb, true}, {charset, cp932}]),
    ok = efirebirdsql:execute(C, <<"select RDB$CHARACTER_SET_NAME from RDB$DATABASE">>),
    ?assertEqual({ok, [{<<"RDB$CHARACTER_SET_NAME">>, <<"SJIS_0208">>}]}, efirebirdsql:fetchone(C)),

    ok = efirebirdsql:close(C),
    ok.
