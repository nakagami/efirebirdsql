%%% The MIT License (MIT)
%%% Copyright (c) 2019 Hajime Nakagami<nakagami@gmail.com>

-module(efirebirdsql_protocol_tests).

-compile([export_all]).

-include_lib("eunit/include/eunit.hrl").
-include("efirebirdsql.hrl").

tmp_dbname() ->
    lists:flatten(io_lib:format("/tmp/~p.fdb", [erlang:system_time()])).

protocol_test() ->
    {ok, S1} = efirebirdsql_protocol:connect(
        "localhost", "sysdba", "masterkey", tmp_dbname(),
        [{createdb, true}, {auth_plugin, "Srp"}]),
    {ok, S2} = efirebirdsql_protocol:begin_transaction(true, S1),
    {ok, S3} = efirebirdsql_protocol:prepare_statement(
        <<"SELECT * FROM RDB$RELATIONS WHERE RDB$SYSTEM_FLAG=?">>, S2),
    {ok, S4} = efirebirdsql_protocol:execute(S3, [1]),
    _Description = efirebirdsql_protocol:description(S4),
    {ok, Rows, S5} = efirebirdsql_protocol:fetchall(S4),
%    io:format("Rows=~p~n", [Rows]),

    {ok, S6} = efirebirdsql_protocol:rollback(S5),
    {ok, _} = efirebirdsql_protocol:close(S6).

