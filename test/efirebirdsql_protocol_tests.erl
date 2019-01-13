%%% The MIT License (MIT)
%%% Copyright (c) 2019 Hajime Nakagami<nakagami@gmail.com>

-module(efirebirdsql_protocol_tests).

-compile([export_all]).

-include_lib("eunit/include/eunit.hrl").
-include("efirebirdsql.hrl").

protocol_test() ->
    {ok, S1} = efirebirdsql_protocol:connect(
        "localhost", "sysdba", "masterkey", "/tmp/erlang_protocol_test.fdb",
        [{createdb, true}, {auth_plugin, "Srp"}]),
    {ok, S2} = efirebirdsql_protocol:begin_transaction(true, S1),
    {ok, S3} = efirebirdsql_protocol:prepare_statement(
        <<"SELECT * FROM RDB$RELATIONS">>, S2),
    {ok, S4} = efirebirdsql_protocol:execute(S3, []),
    _Description = efirebirdsql_protocol:description(S4),
    {ok, Row, S5} = efirebirdsql_protocol:fetchone(S4),
%    io:format("Rows=~p~n", Row),

    {ok, S6} = efirebirdsql_protocol:rollback(S5).
%% TODO: detach
%    {ok, _} = efirebirdsql_protocol:detach(S6).


