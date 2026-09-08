%%% The MIT License (MIT)
%%% Copyright (c) 2016-2022 Hajime Nakagami<nakagami@gmail.com>

-module(efirebirdsql_op_tests).

-include_lib("eunit/include/eunit.hrl").
-include("efirebirdsql.hrl").

%% A dead socket must be reported with the 3-tuple shape that every caller of
%% get_response/1 expects. get_response/2 answers {error, closed}, and forwarding
%% that 2-tuple crashed efirebirdsql_protocol with case_clause: execute/4,
%% fetchall/2, rowcount/2, free_statement/3, commit/1 and the rest all match only
%% {op_response, _, _} and {error, ErrNo, Msg}. The crash surfaced in the Elixir
%% adapter as CaseClauseError/WithClauseError, hiding a plain lost connection.
get_response_on_dead_socket_returns_error_triple_test() ->
    {Sock, Listen} = broken_connection(),
    Conn = #conn{sock=Sock},
    Response = efirebirdsql_op:get_response(Conn),
    ?assertMatch({error, 335544726, _}, Response),
    {error, _ErrNo, Msg} = Response,
    ?assert(is_binary(Msg)),
    %% the reason from the socket stays visible in the message
    ?assertNotEqual(nomatch, binary:match(Msg, <<"closed">>)),
    gen_tcp:close(Sock),
    gen_tcp:close(Listen).

%% get_response/2 keeps its own contract: ping/1 relies on the 2-tuple to tell a
%% silent peer apart, and treats anything that is not op_response as down.
get_response_with_timeout_keeps_two_tuple_test() ->
    {Sock, Listen, Peer} = silent_connection(),
    Conn = #conn{sock=Sock},
    ?assertEqual({error, timeout}, efirebirdsql_op:get_response(Conn, 50)),
    gen_tcp:close(Sock),
    gen_tcp:close(Peer),
    gen_tcp:close(Listen).

%% client socket whose peer closed the connection: any recv fails.
broken_connection() ->
    {ok, Listen} = gen_tcp:listen(0, [binary, {active, false}]),
    {ok, Port} = inet:port(Listen),
    {ok, Sock} = gen_tcp:connect("localhost", Port, [binary, {active, false}]),
    {ok, Peer} = gen_tcp:accept(Listen),
    ok = gen_tcp:close(Peer),
    {Sock, Listen}.

%% client socket whose peer is connected but never sends anything.
silent_connection() ->
    {ok, Listen} = gen_tcp:listen(0, [binary, {active, false}]),
    {ok, Port} = inet:port(Listen),
    {ok, Sock} = gen_tcp:connect("localhost", Port, [binary, {active, false}]),
    {ok, Peer} = gen_tcp:accept(Listen),
    {Sock, Listen, Peer}.

process_dpb_test() ->
    %% nil/nil (the default): nothing is added, so the attach DPB is unchanged.
    ?assertEqual([], efirebirdsql_op:process_dpb(nil, nil)),
    %% an empty process name is treated as unset.
    ?assertEqual([], efirebirdsql_op:process_dpb("", nil)),
    %% isc_dpb_process_name (74) + length + name bytes.
    ?assertEqual([74, 7, $l, $y, $n, $x, $w, $e, $b],
                 efirebirdsql_op:process_dpb("lynxweb", nil)),
    %% isc_dpb_process_id (71) + length 4 + pid as a 32-bit little-endian integer.
    ?assertEqual([71, 4, 12, 0, 0, 0],
                 efirebirdsql_op:process_dpb(nil, 12)),
    %% values > 255 span the little-endian bytes (300 = 16#012C).
    ?assertEqual([71, 4, 16#2C, 16#01, 0, 0],
                 efirebirdsql_op:process_dpb(nil, 300)),
    %% both set: the process_name items are followed by the process_id items.
    ?assertEqual([74, 7, $l, $y, $n, $x, $w, $e, $b, 71, 4, 12, 0, 0, 0],
                 efirebirdsql_op:process_dpb("lynxweb", 12)).
