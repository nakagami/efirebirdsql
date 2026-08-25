%%% The MIT License (MIT)
%%% Copyright (c) 2016-2021 Hajime Nakagami<nakagami@gmail.com>

-module(efirebirdsql_socket_tests).

-include_lib("eunit/include/eunit.hrl").
-include("efirebirdsql.hrl").

%% A socket error during recv (eg. {error, closed} after the server went
%% away, or {error, ealready} when a previous recv timed out and is still
%% pending) must be returned as is. Before the fix the error reason atom
%% was passed to crypto:crypto_update/2 as if it were wire data, crashing
%% with badarg when wire encryption was enabled.
recv_error_returns_error_tuple_test() ->
    {Sock, Listen} = broken_connection(),
    Conn = #conn{
        sock=Sock,
        read_state=crypto:crypto_init(rc4, <<"0123456789abcdef">>, false)},
    ?assertEqual({error, closed}, efirebirdsql_socket:recv(Conn, 4)),
    gen_tcp:close(Sock),
    gen_tcp:close(Listen).

%% A zero length read must yield a binary, not the empty list. Firebird sends
%% zero length strings inside the status vector of an error response, and
%% parse_status_vector_string/1 feeds the result to binary_to_list/1: with the
%% empty list it crashed with badarg while parsing the error message, killing
%% the connection and hiding the error that Firebird actually reported.
recv_zero_length_returns_binary_test() ->
    Conn = #conn{sock=undefined},
    ?assertEqual({ok, <<>>}, efirebirdsql_socket:recv(Conn, 0)),
    {ok, Bin} = efirebirdsql_socket:recv(Conn, 0),
    ?assert(is_binary(Bin)),
    ?assertEqual([], binary_to_list(Bin)).

%% recv_align/2 pads to a 4 byte boundary; with a zero length it must not
%% touch the socket and must return a binary as well.
recv_align_zero_length_returns_binary_test() ->
    Conn = #conn{sock=undefined},
    ?assertEqual({ok, <<>>}, efirebirdsql_socket:recv_align(Conn, 0)).

%% recv/3 passes an explicit timeout to gen_tcp:recv. Against a peer that is
%% connected but never replies (a hung/unresponsive server), a finite timeout
%% must return {error, timeout} instead of blocking forever. This is what lets
%% ping/1 report a stuck connection as down instead of holding a pool worker
%% (and eventually the whole pool) hostage.
recv_with_timeout_on_silent_peer_returns_timeout_test() ->
    {Sock, Listen, Peer} = silent_connection(),
    Conn = #conn{sock=Sock},
    ?assertEqual({error, timeout}, efirebirdsql_socket:recv(Conn, 4, 50)),
    gen_tcp:close(Sock),
    gen_tcp:close(Peer),
    gen_tcp:close(Listen).

%% recv/3 with a zero length read must short circuit and yield a binary
%% without touching the socket, exactly like recv/2, regardless of the timeout.
recv3_zero_length_returns_binary_test() ->
    Conn = #conn{sock=undefined},
    ?assertEqual({ok, <<>>}, efirebirdsql_socket:recv(Conn, 0, 50)).

%% close/1 must close the socket even if the op_detach exchange fails
%% (broken socket after a request timeout). Otherwise the server keeps the
%% attachment alive and its open transactions are left orphaned until the
%% dead TCP connection is detected.
close_closes_socket_on_broken_connection_test() ->
    {Sock, Listen} = broken_connection(),
    Conn = #conn{sock=Sock, db_handle=0},
    {ok, Conn2} = efirebirdsql_protocol:close(Conn),
    ?assertEqual(undefined, Conn2#conn.sock),
    ?assertEqual(undefined, erlang:port_info(Sock)),
    gen_tcp:close(Listen).

%% client socket whose peer is already gone
broken_connection() ->
    {ok, Listen} = gen_tcp:listen(0, [binary, {active, false}]),
    {ok, Port} = inet:port(Listen),
    {ok, Sock} = gen_tcp:connect("localhost", Port, [binary, {active, false}]),
    {ok, Peer} = gen_tcp:accept(Listen),
    ok = gen_tcp:close(Peer),
    {Sock, Listen}.

%% client socket whose peer is connected but never sends anything, so a recv
%% blocks until the timeout fires. Peer is returned so the caller can close it.
silent_connection() ->
    {ok, Listen} = gen_tcp:listen(0, [binary, {active, false}]),
    {ok, Port} = inet:port(Listen),
    {ok, Sock} = gen_tcp:connect("localhost", Port, [binary, {active, false}]),
    {ok, Peer} = gen_tcp:accept(Listen),
    {Sock, Listen, Peer}.
