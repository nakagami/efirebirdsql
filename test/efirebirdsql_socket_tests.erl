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
