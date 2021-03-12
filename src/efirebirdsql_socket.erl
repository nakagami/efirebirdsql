%%% The MIT License (MIT)
%%% Copyright (c) 2016-2021 Hajime Nakagami<nakagami@gmail.com>

-module(efirebirdsql_socket).

-export([send/2, recv/2, recv_align/2, recv_null_bitmap/2]).

-include("efirebirdsql.hrl").

send(Conn, Data) when Conn#conn.write_state =:= undefined ->
    gen_tcp:send(Conn#conn.sock, Data),
    Conn;
send(Conn, Message) ->
    Encrypted = crypto:crypto_update(Conn#conn.write_state, Message),
    gen_tcp:send(Conn#conn.sock, Encrypted),
    Conn.

recv(Conn, Len) when Len =:= 0 ->
    {ok, [], Conn};
recv(Conn, Len) when Conn#conn.read_state =:= undefined ->
    {T, V} = gen_tcp:recv(Conn#conn.sock, Len),
    {T, V, Conn};
recv(Conn, Len) ->
    {T, Encrypted} = gen_tcp:recv(Conn#conn.sock, Len),
    Message = crypto:crypto_update(Conn#conn.read_state, Encrypted),
    {T, Message, Conn}.

recv_align(Conn, Len) ->
    {T, V, C2} = recv(Conn, Len),
    C4 = case Len rem 4 of
        0 -> C2;
        1 -> {_, _, C3} = recv(C2, 3), C3;
        2 -> {_, _, C3} = recv(C2, 2), C3;
        3 -> {_, _, C3} = recv(C2, 1), C3
        end,
    {T, V, C4}.

recv_null_bitmap(Conn, BitLen) when BitLen =:= 0 ->
    {[], Conn};
recv_null_bitmap(Conn, BitLen) ->
    Div8 = BitLen div 8,
    Len = if
        BitLen rem 8 =:= 0 -> Div8;
        BitLen rem 8 =/= 0 -> Div8 + 1
        end,
    {ok, Buf, C2} = recv_align(Conn, Len),
    <<Bitmap:Len/little-unit:8>> = Buf,
    {Bitmap, C2}.
