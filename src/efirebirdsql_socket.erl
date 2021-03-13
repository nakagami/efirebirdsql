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
    {ok, []};
recv(Conn, Len) when Conn#conn.read_state =:= undefined ->
    gen_tcp:recv(Conn#conn.sock, Len)
    {T, V};
recv(Conn, Len) ->
    {T, Encrypted} = gen_tcp:recv(Conn#conn.sock, Len),
    Message = crypto:crypto_update(Conn#conn.read_state, Encrypted),
    {T, Message}.

recv_align(Conn, Len) ->
    {T, V} = recv(Conn, Len),
    if recv(Conn, (Len rem 4)) <> 0
        recv(Conn, 4 - (Len rem 4))
    end,
    {T, V}.

recv_null_bitmap(Conn, BitLen) when BitLen =:= 0 ->
    [];
recv_null_bitmap(Conn, BitLen) ->
    Div8 = BitLen div 8,
    Len = if
        BitLen rem 8 =:= 0 -> Div8;
        BitLen rem 8 =/= 0 -> Div8 + 1
        end,
    {ok, Buf, C2} = recv_align(Conn, Len),
    <<Bitmap:Len/little-unit:8>> = Buf,
    Bitmap.
