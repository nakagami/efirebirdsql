%%% The MIT License (MIT)
%%% Copyright (c) 2016-2021 Hajime Nakagami<nakagami@gmail.com>

-module(efirebirdsql_socket).

-export([send/2, recv/2, recv/3, recv_align/2, recv_null_bitmap/2]).

-include("efirebirdsql.hrl").

send(Conn, Data) when Conn#conn.write_state =:= undefined ->
    gen_tcp:send(Conn#conn.sock, Data);
send(Conn, Message) ->
    Encrypted = crypto:crypto_update(Conn#conn.write_state, Message),
    gen_tcp:send(Conn#conn.sock, Encrypted).

recv(Conn, Len) ->
    recv(Conn, Len, infinity).

%% recv/3 adds an explicit gen_tcp:recv timeout. recv/2 keeps the historical
%% behavior (infinity), so normal reads are unchanged; only ping/1 passes a
%% finite timeout so a server that stops responding fails the health-check fast
%% instead of blocking the connection (and, in a pool, every idle worker) forever.
%%
%% A zero length read must still yield a binary: callers pattern match on
%% binaries (and pass the result to binary_to_list/1), so returning the empty
%% list made every zero length field crash with badarg.
recv(_Conn, Len, _Timeout) when Len =:= 0 ->
    {ok, <<>>};
recv(Conn, Len, Timeout) when Conn#conn.read_state =:= undefined ->
    gen_tcp:recv(Conn#conn.sock, Len, Timeout);
recv(Conn, Len, Timeout) ->
    case gen_tcp:recv(Conn#conn.sock, Len, Timeout) of
        {ok, Encrypted} ->
            {ok, crypto:crypto_update(Conn#conn.read_state, Encrypted)};
        {error, _Reason} = Error ->
            Error
    end.

recv_align(Conn, Len) ->
    {T, V} = recv(Conn, Len),
    if
        Len rem 4 =/= 0 -> recv(Conn, 4 - (Len rem 4));
        true -> nil
    end,
    {T, V}.

recv_null_bitmap(_Conn, BitLen) when BitLen =:= 0 ->
    <<>>;
recv_null_bitmap(Conn, BitLen) ->
    Div8 = BitLen div 8,
    Len = if
        BitLen rem 8 =:= 0 -> Div8;
        BitLen rem 8 =/= 0 -> Div8 + 1
        end,
    {ok, Buf} = recv_align(Conn, Len),
    <<Bitmap:Len/little-unit:8>> = Buf,
    Bitmap.
