%%% The MIT License (MIT)
%%% Copyright (c) 2019 Hajime Nakagami<nakagami@gmail.com>

-module(efirebirdsql_socket).

-export([send/2, recv/2, recv_align/2, recv_null_bitmap/2]).

-include("efirebirdsql.hrl").

send(State, Data) ->
    gen_tcp:send(State#state.sock, Data),
    State.

recv(State, Len) when Len =:= 0 ->
    {ok, [], State};
recv(State, Len) ->
    {T, V} = gen_tcp:recv(State#state.sock, Len),
    {T, V, State}.

recv_align(State, Len) ->
    {T, V, S2} = recv(State, Len),
    S4 = case Len rem 4 of
        0 -> S2;
        1 -> {_, _, S3} = recv(S2, 3), S3;
        2 -> {_, _, S3} = recv(S2, 2), S3;
        3 -> {_, _, S3} = recv(S2, 1), S3
    end,
    {T, V, S4}.

recv_null_bitmap(State, BitLen) when BitLen =:= 0 ->
    {[], State};
recv_null_bitmap(State, BitLen) ->
    Div8 = BitLen div 8,
    Len = if
        BitLen rem 8 =:= 0 -> Div8;
        BitLen rem 8 =/= 0 -> Div8 + 1
    end,
    {ok, Buf, S2} = recv_align(State, Len),
    <<Bitmap:Len/little-unit:8>> = Buf,
    {Bitmap, S2}.
