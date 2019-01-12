%%% The MIT License (MIT)
%%% Copyright (c) 2019 Hajime Nakagami<nakagami@gmail.com>

-module(efirebirdsql_socket).

-export([send/2, recv/2, recv_align/2]).

-include("efirebirdsql.hrl").

send(State, Data) ->
    gen_tcp:send(State#state.sock, Data).

recv(State, Len) ->
    gen_tcp:recv(State#state.sock, Len).

recv_align(State, Len) ->
    {T, V} = recv(State, Len),
    case Len rem 4 of
        0 -> ok;
        1 -> recv(State, 3);
        2 -> recv(State, 2);
        3 -> recv(State, 1)
    end,
    {T, V}.
