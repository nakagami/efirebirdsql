%%% The MIT License (MIT)
%%% Copyright (c) 2019 Hajime Nakagami<nakagami@gmail.com>

-module(efirebirdsql_socket).

-export([send/2, recv/2]).

-include("efirebirdsql.hrl").

send(State, Data) ->
    gen_tcp:send(State#state.sock, Data).

recv(State, Len) ->
    gen_tcp:recv(State#state.sock, Len).

