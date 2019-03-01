%%% The MIT License (MIT)
%%% Copyright (c) 2016-2019 Hajime Nakagami<nakagami@gmail.com>

-module(efirebirdsql).

-export([start_link/0]).
-export([connect/5, connect/6, prepare/2, execute/1, execute/2, execute/3,
        description/1, fetchone/1, fetchall/1, commit/1, rollback/1,
        close/1, cancel/1, sync/1]).

-export_type([connection/0, connect_option/0]).

-include("efirebirdsql.hrl").

-type connection() :: pid().
-type connect_option() ::
    {port, PortNumber :: inet:port_number()} |
    {timeout, Timeout :: integer()} |
    {createdb, IsCreateDB :: boolean()} |
    {auto_commit, AutoCommit :: boolean()} |
    {pagesize, PageSize :: integer()} |
    {auth_plugin, AuthPlugin :: string()}.

-spec start_link() -> {ok, pid()}.
start_link() ->
    efirebirdsql_server:start_link().

-spec connect(connection(), string(), string(), string(), string(), [connect_option()])
        -> ok | {error, Reason :: binary}.
connect(C, Host, Username, Password, Database, Ops) ->
    case gen_server:call(C, {connect, Host, Username, Password, Database, Ops}, infinity) of
    ok -> {ok, C};
    {error, _} ->
        {ok, Msg} = gen_server:call(C, get_last_error, infinity),
        {error, Msg}
    end.

-spec connect(string(), string(), string(), string(), [connect_option()])
        -> {ok, Connection :: connection()} | {error, Reason :: binary()}.
connect(Host, Username, Password, Database, Ops) ->
    {ok, C} = start_link(),
    connect(C, Host, Username, Password, Database, Ops).

-spec prepare(connection(), binary())
    -> ok | {error, Reason :: binary()}.
prepare(C, QueryString) ->
    case gen_server:call(C, {prepare, QueryString}, infinity) of
    {error, _} ->
        {ok, Msg} = gen_server:call(C, get_last_error, infinity),
        {error, Msg};
    R -> R
    end.

-spec execute(connection(), binary(), list())
        -> ok | {error, Reason :: binary()}.
execute(C, QueryString, Params) ->
    case R = prepare(C, QueryString) of
    ok -> execute(C, Params);
    {error, _} -> R
    end.

-spec execute(connection(), binary() | list())
        -> ok | {error, Reason :: binary()}.
execute(C, QueryString) when is_binary(QueryString) ->
    execute(C, QueryString, []);
execute(C, Params) when is_list(Params) ->
    case gen_server:call(C, {execute, Params}, infinity) of
    ok -> ok;
    {error, _} ->
        {ok, Msg} = gen_server:call(C, get_last_error, infinity),
        {error, Msg}
    end.

-spec execute(connection())
        -> ok | {error, Reason :: binary()}.
execute(C) ->
    execute(C, []).

-spec description(connection()) -> list() | nil.
description(C) ->
    gen_server:call(C, description, infinity).

-spec fetchone(connection())
        -> {ok, list()} | {error, Reason :: binary()}.
fetchone(C) ->
    case gen_server:call(C, fetchone, infinity) of
    {error, _} ->
        {ok, Msg} = gen_server:call(C, get_last_error, infinity),
        {error, Msg};
    R -> R
    end.

-spec fetchall(connection())
        -> {ok, list()} | {error, Reason :: binary()}.
fetchall(C) ->
    case gen_server:call(C, fetchall, infinity) of
    {error, _} ->
        {ok, Msg} = gen_server:call(C, get_last_error, infinity),
        {error, Msg};
    R -> R
    end.

-spec commit(connection())
    -> ok | {error, Reason :: binary()}.
commit(C) ->
    gen_server:call(C, commit, infinity).

-spec rollback(connection())
    -> ok | {error, Reason :: binary()}.
rollback(C) ->
    gen_server:call(C, rollback, infinity).

-spec close(efirebirdsql:connection())
    -> ok | {error, Reason :: binary()}.
close(C) ->
    gen_server:call(C, close, infinity),
    catch gen_server:cast(C, stop),
    ok.

-spec get_last_error(connection()) -> binary().
get_last_error(C) ->
    {ok, Msg} = gen_server:call(C, get_last_error, infinity),
    Msg.

cancel(C) ->
    gen_server:cast(C, cancel).

sync(C) ->
    gen_server:call(C, sync).

