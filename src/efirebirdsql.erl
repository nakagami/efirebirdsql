%%% The MIT License (MIT)
%%% Copyright (c) 2016 Hajime Nakagami<nakagami@gmail.com>

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
    {pagesize, PageSize :: integer()}.

-spec start_link() -> {ok, pid()}.
start_link() ->
    efirebirdsql_server:start_link().

-spec connect(connection(), string(), string(), string(), string(), [connect_option()])
        -> ok | {error, Reason :: binary}.
connect(C, Host, Username, Password, Database, Ops) ->
    case gen_server:call(
        C, {connect, Host, Username, Password, Database, Ops}, infinity) of
        ok -> gen_server:call(C, {transaction, Ops}, infinity);
        Error = {error, _} -> Error
    end.

-spec connect(string(), string(), string(), string(), [connect_option()])
        -> {ok, Connection :: connection()} | {error, Reason :: binary()}.
connect(Host, Username, Password, Database, Ops) ->
    {ok, C} = start_link(),
    case connect(C, Host, Username, Password, Database, Ops) of
        ok -> {ok, C};
        Error = {error, _} -> Error
    end.

-spec prepare(connection(), binary())
    -> ok | {error, Reason :: binary()}.
prepare(C, QueryString) ->
    gen_server:call(C, {prepare, QueryString}, infinity).

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
    gen_server:call(C, {execute, Params}, infinity).

-spec execute(connection())
        -> ok | {error, Reason :: binary()}.
execute(C) ->
    execute(C, []).

-spec description(connection())
        -> list().
description(C) ->
    gen_server:call(C, description, infinity).

-spec fetchone(connection())
        -> {ok, list()} | {error, Reason :: binary()}.
fetchone(C) ->
    gen_server:call(C, fetchone, infinity).

-spec fetchall(connection())
        -> {ok, list()} | {error, Reason :: binary()}.
fetchall(C) ->
    gen_server:call(C, fetchall, infinity).

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
    gen_server:call(C, detach, infinity),
    catch gen_server:cast(C, stop),
    ok.

cancel(C) ->
    gen_server:cast(C, cancel).

sync(C) ->
    gen_server:call(C, sync).

