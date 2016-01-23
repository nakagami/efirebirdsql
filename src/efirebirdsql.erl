%%% The MIT License (MIT)
%%% Copyright (c) 2016 Hajime Nakagami<nakagami@gmail.com>

-module(efirebirdsql).

-export([start_link/0]).
-export([connect/4, connect/5, prepare/2, execute/2, execute/3, description/1,
        fetchone/1, fetchall/1, commit/1, rollback/1,
        close/1, cancel/1, sync/1]).

-export_type([connection/0, connect_option/0,
    connect_error/0, query_error/0]).

-include("efirebirdsql.hrl").

-type connection() :: pid().
-type connect_option() ::
    {port, PortNum :: inet:port_number()} |
    {timeout, Timeout :: integer()} |
    {createdb, IsCreateDB :: boolean()} |
    {auto_commit, AutoCommit :: boolean()} |
    {pagesize, PageSize :: integer()}.
-type connect_error() :: #error{}.
-type query_error() :: #error{}.

-spec start_link() -> {ok, pid()}.
start_link() ->
    efirebirdsql_server:start_link().

-spec connect(string(), string(), string(), string(), [connect_option()])
        -> {ok, Connection :: connection()} | {error, Reason :: connect_error()}.
connect(Host, Username, Password, Database, Ops) ->
    {ok, C} = start_link(),
    case gen_server:call(C,
                         {connect, Host, Username, Password, Database, Ops},
                         infinity) of
        ok ->
            case gen_server:call(C, {transaction, Ops}, infinity) of
                ok -> {ok, C};
                Error = {error, _}
                    -> Error
            end;
        Error = {error, _}
            -> Error
    end.

-spec connect(string(), string(), string(), string())
    -> {ok, Connection :: connection()} | {error, Reason :: connect_error()}.
connect(Host, Username, Password, Database) ->
    connect(Host, Username, Password, Database, []).

-spec prepare(connection(), binary())
    -> ok.
prepare(C, QueryString) ->
    gen_server:call(C, {prepare, QueryString}, infinity).

execute(C, QueryString, Params) ->
    prepare(C, QueryString),
    execute(C, Params).

execute(C, QueryString) when is_binary(QueryString) ->
    execute(C, QueryString, []);
execute(C, Params) when is_list(Params) ->
    gen_server:call(C, {execute, Params}, infinity).

description(C) ->
    gen_server:call(C, description, infinity).

fetchone(C) ->
    gen_server:call(C, fetchone, infinity).

fetchall(C) ->
    gen_server:call(C, fetchall, infinity).

-spec commit(connection())
    -> ok | {error, _Reason}.
commit(C) ->
    gen_server:call(C, commit, infinity).

-spec rollback(connection())
    -> ok | {error, _Reason}.
rollback(C) ->
    gen_server:call(C, rollback, infinity).

-spec close(efirebirdsql:connection())
    -> ok | {error, Reason :: connect_error()}.
close(C) ->
    gen_server:call(C, detach, infinity),
    catch gen_server:cast(C, stop),
    ok.

cancel(C) ->
    gen_server:cast(C, cancel).

sync(C) ->
    gen_server:call(C, sync).

