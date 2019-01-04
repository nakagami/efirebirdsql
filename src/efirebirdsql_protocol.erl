%%% The MIT License (MIT)

%%% Copyright (c) 2016-2018 Hajime Nakagami<nakagami@gmail.com>

-module(efirebirdsql_protocol).

-export([connect/5, connect/6, detach/1, begin_transaction/2]).
-export([allocate_statement/1, prepare_statement/2, free_statement/1]).
-export([execute/2, execute2/2, fetchrows/1, description/1]).
-export([commit/1, rollback/1]).

-include("efirebirdsql.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Utility functions in module

connect_database(Host, Database, IsCreateDB, PageSize, State) ->
    TcpMod = State#state.mod,
    Sock = State#state.sock,
    TcpMod:send(Sock,
        efirebirdsql_op:op_connect(Host, Database, State)),
    case efirebirdsql_op:get_connect_response(TcpMod, Sock, State) of
        {ok, NewState} ->
            case IsCreateDB of
                true ->
                    TcpMod:send(Sock,
                        efirebirdsql_op:op_create(Database, PageSize, NewState));
                false ->
                    TcpMod:send(Sock,
                        efirebirdsql_op:op_attach(Database, NewState))
            end,
            case efirebirdsql_op:get_response(TcpMod, Sock) of
                {op_response, {ok, Handle, _}} ->
                    allocate_statement(NewState#state{db_handle=Handle});
                {op_response, {error, Msg}} ->
                    {error, Msg, NewState}
            end;
        {error, Reason, NewState} ->
            {error, Reason, NewState}
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% public functions

connect(Host, Username, Password, Database, Options) ->
    connect(Host, Username, Password, Database, Options, #state{}).

connect(Host, Username, Password, Database, Options, State) ->
    SockOptions = [{active, false}, {packet, raw}, binary],
    Port = proplists:get_value(port, Options, 3050),
    IsCreateDB = proplists:get_value(createdb, Options, false),
    PageSize = proplists:get_value(pagesize, Options, 4096),
    Private = efirebirdsql_srp:get_private_key(),
    Public = efirebirdsql_srp:client_public(Private),
    case (State#state.mod):connect(Host, Port, SockOptions) of
        {ok, Sock} ->
            State2 = State#state{
                sock=Sock,
                user=Username,
                password=Password,
                client_private=Private,
                client_public=Public,
                auth_plugin=proplists:get_value(auth_plugin, Options, ''),
                wire_crypt=proplists:get_value(wire_crypt, Options, false)
            },
            connect_database(Host, Database, IsCreateDB, PageSize, State2);
        {error, Reason} ->
            {error, Reason, State}
    end.

detach(State) ->
    TcpMod = State#state.mod,
    Sock = State#state.sock,
    DbHandle = State#state.db_handle,
    TcpMod:send(Sock, efirebirdsql_op:op_detach(DbHandle)),
    case efirebirdsql_op:get_response(TcpMod, Sock) of
        {op_response,  {ok, _, _}} -> ok;
        {op_response, {error, Msg}} -> {error, Msg}
    end.

%% Transaction
begin_transaction(Tpb, State) ->
    TcpMod = State#state.mod,
    Sock = State#state.sock,
    DbHandle = State#state.db_handle,
    TcpMod:send(Sock,
        efirebirdsql_op:op_transaction(DbHandle, Tpb)),
    case efirebirdsql_op:get_response(TcpMod, Sock) of
        {op_response,  {ok, Handle, _}} -> {ok, Handle};
        {op_response, {error, Msg}} -> {error, Msg}
    end.

%% allocate, prepare and free statement
allocate_statement(State) ->
    TcpMod = State#state.mod,
    Sock = State#state.sock,
    DbHandle = State#state.db_handle,
    TcpMod:send(Sock,
        efirebirdsql_op:op_allocate_statement(DbHandle)),
    case efirebirdsql_op:get_response(TcpMod, Sock) of
        {op_response, {ok, Handle, _}} -> {ok, State#state{stmt_handle=Handle}};
        {op_response, {error, Msg}} ->{error, Msg, State}
    end.

prepare_statement(Sql, State) ->
    TcpMod = State#state.mod,
    Sock = State#state.sock,
    TransHandle = State#state.trans_handle,
    StmtHandle = State#state.stmt_handle,
    TcpMod:send(Sock,
        efirebirdsql_op:op_prepare_statement(TransHandle, StmtHandle, Sql)),
    efirebirdsql_op:get_prepare_statement_response(TcpMod, Sock, StmtHandle).

free_statement(State) ->
    TcpMod = State#state.mod,
    Sock = State#state.sock,
    StmtHandle = State#state.stmt_handle,
    TcpMod:send(Sock, efirebirdsql_op:op_free_statement(StmtHandle)),
    case efirebirdsql_op:get_response(TcpMod, Sock) of
        {op_response,  {ok, _, _}} -> ok;
        {op_response, {error, Msg}} -> {error, Msg}
    end.

%% Execute, Fetch and Description
execute(Params, State) ->
    TcpMod = State#state.mod,
    Sock = State#state.sock,
    TransHandle = State#state.trans_handle,
    StmtHandle = State#state.stmt_handle,
    TcpMod:send(Sock,
        efirebirdsql_op:op_execute(TransHandle, StmtHandle, Params)),
    case efirebirdsql_op:get_response(TcpMod, Sock) of
        {op_response,  {ok, _, _}} -> ok;
        {op_response, {error, Msg}} -> {error, Msg}
    end.

execute2(Param, State) ->
    TcpMod = State#state.mod,
    Sock = State#state.sock,
    TransHandle = State#state.trans_handle,
    StmtHandle = State#state.stmt_handle,
    XSqlVars = State#state.xsqlvars,
    TcpMod:send(Sock,
        efirebirdsql_op:op_execute2(TransHandle, StmtHandle, Param, XSqlVars)),
    Row = efirebirdsql_op:get_sql_response(TcpMod, Sock, XSqlVars),
    case efirebirdsql_op:get_response(TcpMod, Sock) of
        {op_response,  {ok, _, _}} -> {ok, Row};
        {op_response, {error, Msg}} -> {error, Msg}
    end.

fetchrows(Results, State) ->
    TcpMod = State#state.mod,
    Sock = State#state.sock,
    StmtHandle = State#state.stmt_handle,
    XSqlVars = State#state.xsqlvars,
    TcpMod:send(Sock,
        efirebirdsql_op:op_fetch(StmtHandle, XSqlVars)),
    {op_fetch_response, {NewResults, MoreData}} = efirebirdsql_op:get_fetch_response(TcpMod, Sock, XSqlVars),
    case MoreData of
        true -> fetchrows(lists:flatten([Results, NewResults]), State);
        false -> {ok, Results ++ NewResults}
    end.
fetchrows(State) ->
    fetchrows([], State).

description([], XSqlVar) ->
    lists:reverse(XSqlVar);
description(InXSqlVars, XSqlVar) ->
    [H | T] = InXSqlVars,
    description(T, [{H#column.name, H#column.type, H#column.scale,
                      H#column.length, H#column.null_ind} | XSqlVar]).
description(InXSqlVar) ->
    description(InXSqlVar, []).

%% Commit and rollback
commit(State) ->
    TcpMod = State#state.mod,
    Sock = State#state.sock,
    TransHandle = State#state.trans_handle,
    TcpMod:send(Sock, efirebirdsql_op:op_commit_retaining(TransHandle)),
    case efirebirdsql_op:get_response(TcpMod, Sock) of
        {op_response,  {ok, _, _}} -> ok;
        {op_response, {error, Msg}} -> {error, Msg}
    end.

rollback(State) ->
    TcpMod = State#state.mod,
    Sock = State#state.sock,
    TransHandle = State#state.trans_handle,
    TcpMod:send(Sock, efirebirdsql_op:op_rollback_retaining(TransHandle)),
    case efirebirdsql_op:get_response(TcpMod, Sock) of
        {op_response,  {ok, _, _}} -> ok;
        {op_response, {error, Msg}} -> {error, Msg}
    end.
