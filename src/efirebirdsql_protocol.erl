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
    efirebirdsql_socket:send(State,
        efirebirdsql_op:op_connect(Host, Database, State)),
    case efirebirdsql_op:get_connect_response(State) of
        {ok, NewState} ->
            case IsCreateDB of
                true ->
                    efirebirdsql_socket:send(State,
                        efirebirdsql_op:op_create(Database, PageSize, NewState));
                false ->
                    efirebirdsql_socket:send(State,
                        efirebirdsql_op:op_attach(Database, NewState))
            end,
            case efirebirdsql_op:get_response(State) of
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
    case gen_tcp:connect(Host, Port, SockOptions) of
        {ok, Sock} ->
            State2 = State#state{
                sock=Sock,
                user=string:to_upper(Username),
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
    efirebirdsql_socket:send(State, efirebirdsql_op:op_detach(State#state.db_handle)),
    case efirebirdsql_op:get_response(State) of
        {op_response,  {ok, _, _}} -> ok;
        {op_response, {error, Msg}} -> {error, Msg}
    end.

%% Transaction
begin_transaction(Tpb, State) ->
    efirebirdsql_socket:send(State,
        efirebirdsql_op:op_transaction(State#state.db_handle, Tpb)),
    case efirebirdsql_op:get_response(State) of
        {op_response,  {ok, Handle, _}} -> {ok, Handle};
        {op_response, {error, Msg}} -> {error, Msg}
    end.

%% allocate, prepare and free statement
allocate_statement(State) ->
    efirebirdsql_socket:send(State,
        efirebirdsql_op:op_allocate_statement(State#state.db_handle)),
    case efirebirdsql_op:get_response(State) of
        {op_response, {ok, Handle, _}} -> {ok, State#state{stmt_handle=Handle}};
        {op_response, {error, Msg}} ->{error, Msg, State}
    end.

prepare_statement(Sql, State) ->
    TransHandle = State#state.trans_handle,
    StmtHandle = State#state.stmt_handle,
    efirebirdsql_socket:send(State,
        efirebirdsql_op:op_prepare_statement(TransHandle, StmtHandle, Sql)),
    efirebirdsql_op:get_prepare_statement_response(State).

free_statement(State) ->
    efirebridsql_socket:send(State, efirebirdsql_op:op_free_statement(State#state.stmt_handle)),
    case efirebirdsql_op:get_response(State) of
        {op_response,  {ok, _, _}} -> ok;
        {op_response, {error, Msg}} -> {error, Msg}
    end.

%% Execute, Fetch and Description
execute(Params, State) ->
    TransHandle = State#state.trans_handle,
    StmtHandle = State#state.stmt_handle,
    efirebirdsql_socket:send(State,
        efirebirdsql_op:op_execute(TransHandle, StmtHandle, Params)),
    case efirebirdsql_op:get_response(State) of
        {op_response,  {ok, _, _}} -> ok;
        {op_response, {error, Msg}} -> {error, Msg}
    end.

execute2(Param, State) ->
    TransHandle = State#state.trans_handle,
    StmtHandle = State#state.stmt_handle,
    XSqlVars = State#state.xsqlvars,
    efirebirdsql_socket:send(State,
        efirebirdsql_op:op_execute2(TransHandle, StmtHandle, Param, XSqlVars)),
    Row = efirebirdsql_op:get_sql_response(State),
    case efirebirdsql_op:get_response(State) of
        {op_response,  {ok, _, _}} -> {ok, Row};
        {op_response, {error, Msg}} -> {error, Msg}
    end.

fetchrows(Results, State) ->
    StmtHandle = State#state.stmt_handle,
    XSqlVars = State#state.xsqlvars,
    efirebirdsql_socket:send(State,
        efirebirdsql_op:op_fetch(StmtHandle, XSqlVars)),
    {op_fetch_response, {NewResults, MoreData}} = efirebirdsql_op:get_fetch_response(State),
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
    efirebirdsql_socket:send(State,
        efirebirdsql_op:op_commit_retaining(State#state.trans_handle)),
    case efirebirdsql_op:get_response(State) of
        {op_response,  {ok, _, _}} -> ok;
        {op_response, {error, Msg}} -> {error, Msg}
    end.

rollback(State) ->
    efirebirdsql_socket:send(State,
        efirebirdsql_op:op_rollback_retaining(State#state.trans_handle)),
    case efirebirdsql_op:get_response(State) of
        {op_response,  {ok, _, _}} -> ok;
        {op_response, {error, Msg}} -> {error, Msg}
    end.
