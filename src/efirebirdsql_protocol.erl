%%% The MIT License (MIT)

%%% Copyright (c) 2016-2018 Hajime Nakagami<nakagami@gmail.com>

-module(efirebirdsql_protocol).

-export([connect/5, connect/6, detach/1, begin_transaction/2]).
-export([allocate_statement/1, prepare_statement/2, free_statement/1]).
-export([execute/2, execute2/2, fetchrows/1, description/2]).
-export([commit/1, rollback/1]).

-include("efirebirdsql.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Utility functions in module

connect_database(TcpMod, Sock, Username, Password, Database, PageSize, IsCreateDB, State) ->
    case IsCreateDB of
        true ->
            create_database(TcpMod, Sock, Username, Password, Database, PageSize, State);
        false ->
            attach_database(TcpMod, Sock, Username, Password, Database, State)
    end.

attach_database(TcpMod, Sock, User, Password, Database, State) ->
    TcpMod:send(Sock,
        efirebirdsql_op:op_attach(User, Password, Database, State#state.accept_version)),
    R = case efirebirdsql_op:get_response(TcpMod, Sock) of
        {op_response, {ok, Handle, _}} -> {ok, Handle};
        {op_response, {error, Msg}} ->{error, Msg}
    end,
    case R of
        {ok, DbHandle} ->
            case allocate_statement(TcpMod, Sock, DbHandle) of
                {ok, StmtHandle} ->
                    {ok, State#state{db_handle = DbHandle, stmt_handle = StmtHandle}};
                {error, Msg2} ->
                    {{error, Msg2}, State#state{db_handle = DbHandle}}
            end;
        {error, Msg3} ->
            {{error, Msg3}, State}
    end.

create_database(TcpMod, Sock, User, Password, Database, PageSize, State) ->
    TcpMod:send(Sock,
        efirebirdsql_op:op_create(User, Password, Database, PageSize, State#state.accept_version)),
    R = case efirebirdsql_op:get_response(TcpMod, Sock) of
        {op_response, {ok, Handle, _}} -> {ok, Handle};
        {op_response, {error, Msg}} ->{error, Msg}
    end,
    case R of
        {ok, DbHandle} ->
            case allocate_statement(TcpMod, Sock, DbHandle) of
                {ok, StmtHandle} ->
                    {ok, State#state{db_handle = DbHandle, stmt_handle = StmtHandle}};
                {error, Msg2} ->
                    {{error, Msg2}, State#state{db_handle = DbHandle}}
            end;
        {error, Msg3} ->
            {{error, Msg3}, State}
    end.

allocate_statement(TcpMod, Sock, DbHandle) ->
    TcpMod:send(Sock,
        efirebirdsql_op:op_allocate_statement(DbHandle)),
    case efirebirdsql_op:get_response(TcpMod, Sock) of
        {op_response, {ok, Handle, _}} -> {ok, Handle};
        {op_response, {error, Msg}} ->{error, Msg}
    end.


connect(Host, Username, Password, Database, IsCreateDB, PageSize, State) ->
    TcpMod = State#state.mod,
    Sock = State#state.sock,
    TcpMod:send(Sock,
        efirebirdsql_op:op_connect(Host, Username, Password, Database, State#state.public_key, State#state.wire_crypt)),
    case efirebirdsql_op:get_response(TcpMod, Sock) of
        {op_accept, {AcceptVersion, _AcceptType}} ->
            connect_database(TcpMod, Sock, Username, Password, Database, PageSize, IsCreateDB, State#state{accept_version=AcceptVersion});
        {op_cond_accept, {_AcceptVersion, _AcceptType}} ->
            io:format("op_cond_accept");
        {op_accept_data, {_AcceptVersion, _AcceptType}} ->
            io:format("op_accept_data");
        {op_reject, _} -> {{error, "Connection Rejected"}, State}
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
    {Pub ,Private} = efirebirdsql_srp:client_seed(),
    case (State#state.mod):connect(Host, Port, SockOptions) of
        {ok, Sock} ->
            State2 = State#state{
                sock=Sock,
                public_key=Pub,
                private_key=Private,
                wire_crypt=proplists:get_value(wire_crypt, Options, false)
            },
            {R, NewState} = connect(Host, Username, Password, Database, IsCreateDB, PageSize, State2),
            {ok, R, NewState};
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
        {op_response, {error, Msg}} ->{error, Msg}
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
