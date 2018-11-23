%%% The MIT License (MIT)

%%% Copyright (c) 2016-2018 Hajime Nakagami<nakagami@gmail.com>

-module(efirebirdsql_protocol).

-export([connect/6, detach/3, begin_transaction/4]).
-export([prepare_statement/5, free_statement/3]).
-export([execute/5, execute2/6, fetchrows/4, description/2]).
-export([commit/3, rollback/3]).

-include("efirebirdsql.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Utility functions in module

connect_database(TcpMod, Sock, Username, Password, Database, PageSize, AcceptVersion, IsCreateDB, State) ->
    case IsCreateDB of
        true ->
            create_database(TcpMod, Sock, Username, Password, Database, PageSize, AcceptVersion, State);
        false ->
            attach_database(TcpMod, Sock, Username, Password, Database, AcceptVersion, State)
    end.

attach_database(TcpMod, Sock, User, Password, Database, AcceptVersion, State) ->
    TcpMod:send(Sock,
        efirebirdsql_op:op_attach(User, Password, Database, AcceptVersion)),
    R = case efirebirdsql_op:get_response(TcpMod, Sock) of
        {op_response, {ok, Handle, _}} -> {ok, Handle};
        {op_response, {error, Msg}} ->{error, Msg}
    end,
    case R of
        {ok, DbHandle} ->
            case allocate_statement(TcpMod, Sock, DbHandle) of
                {ok, StmtHandle} ->
                    {ok, State#state{db_handle = DbHandle, stmt_handle = StmtHandle, accept_version = AcceptVersion}};
                {error, Msg2} ->
                    {{error, Msg2}, State#state{db_handle = DbHandle, accept_version = AcceptVersion}}
            end;
        {error, Msg3} ->
            {{error, Msg3}, State}
    end.

create_database(TcpMod, Sock, User, Password, Database, PageSize, AcceptVersion, State) ->
    TcpMod:send(Sock,
        efirebirdsql_op:op_create(User, Password, Database, PageSize, AcceptVersion)),
    R = case efirebirdsql_op:get_response(TcpMod, Sock) of
        {op_response, {ok, Handle, _}} -> {ok, Handle};
        {op_response, {error, Msg}} ->{error, Msg}
    end,
    case R of
        {ok, DbHandle} ->
            case allocate_statement(TcpMod, Sock, DbHandle) of
                {ok, StmtHandle} ->
                    {ok, State#state{db_handle = DbHandle, stmt_handle = StmtHandle, accept_version = AcceptVersion}};
                {error, Msg2} ->
                    {{error, Msg2}, State#state{db_handle = DbHandle, accept_version = AcceptVersion}}
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
            connect_database(TcpMod, Sock, Username, Password, Database, PageSize, AcceptVersion, IsCreateDB, State);
        {op_cond_accept, {_AcceptVersion, _AcceptType}} ->
            io:format("op_cond_accept");
        {op_accept_data, {_AcceptVersion, _AcceptType}} ->
            io:format("op_accept_data");
        {op_reject, _} -> {{error, "Connection Rejected"}, State}
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% public functions

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
            {reply, R, NewState};
        Error = {error, _} ->
            {reply, Error, State}
    end.

detach(TcpMod, Sock, DbHandle) ->
    TcpMod:send(Sock, efirebirdsql_op:op_detach(DbHandle)),
    case efirebirdsql_op:get_response(TcpMod, Sock) of
        {op_response,  {ok, _, _}} -> ok;
        {op_response, {error, Msg}} -> {error, Msg}
    end.

%% Transaction
begin_transaction(TcpMod, Sock, DbHandle, Tpb) ->
    TcpMod:send(Sock,
        efirebirdsql_op:op_transaction(DbHandle, Tpb)),
    case efirebirdsql_op:get_response(TcpMod, Sock) of
        {op_response,  {ok, Handle, _}} -> {ok, Handle};
        {op_response, {error, Msg}} -> {error, Msg}
    end.

%% prepare and free statement
prepare_statement(TcpMod, Sock, TransHandle, StmtHandle, Sql) ->
    TcpMod:send(Sock,
        efirebirdsql_op:op_prepare_statement(TransHandle, StmtHandle, Sql)),
    efirebirdsql_op:get_prepare_statement_response(TcpMod, Sock, StmtHandle).

free_statement(TcpMod, Sock, StmtHandle) ->
    TcpMod:send(Sock, efirebirdsql_op:op_free_statement(StmtHandle)),
    case efirebirdsql_op:get_response(TcpMod, Sock) of
        {op_response,  {ok, _, _}} -> ok;
        {op_response, {error, Msg}} -> {error, Msg}
    end.

%% Execute, Fetch and Description
execute(TcpMod, Sock, TransHandle, StmtHandle, Params) ->
    TcpMod:send(Sock,
        efirebirdsql_op:op_execute(TransHandle, StmtHandle, Params)),
    case efirebirdsql_op:get_response(TcpMod, Sock) of
        {op_response,  {ok, _, _}} -> ok;
        {op_response, {error, Msg}} -> {error, Msg}
    end.

execute2(TcpMod, Sock, TransHandle, StmtHandle, Param, XSqlVars) ->
        TcpMod:send(Sock,
            efirebirdsql_op:op_execute2(TransHandle, StmtHandle, Param, XSqlVars)),
        Row = efirebirdsql_op:get_sql_response(TcpMod, Sock, XSqlVars),
        case efirebirdsql_op:get_response(TcpMod, Sock) of
            {op_response,  {ok, _, _}} -> {ok, Row};
            {op_response, {error, Msg}} -> {error, Msg}
        end.

fetchrows(TcpMod, Sock, StmtHandle, XSqlVars, Results) ->
    TcpMod:send(Sock,
        efirebirdsql_op:op_fetch(StmtHandle, XSqlVars)),
    {op_fetch_response, {NewResults, MoreData}} = efirebirdsql_op:get_fetch_response(TcpMod, Sock, XSqlVars),
    case MoreData of
        true -> fetchrows(TcpMod, Sock,
            StmtHandle, XSqlVars,lists:flatten([Results, NewResults]));
        false -> {ok, Results ++ NewResults}
    end.
fetchrows(TcpMod, Sock, StmtHandle, XSqlVars) ->
    fetchrows(TcpMod, Sock, StmtHandle, XSqlVars, []).

description([], XSqlVar) ->
    lists:reverse(XSqlVar);
description(InXSqlVars, XSqlVar) ->
    [H | T] = InXSqlVars,
    description(T, [{H#column.name, H#column.type, H#column.scale,
                      H#column.length, H#column.null_ind} | XSqlVar]).

%% Commit and rollback
commit(TcpMod, Sock, TransHandle) ->
    TcpMod:send(Sock, efirebirdsql_op:op_commit_retaining(TransHandle)),
    case efirebirdsql_op:get_response(TcpMod, Sock) of
        {op_response,  {ok, _, _}} -> ok;
        {op_response, {error, Msg}} -> {error, Msg}
    end.

rollback(TcpMod, Sock, TransHandle) ->
    TcpMod:send(Sock, efirebirdsql_op:op_rollback_retaining(TransHandle)),
    case efirebirdsql_op:get_response(TcpMod, Sock) of
        {op_response,  {ok, _, _}} -> ok;
        {op_response, {error, Msg}} -> {error, Msg}
    end.
