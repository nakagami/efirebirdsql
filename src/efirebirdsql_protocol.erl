%%% The MIT License (MIT)

%%% Copyright (c) 2016-2018 Hajime Nakagami<nakagami@gmail.com>

-module(efirebirdsql_protocol).

-export([connect/5, close/1, begin_transaction/2]).
-export([allocate_statement/1, prepare_statement/3, free_statement/3]).
-export([execute/3, description/1]).
-export([commit/1, rollback/1]).
-export([fetchone/2, fetchall/2]).

-include("efirebirdsql.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Utility functions in module

connect_database(Conn, Host, Database, IsCreateDB, PageSize) ->
    C2 = efirebirdsql_socket:send(Conn,
        efirebirdsql_op:op_connect(Conn, Host, Database)),
    case efirebirdsql_op:get_connect_response(C2) of
    {ok, C3} ->
        C4 = case IsCreateDB of
        true ->
            efirebirdsql_socket:send(C3, efirebirdsql_op:op_create(C3, Database, PageSize));
        false ->
            efirebirdsql_socket:send(C3, efirebirdsql_op:op_attach(C3, Database))
        end,
        case efirebirdsql_op:get_response(C4) of
        {op_response, {ok, Handle, _}, C5} -> {ok, C5#conn{db_handle=Handle}};
        {op_response, {error, Msg}, C5} -> {error, Msg, C5}
        end;
    {error, Reason, C3} ->
        {error, Reason, C3}
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% public functions

connect(Host, Username, Password, Database, Options) ->
    SockOptions = [{active, false}, {packet, raw}, binary],
    Port = proplists:get_value(port, Options, 3050),
    IsCreateDB = proplists:get_value(createdb, Options, false),
    PageSize = proplists:get_value(pagesize, Options, 4096),
    Private = efirebirdsql_srp:get_private_key(),
    Public = efirebirdsql_srp:client_public(Private),
    case gen_tcp:connect(Host, Port, SockOptions) of
    {ok, Sock} ->
        Conn = #conn{
            sock=Sock,
            user=string:to_upper(Username),
            password=Password,
            client_private=Private,
            client_public=Public,
            auth_plugin=proplists:get_value(auth_plugin, Options, "Srp"),
            wire_crypt=proplists:get_value(wire_crypt, Options, true)
        },
        connect_database(Conn, Host, Database, IsCreateDB, PageSize);
    {error, Reason} ->
        {error, Reason}
    end.

close(Conn) ->
    C2 = efirebirdsql_socket:send(Conn,
        efirebirdsql_op:op_commit_retaining(Conn#conn.trans_handle)),
    C3 = efirebirdsql_socket:send(C2,
        efirebirdsql_op:op_detach(C2#conn.db_handle)),
    case efirebirdsql_op:get_response(C3) of
    {op_response,  {ok, _, _}, C4} ->
        gen_tcp:close(C4#conn.sock),
        {ok, #conn{}};
    {op_response, {error, Msg}, C4} ->
        {error, Msg, C4}
    end.

%% Transaction
begin_transaction(AutoCommit, Conn) ->
    %% isc_tpb_version3,isc_tpb_write,isc_tpb_wait,isc_tpb_read_committed,isc_tpb_no_rec_version
    Tpb = if
        AutoCommit =:= true -> [3, 9, 6, 15, 18, 16];
        AutoCommit =:= false -> [3, 9, 6, 15, 18]
        end,
    C2 = efirebirdsql_socket:send(Conn,
        efirebirdsql_op:op_transaction(Conn#conn.db_handle, Tpb)),
    case efirebirdsql_op:get_response(C2) of
    {op_response,  {ok, Handle, _}, C3} -> {ok, C3#conn{trans_handle=Handle}};
    {op_response, {error, Msg}, C3} -> {error, Msg, C3}
    end.

%% allocate, prepare and free statement
allocate_statement(Conn) ->
    C2 = efirebirdsql_socket:send(Conn,
        efirebirdsql_op:op_allocate_statement(Conn#conn.db_handle)),
    case efirebirdsql_op:get_response(C2) of
    {op_response, {ok, Handle, _}, C3} -> {ok, C3, #stmt{stmt_handle=Handle}};
    {op_response, {error, Msg}, C3} ->{error, Msg, C3}
    end.

prepare_statement(Sql, Conn, Stmt) ->
    TransHandle = Conn#conn.trans_handle,
    StmtHandle = Stmt#stmt.stmt_handle,
    C2 = efirebirdsql_socket:send(Conn,
        efirebirdsql_op:op_prepare_statement(TransHandle, StmtHandle, Sql)),
    efirebirdsql_op:get_prepare_statement_response(C2, Stmt).

free_statement(Conn, Stmt, Type) ->
    C2 = efirebirdsql_socket:send(Conn,
        efirebirdsql_op:op_free_statement(Stmt#stmt.stmt_handle, Type)),
    case efirebirdsql_op:get_response(C2) of
    {op_response,  {ok, _, _}, C3} -> {ok, C3};
    {op_response, {error, Msg}, C3} -> {error, Msg, C3}
    end.

%% Execute & Fetch
fetchrows(Results, Conn, Stmt) ->
    StmtHandle = Stmt#stmt.stmt_handle,
    XSqlVars = Stmt#stmt.xsqlvars,
    C2 = efirebirdsql_socket:send(Conn,
        efirebirdsql_op:op_fetch(StmtHandle, XSqlVars)),
    {op_fetch_response, NewResults, MoreData, C3} = efirebirdsql_op:get_fetch_response(C2, Stmt),
    case MoreData of
    true -> fetchrows(lists:flatten([Results, NewResults]), C3, Stmt);
    false -> {ok, Results ++ NewResults, C3}
    end.
fetchrows(Conn, Stmt) ->
    fetchrows([], Conn, Stmt).

execute(Conn, Stmt, Params, isc_info_sql_stmt_exec_procedure) ->
    C2 = efirebirdsql_socket:send(Conn,
        efirebirdsql_op:op_execute2(Conn, Stmt, Params)),
    {Row, C3} = efirebirdsql_op:get_sql_response(C2, Stmt),
    case efirebirdsql_op:get_response(C3) of
    {op_response,  {ok, _, _}, C4} -> {ok, C4, [Row]};
    {op_response, {error, Msg}, C4} -> {error, Msg, C4}
    end
;
execute(Conn, Stmt, Params, isc_info_sql_stmt_select) ->
    C2 = efirebirdsql_socket:send(Conn,
        efirebirdsql_op:op_execute(Conn, Stmt, Params)),
    case efirebirdsql_op:get_response(C2) of
    {op_response, {ok, _, _}, C3} ->
        case fetchrows(C3, Stmt) of
        {ok, Rows, C4} ->
            {ok, C5} = free_statement(C4, Stmt, close),
            {ok, C5, Rows};
        {error, Msg, C4} ->
            {error, Msg, C4}
        end;
    {op_response, {error, Msg}, C3} ->
        {error, Msg, C3}
    end
;
execute(Conn, Stmt, Params, _StmtType) ->
    C2 = efirebirdsql_socket:send(Conn,
        efirebirdsql_op:op_execute(Conn, Stmt, Params)),
    case efirebirdsql_op:get_response(C2) of
    {op_response,  {ok, _, _}, C3} -> {ok, C3, []};
    {op_response, {error, Msg}, C3} -> {error, Msg, C3}
    end.

execute(Conn, Stmt, Params) ->
    {ok, C2, Rows} = execute(Conn, Stmt, Params, Stmt#stmt.stmt_type),
    {ok, C2, Stmt#stmt{rows=Rows}}.

%% Description
description([], XSqlVar) ->
    lists:reverse(XSqlVar);
description(InXSqlVars, XSqlVar) ->
    [H | T] = InXSqlVars,
    description(T, [{H#column.name, H#column.type, H#column.scale,
                      H#column.length, H#column.null_ind} | XSqlVar]).
description(Stmt) ->
    description(Stmt#stmt.xsqlvars, []).

%% Commit and rollback
commit(Conn) ->
    C2 = efirebirdsql_socket:send(Conn,
        efirebirdsql_op:op_commit_retaining(Conn#conn.trans_handle)),
    case efirebirdsql_op:get_response(C2) of
    {op_response,  {ok, _, _}, C3} -> {ok, C3};
    {op_response, {error, Msg}, C3} -> {{error, Msg}, C3}
    end.

rollback(Conn) ->
    C2 = efirebirdsql_socket:send(Conn,
        efirebirdsql_op:op_rollback_retaining(Conn#conn.trans_handle)),
    case efirebirdsql_op:get_response(C2) of
    {op_response,  {ok, _, _}, C3} -> {ok, C3};
    {op_response, {error, Msg}, C3} -> {{error, Msg}, C3}
    end.

fetchone(Conn, Stmt) ->
    [R | Rest] = Stmt#stmt.rows,
    {ConvertedRow, C2} = efirebirdsql_op:convert_row(Conn, Stmt#stmt.xsqlvars, R),
    {ok, ConvertedRow, C2, Stmt#stmt{rows=Rest}}.

fetchall(Conn, _Stmt, ConvertedRows, []) ->
    {ok, lists:reverse(ConvertedRows), Conn};
fetchall(Conn, Stmt, ConvertedRows, Rows) ->
    [R | Rest] = Rows,
    {ConvertedRow, C2} = efirebirdsql_op:convert_row(
        Conn, Stmt#stmt.xsqlvars, R
    ),
    fetchall(C2, Stmt, [ConvertedRow | ConvertedRows], Rest).
fetchall(Conn, Stmt) ->
    fetchall(Conn, Stmt, [], Stmt#stmt.rows).
