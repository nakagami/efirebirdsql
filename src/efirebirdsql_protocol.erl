%%% The MIT License (MIT)

%%% Copyright (c) 2016-2018 Hajime Nakagami<nakagami@gmail.com>

-module(efirebirdsql_protocol).

-export([connect/5, connect/6, detach/1, begin_transaction/2]).
-export([allocate_statement/1, prepare_statement/2]).
-export([execute/2, description/1]).
-export([commit/1, rollback/1]).
-export([fetchone/1, fetchall/1]).

-include("efirebirdsql.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Utility functions in module

connect_database(Host, Database, IsCreateDB, PageSize, State) ->
    S2 = efirebirdsql_socket:send(State,
        efirebirdsql_op:op_connect(Host, Database, State)),
    case efirebirdsql_op:get_connect_response(S2) of
        {ok, S3} ->
            S4 = case IsCreateDB of
                true ->
                    efirebirdsql_socket:send(S3,
                        efirebirdsql_op:op_create(Database, PageSize, S3));
                false ->
                    efirebirdsql_socket:send(S3,
                        efirebirdsql_op:op_attach(Database, S3))
            end,
            case efirebirdsql_op:get_response(S4) of
                {op_response, {ok, Handle, _}, S5} ->
                    allocate_statement(S5#state{db_handle=Handle});
                {op_response, {error, Msg}, S5} ->
                    {error, Msg, S5}
            end;
        {error, Reason, S3} ->
            {error, Reason, S3}
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
    S2 = efirebirdsql_socket:send(State,
        efirebirdsql_op:op_detach(State#state.db_handle)),
    case efirebirdsql_op:get_response(S2) of
        {op_response,  {ok, _, _}, S3} -> {ok, S3};
        {op_response, {error, Msg}, S3} -> {error, Msg, S3}
    end.

%% Transaction
begin_transaction(AutoCommit, State) ->
    %% isc_tpb_version3,isc_tpb_write,isc_tpb_wait,isc_tpb_read_committed,isc_tpb_no_rec_version
    Tpb = if
        AutoCommit =:= true -> [3, 9, 6, 15, 18, 16];
        AutoCommit =:= false -> [3, 9, 6, 15, 18]
    end,
    S2 = efirebirdsql_socket:send(State,
        efirebirdsql_op:op_transaction(State#state.db_handle, Tpb)),
    case efirebirdsql_op:get_response(S2) of
        {op_response,  {ok, Handle, _}, S3} -> {ok, S3#state{trans_handle=Handle}};
        {op_response, {error, Msg}, S3} -> {error, Msg, S3}
    end.

%% allocate, prepare and free statement
allocate_statement(State) ->
    S2 = efirebirdsql_socket:send(State,
        efirebirdsql_op:op_allocate_statement(State#state.db_handle)),
    case efirebirdsql_op:get_response(S2) of
        {op_response, {ok, Handle, _}, S3} -> {ok, S3#state{stmt_handle=Handle}};
        {op_response, {error, Msg}, S3} ->{error, Msg, S3}
    end.

prepare_statement(Sql, State) ->
    TransHandle = State#state.trans_handle,
    StmtHandle = State#state.stmt_handle,
    S2 = efirebirdsql_socket:send(State,
        efirebirdsql_op:op_prepare_statement(TransHandle, StmtHandle, Sql)),
    efirebirdsql_op:get_prepare_statement_response(S2).

free_statement(State) ->
    S2 = efirebirdsql_socket:send(State,
        efirebirdsql_op:op_free_statement(State#state.stmt_handle)),
    case efirebirdsql_op:get_response(S2) of
        {op_response,  {ok, _, _}, S3} -> {ok, S3};
        {op_response, {error, Msg}, S3} -> {error, Msg, S3}
    end.

%% Execute & Fetch
fetchrows(Results, State) ->
    StmtHandle = State#state.stmt_handle,
    XSqlVars = State#state.xsqlvars,
    S2 = efirebirdsql_socket:send(State,
        efirebirdsql_op:op_fetch(StmtHandle, XSqlVars)),
    {op_fetch_response, NewResults, MoreData, S3} = efirebirdsql_op:get_fetch_response(S2),
    case MoreData of
        true -> fetchrows(lists:flatten([Results, NewResults]), S3);
        false -> {ok, Results ++ NewResults, S3}
    end.
fetchrows(State) ->
    fetchrows([], State).

execute(State, Params, isc_info_sql_stmt_exec_procedure) ->
    TransHandle = State#state.trans_handle,
    StmtHandle = State#state.stmt_handle,
    XSqlVars = State#state.xsqlvars,
    S2 = efirebirdsql_socket:send(State,
        efirebirdsql_op:op_execute2(TransHandle, StmtHandle, Params, XSqlVars)),
    {Row, S3} = efirebirdsql_op:get_sql_response(S2),
    case efirebirdsql_op:get_response(S3) of
        {op_response,  {ok, _, _}, S4} -> {ok, S4#state{rows=[Row]}};
        {op_response, {error, Msg}, S4} -> {error, Msg, S4}
    end
;
execute(State, Params, isc_info_sql_stmt_select) ->
    TransHandle = State#state.trans_handle,
    StmtHandle = State#state.stmt_handle,
    S2 = efirebirdsql_socket:send(State,
        efirebirdsql_op:op_execute(TransHandle, StmtHandle, Params)),
    case efirebirdsql_op:get_response(S2) of
        {op_response, {ok, _, _}, S3} ->
            case fetchrows(S3) of
                {ok, Rows, S4} ->
                    {ok, S5} = free_statement(S4#state{rows=Rows}),
                    {ok, S5};
                {error, Msg, S4} -> {error, Msg, S4}
            end;
        {op_response, {error, Msg}, S3} ->
            {error, Msg, S3}
    end
;
execute(State, Params, _StmtType) ->
    TransHandle = State#state.trans_handle,
    StmtHandle = State#state.stmt_handle,
    S2 = efirebirdsql_socket:send(State,
        efirebirdsql_op:op_execute(TransHandle, StmtHandle, Params)),
    case efirebirdsql_op:get_response(S2) of
        {op_response,  {ok, _, _}, S3} -> {ok, S3};
        {op_response, {error, Msg}, S3} -> {error, Msg, S3}
    end.

execute(State, Params) ->
    execute(State, Params, State#state.stmt_type).

%% Description
description([], XSqlVar) ->
    lists:reverse(XSqlVar);
description(InXSqlVars, XSqlVar) ->
    [H | T] = InXSqlVars,
    description(T, [{H#column.name, H#column.type, H#column.scale,
                      H#column.length, H#column.null_ind} | XSqlVar]).
description(State) ->
    description(State#state.xsqlvars, []).

%% Commit and rollback
commit(State) ->
    S2 = efirebirdsql_socket:send(State,
        efirebirdsql_op:op_commit_retaining(State#state.trans_handle)),
    case efirebirdsql_op:get_response(S2) of
        {op_response,  {ok, _, _}, S3} -> {ok, S3};
        {op_response, {error, Msg}, S3} -> {{error, Msg}, S3}
    end.

rollback(State) ->
    S2 = efirebirdsql_socket:send(State,
        efirebirdsql_op:op_rollback_retaining(State#state.trans_handle)),
    case efirebirdsql_op:get_response(S2) of
        {op_response,  {ok, _, _}, S3} -> {ok, S3};
        {op_response, {error, Msg}, S3} -> {{error, Msg}, S3}
    end.

fetchone(State) ->
    [R | Rest] = State#state.rows,
    {ConvertedRow, S2} = efirebirdsql_op:convert_row(
        State#state{rows=Rest}, State#state.xsqlvars, R
    ),
    {ok, ConvertedRow, S2}.

fetchall(State, ConvertedRows, []) ->
    {ok, lists:reverse(ConvertedRows), State};
fetchall(State, ConvertedRows, Rows) ->
    [R | Rest] = Rows,
    {ConvertedRow, S2} = efirebirdsql_op:convert_row(
        State, State#state.xsqlvars, R
    ),
    fetchall(S2, [ConvertedRow | ConvertedRows], Rest).
fetchall(State) ->
    fetchall(State, [], State#state.rows).
