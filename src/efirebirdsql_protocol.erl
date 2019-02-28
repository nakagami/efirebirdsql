%%% The MIT License (MIT)

%%% Copyright (c) 2016-2018 Hajime Nakagami<nakagami@gmail.com>

-module(efirebirdsql_protocol).

-export([connect/5, close/1, begin_transaction/2]).
-export([allocate_statement/1, prepare_statement/3, free_statement/3, columns/1]).
-export([execute/2, execute/3]).
-export([fetchone/2, fetchall/2, fetchsegment/2]).
-export([description/1]).
-export([commit/1, rollback/1]).

-include("efirebirdsql.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Utility functions in module

-spec connect_database(conn(), list(), list(), boolean(), integer()) -> {ok, conn()} | {error, binary(), conn()}.
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

ready_fetch_segment(Conn, Stmt) when Stmt#stmt.rows =:= [], Stmt#stmt.more_data =:= true ->
    StmtHandle = Stmt#stmt.stmt_handle,
    XSqlVars = Stmt#stmt.xsqlvars,
    C2 = efirebirdsql_socket:send(Conn,
        efirebirdsql_op:op_fetch(StmtHandle, XSqlVars)),
    {op_fetch_response, Rows, MoreData, C3} = efirebirdsql_op:get_fetch_response(C2, Stmt),
    Stmt2 = Stmt#stmt{rows=Rows, more_data=MoreData},
    {ok, C4, Stmt3} = if MoreData =:= true ->
        {ok, C3, Stmt2};
    MoreData =:= false ->
        free_statement(C3, Stmt2, close)
    end,
    {C4, Stmt3};
ready_fetch_segment(Conn, Stmt) ->
    {Conn, Stmt}.

fetchrow(Conn, Stmt) ->
    Rows = Stmt#stmt.rows,
    case Rows of
    [] ->
        {nil, Conn, Stmt};
    _ ->
        [R | Rest] = Rows,
        {ConvertedRow, C2} = efirebirdsql_op:convert_row(Conn, Stmt#stmt.xsqlvars, R),
        {ConvertedRow, C2, Stmt#stmt{rows=Rest}}
    end.

-spec puts_timezone_data(binary(), binary(), tuple()) -> tuple().
puts_timezone_data(TimeZoneNameById, TimeZoneIdByName, {nil, Conn, Stmt}) ->
    {TimeZoneNameById, TimeZoneIdByName, Conn, Stmt};
puts_timezone_data(TimeZoneNameById, TimeZoneIdByName, {[{_, ID}, {_, Name}], Conn, Stmt}) ->
    TimeZoneNameById2 = maps:put(ID, Name, TimeZoneNameById),
    TimeZoneIdByName2 = maps:put(Name, ID, TimeZoneIdByName),
    puts_timezone_data(TimeZoneNameById2, TimeZoneIdByName2, fetchone(Conn, Stmt)).

-spec load_timezone_data(conn()) -> {ok, conn()}.
load_timezone_data(Conn) ->
    {ok, C1, Stmt} = allocate_statement(Conn),
    {ok, C2, Stmt2} = prepare_statement(
        <<"select count(*) from rdb$relations where rdb$relation_name='RDB$TIME_ZONES' and rdb$system_flag=1">>, C1, Stmt),
    {ok, C4, Stmt3} = execute(C2, Stmt2),
    {[{_, Count}], C5, Stmt4} = fetchone(C4, Stmt3),

    TimeZoneNameById = maps:new(),
    TimeZoneIdByName = maps:new(),
    case Count of
    0 ->
        {ok, NewConn, _} = free_statement(C5, Stmt4, drop),
        TimeZoneNameById2 = TimeZoneNameById,
        TimeZoneIdByName2 = TimeZoneIdByName;
    _ ->
        {ok, C6, Stmt5} = prepare_statement(
            <<"select rdb$time_zone_id, rdb$time_zone_name from rdb$time_zones">>, C5, Stmt4),
        {ok, C7, Stmt6} = execute(C6, Stmt5),
        {TimeZoneNameById2, TimeZoneIdByName2, C8, Stmt7} = puts_timezone_data(TimeZoneNameById, TimeZoneIdByName, fetchone(C7, Stmt6)),
        {ok, NewConn, _} = free_statement(C8, Stmt7, drop)
    end,
    {ok, NewConn#conn{timezone_name_by_id=TimeZoneNameById2, timezone_id_by_name=TimeZoneIdByName2}}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% public functions

-spec connect(list(), list(), list(), list(), list()) -> {ok, conn()} | {error, binary(), conn()}.
connect(Host, Username, Password, Database, Options) ->
    SockOptions = [{active, false}, {packet, raw}, binary],
    Port = proplists:get_value(port, Options, 3050),
    IsCreateDB = proplists:get_value(createdb, Options, false),
    PageSize = proplists:get_value(pagesize, Options, 4096),
    AutoCommit = proplists:get_value(auto_commit, Options, true),
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
            wire_crypt=proplists:get_value(wire_crypt, Options, true),
            timezone=proplists:get_value(timezone, Options, nil)
        },
        case connect_database(Conn, Host, Database, IsCreateDB, PageSize) of
        {ok, C2} ->
            case begin_transaction(AutoCommit, C2) of
            {ok, C3} -> load_timezone_data(C3);
            {error, Reason, C3} -> {error, Reason, C3}
            end;
        {error, Reason, C2} ->
            {error, Reason, C2}
        end;
    {error, Reason} ->
        {error, Reason, #conn{}}
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
    {ok, C2, S2} = case Stmt#stmt.closed of
    true -> {ok, Conn, Stmt};
    false -> free_statement(Conn, Stmt, close)
    end,

    TransHandle = C2#conn.trans_handle,
    StmtHandle = S2#stmt.stmt_handle,
    C3 = efirebirdsql_socket:send(C2,
        efirebirdsql_op:op_prepare_statement(TransHandle, StmtHandle, Sql)),
    efirebirdsql_op:get_prepare_statement_response(C3, S2).

free_statement(Conn, Stmt, Type) ->
    C2 = efirebirdsql_socket:send(Conn,
        efirebirdsql_op:op_free_statement(Stmt#stmt.stmt_handle, Type)),
    case efirebirdsql_op:get_response(C2) of
    {op_response,  {ok, _, _}, C3} -> {ok, C3, Stmt#stmt{closed=true}};
    {op_response, {error, Msg}, C3} -> {error, Msg, C3}
    end.

columns(Columns, []) ->
    lists:reverse(Columns);
columns(Columns, XSQLVars) ->
    [C | Rest] = XSQLVars,
    columns([{C#column.name, C#column.type, C#column.scale, C#column.length, C#column.null_ind} | Columns], Rest).
columns(Stmt) ->
    columns([], Stmt#stmt.xsqlvars).


%% Execute

execute(Conn, Stmt, Params, isc_info_sql_stmt_exec_procedure) ->
    C2 = efirebirdsql_socket:send(Conn,
        efirebirdsql_op:op_execute2(Conn, Stmt, Params)),
    {Row, C3} = efirebirdsql_op:get_sql_response(C2, Stmt),
    case efirebirdsql_op:get_response(C3) of
    {op_response,  {ok, _, _}, C4} -> {ok, C4, Stmt#stmt{rows=[Row], more_data=false}};
    {op_response, {error, Msg}, C4} -> {error, Msg, C4}
    end;
execute(Conn, Stmt, Params, isc_info_sql_stmt_select) ->
    C2 = efirebirdsql_socket:send(Conn,
        efirebirdsql_op:op_execute(Conn, Stmt, Params)),
    case efirebirdsql_op:get_response(C2) of
    {op_response, {ok, _, _}, C3} ->
        {ok, C3, Stmt#stmt{rows=[], more_data=true, closed=false}};
    {op_response, {error, Msg}, C3} ->
        {error, Msg, C3}
    end;
execute(Conn, Stmt, Params, _StmtType) ->
    C2 = efirebirdsql_socket:send(Conn,
        efirebirdsql_op:op_execute(Conn, Stmt, Params)),
    case efirebirdsql_op:get_response(C2) of
    {op_response,  {ok, _, _}, C3} -> {ok, C3, Stmt#stmt{rows=[], more_data=false}};
    {op_response, {error, Msg}, C3} -> {error, Msg, C3}
    end.

execute(Conn, Stmt, Params) ->
    execute(Conn, Stmt, Params, Stmt#stmt.stmt_type).
execute(Conn, Stmt) ->
    execute(Conn, Stmt, []).


%% Fetch

fetchone(Conn, Stmt) ->
    {C2, S2} = ready_fetch_segment(Conn, Stmt),
    fetchrow(C2, S2).

fetchall(Rows, {nil, Conn, Stmt}) ->
    {ok, lists:reverse(Rows), Conn, Stmt};
fetchall(Rows, {Row, Conn, Stmt}) ->
    fetchall([Row | Rows], fetchone(Conn, Stmt));
fetchall(Conn, Stmt) ->
    fetchall([], fetchone(Conn, Stmt)).

fetchsegment(Rows, {nil, Conn, Stmt}) ->
    {ok, lists:reverse(Rows), Conn, Stmt};
fetchsegment(Rows, {Row, Conn, Stmt}) ->
    fetchsegment([Row | Rows], fetchrow(Conn, Stmt));
fetchsegment(Conn, Stmt) ->
    {C2, S2} = ready_fetch_segment(Conn, Stmt),
    fetchsegment([], fetchrow(C2, S2)).

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
