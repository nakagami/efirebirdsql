%%% The MIT License (MIT)

%%% Copyright (c) 2016-2021 Hajime Nakagami<nakagami@gmail.com>

-module(efirebirdsql_protocol).

-export([connect/5, close/1, begin_transaction/2]).
-export([allocate_statement/1, prepare_statement/3, free_statement/3, columns/1]).
-export([execute/2, execute/3, rowcount/2, exec_immediate/2]).
-export([fetchone/2, fetchall/2]).
-export([description/1]).
-export([commit/1, rollback/1]).

-include("efirebirdsql.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Utility functions in module

-spec connect_database(conn(), string(), list(), boolean(), integer()) -> {ok, conn()} | {error, integer(), binary(), conn()}.
connect_database(Conn, Host, Database, IsCreateDB, PageSize) ->
    efirebirdsql_socket:send(Conn,
        efirebirdsql_op:op_connect(Host, Conn#conn.user, Conn#conn.client_public, Conn#conn.auth_plugin, Conn#conn.wire_crypt, Database)),
    case efirebirdsql_op:get_connect_response(Conn) of
    {ok, NewConn} ->
        case IsCreateDB of
        true ->
            efirebirdsql_socket:send(NewConn, efirebirdsql_op:op_create(NewConn, Database, PageSize));
        false ->
            efirebirdsql_socket:send(NewConn, efirebirdsql_op:op_attach(NewConn, Database))
        end,
        case efirebirdsql_op:get_response(NewConn) of
        {op_response, Handle, _} -> {ok, NewConn#conn{db_handle=Handle}};
        {op_fetch_response, _, _} -> {error, <<"Unknown op_fetch_response">>, NewConn};
        {op_sql_response, _} -> {error, <<"Unknown op_sql_response">>, NewConn};
        {error, ErrNo, Msg} -> {error, ErrNo, Msg, NewConn}
        end;
    {error, Reason, NewConn} ->
        {error, Reason, NewConn}
    end.

-spec ready_fetch_segment(conn(), stmt()) -> stmt().
ready_fetch_segment(Conn, Stmt) when Stmt#stmt.rows =:= [], Stmt#stmt.more_data =:= true ->
    StmtHandle = Stmt#stmt.stmt_handle,
    XSqlVars = Stmt#stmt.xsqlvars,
    efirebirdsql_socket:send(Conn,
        efirebirdsql_op:op_fetch(StmtHandle, XSqlVars)),
    {ok, Rows, MoreData} = efirebirdsql_op:get_fetch_response(Conn, Stmt),
    Stmt2 = Stmt#stmt{rows=Rows, more_data=MoreData},
    {ok, Stmt3} = if
        MoreData =:= true -> {ok, Stmt2};
        MoreData =:= false -> free_statement(Conn, Stmt2, close)
    end,
    Stmt3;
ready_fetch_segment(_Conn, Stmt) ->
    Stmt.

fetchrow(Conn, Stmt) ->
    Rows = Stmt#stmt.rows,
    case Rows of
    [] ->
        {nil, Stmt};
    _ ->
        [R | Rest] = Rows,
        ConvertedRow = efirebirdsql_op:convert_row(Conn, Stmt#stmt.xsqlvars, R),
        {ConvertedRow, Stmt#stmt{rows=Rest}}
    end.

puts_timezone_data(_Conn, TimeZoneNameById, TimeZoneIdByName, {nil, Stmt}) ->
    {TimeZoneNameById, TimeZoneIdByName, Stmt};
puts_timezone_data(Conn, TimeZoneNameById, TimeZoneIdByName, {[{_, ID}, {_, Name}], Stmt}) ->
    TimeZoneNameById2 = maps:put(ID, Name, TimeZoneNameById),
    TimeZoneIdByName2 = maps:put(Name, ID, TimeZoneIdByName),
    puts_timezone_data(Conn, TimeZoneNameById2, TimeZoneIdByName2, fetchone(Conn, Stmt)).

-spec load_timezone_data(conn()) -> {ok, conn()}.
load_timezone_data(Conn) ->
    {ok, Stmt} = allocate_statement(Conn),
    {ok, Stmt2} = prepare_statement(
        <<"select count(*) from rdb$relations where rdb$relation_name='RDB$TIME_ZONES' and rdb$system_flag=1">>, Conn, Stmt),
    {ok, Stmt3} = execute(Conn, Stmt2),
    {[{_, Count}], Stmt4} = fetchone(Conn, Stmt3),

    TimeZoneNameById = maps:new(),
    TimeZoneIdByName = maps:new(),
    case Count of
    0 ->
        {ok, _} = free_statement(Conn, Stmt4, drop),
        TimeZoneNameById2 = TimeZoneNameById,
        TimeZoneIdByName2 = TimeZoneIdByName;
    _ ->
        {ok, Stmt5} = prepare_statement(
            <<"select rdb$time_zone_id, rdb$time_zone_name from rdb$time_zones">>, Conn, Stmt4),
        {ok, Stmt6} = execute(Conn, Stmt5),
        {TimeZoneNameById2, TimeZoneIdByName2, Stmt8} = puts_timezone_data(Conn, TimeZoneNameById, TimeZoneIdByName, fetchone(Conn, Stmt6)),
        {ok, _} = free_statement(Conn, Stmt8, drop)
    end,
    {ok, Conn#conn{timezone_name_by_id=TimeZoneNameById2, timezone_id_by_name=TimeZoneIdByName2}}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% public functions

-spec connect(string(), string(), string(), string(), list()) -> {ok, conn()} | {error, integer(), binary(), conn()}.
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
            auto_commit=AutoCommit,
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
            {error, ErrNo, Reason, C3} -> {error, ErrNo, Reason, C3}
            end;
        {error, ErrNo, Reason, C2} ->
            {error, ErrNo, Reason, C2}
        end;
    {error, Reason} ->
        {error, 0, atom_to_binary(Reason, latin1), #conn{
            user=string:to_upper(Username),
            password=Password,
            client_private=Private,
            client_public=Public,
            auth_plugin=proplists:get_value(auth_plugin, Options, "Srp"),
            wire_crypt=proplists:get_value(wire_crypt, Options, true),
            auto_commit=proplists:get_value(auto_commit, Options, false),
            timezone=proplists:get_value(timezone, Options, nil)
        }}
    end.

-spec close(conn()) -> {ok, conn()} | {error, integer(), binary(), conn()}.
close(Conn) ->
    efirebirdsql_socket:send(Conn,
        efirebirdsql_op:op_commit_retaining(Conn#conn.trans_handle)),
    efirebirdsql_socket:send(Conn,
        efirebirdsql_op:op_detach(Conn#conn.db_handle)),
    case efirebirdsql_op:get_response(Conn) of
    {op_response, _, _} ->
        gen_tcp:close(Conn#conn.sock),
        {ok, Conn#conn{sock=undefined}};
    {error, ErrNo, Msg} ->
        {error, ErrNo, Msg, Conn}
    end.

%% Transaction
-spec begin_transaction(boolean(), conn()) -> {ok, conn()} | {error, integer(), binary(), conn()}.
begin_transaction(AutoCommit, Conn) ->
    %% ISOLATION_LEVEL_READ_COMMITED
    %% isc_tpb_version3,isc_tpb_write,isc_tpb_wait,isc_tpb_read_committed,isc_tpb_rec_version
    Tpb = if
        AutoCommit =:= true -> [3, 9, 6, 15, 17, 16];
        AutoCommit =:= false -> [3, 9, 6, 15, 17]
        end,
    efirebirdsql_socket:send(Conn,
        efirebirdsql_op:op_transaction(Conn#conn.db_handle, Tpb)),
    case efirebirdsql_op:get_response(Conn) of
    {op_response, Handle, _} -> {ok, Conn#conn{trans_handle=Handle}};
    {error, ErrNo, Msg} -> {error, ErrNo, Msg, Conn}
    end.

%% allocate, prepare and free statement
-spec allocate_statement(conn()) -> {ok, stmt()} | {error, integer(), binary()}.
allocate_statement(Conn) ->
    efirebirdsql_socket:send(Conn,
        efirebirdsql_op:op_allocate_statement(Conn#conn.db_handle)),
    case efirebirdsql_op:get_response(Conn) of
    {op_response, Handle, _} -> {ok, #stmt{stmt_handle=Handle}};
    {error, ErrNo, Msg} -> {error, ErrNo, Msg}
    end.

-spec prepare_statement(binary(), conn(), stmt()) -> {ok, stmt()} | {error, integer(), binary()}.
prepare_statement(Sql, Conn, Stmt) ->
    {ok, Stmt2} = case Stmt#stmt.closed of
    true -> {ok, Stmt};
    false -> free_statement(Conn, Stmt, close)
    end,
    efirebirdsql_socket:send(Conn,
        efirebirdsql_op:op_prepare_statement(Conn#conn.trans_handle, Stmt2#stmt.stmt_handle, Sql)),
    efirebirdsql_op:get_prepare_statement_response(Conn, Stmt2).

-spec free_statement(conn(), stmt(), atom()) -> {ok, stmt()} | {error, integer(), binary()}.
free_statement(Conn, Stmt, Type) ->
    efirebirdsql_socket:send(Conn,
        efirebirdsql_op:op_free_statement(Stmt#stmt.stmt_handle, Type)),
    case efirebirdsql_op:get_response(Conn) of
    {op_response, _, _} -> {ok, Stmt#stmt{closed=true}};
    {error, ErrNo, Msg} -> {error, ErrNo, Msg}
    end.

-spec columns(stmt()) -> list().
columns(Columns, []) ->
    lists:reverse(Columns);
columns(Columns, XSQLVars) ->
    [C | Rest] = XSQLVars,
    columns([{C#column.name, C#column.type, C#column.scale, C#column.length, C#column.null_ind} | Columns], Rest).
columns(Stmt) ->
    columns([], Stmt#stmt.xsqlvars).


%% Execute

execute(Conn, Stmt, Params, isc_info_sql_stmt_exec_procedure) ->
    efirebirdsql_socket:send(Conn, efirebirdsql_op:op_execute2(Conn, Stmt, Params)),
    {Row, C3} = efirebirdsql_op:get_sql_response(Conn, Stmt),
    case efirebirdsql_op:get_response(C3) of
    {op_response, _, _} -> {ok, Stmt#stmt{rows=[Row], more_data=false}};
    {error, ErrNo, Msg} -> {error, ErrNo, Msg}
    end;
execute(Conn, Stmt, Params, isc_info_sql_stmt_select) ->
    efirebirdsql_socket:send(Conn, efirebirdsql_op:op_execute(Conn, Stmt, Params)),
    case efirebirdsql_op:get_response(Conn) of
    {op_response, _, _} ->
        {ok, Stmt#stmt{rows=[], more_data=true, closed=false}};
    {error, ErrNo, Msg} ->
        {error, ErrNo, Msg}
    end;
execute(Conn, Stmt, Params, _StmtType) ->
    efirebirdsql_socket:send(Conn, efirebirdsql_op:op_execute(Conn, Stmt, Params)),
    case efirebirdsql_op:get_response(Conn) of
    {op_response, _, _} -> {ok, Stmt#stmt{rows=nil, more_data=false}};
    {error, ErrNo, Msg} -> {error, ErrNo, Msg}
    end.

-spec execute(conn(), stmt(), list()) -> {ok, stmt()} | {error, integer(), binary()}.
execute(Conn, Stmt, Params) ->
    execute(Conn, Stmt, Params, Stmt#stmt.stmt_type).
execute(Conn, Stmt) ->
    execute(Conn, Stmt, []).

-spec rowcount(conn(), stmt()) -> {ok, integer()} | {error, integer(), binary()}.
rowcount(_Conn, Stmt) when Stmt#stmt.stmt_type =:= isc_info_sql_stmt_ddl ->
    {ok, 0};
rowcount(Conn, Stmt) ->
    efirebirdsql_socket:send(Conn,
        efirebirdsql_op:op_info_sql(Stmt#stmt.stmt_handle, [23])), % 23:isc_info_sql_records
    case efirebirdsql_op:get_response(Conn) of
    {op_response, _, Buf} ->
        << _:48, Count1:32/little-unsigned, _:24, Count2:32/little-unsigned, _:24, Count3:32/little-unsigned, _:24, Count4:32/little-unsigned, _Rest/binary >> = Buf,
        {ok, Count1+Count2+Count3+Count4};
    {error, ErrNo, Msg} ->
        {error, ErrNo, Msg}
    end.

-spec exec_immediate(binary(), conn()) -> ok | {error, integer(), binary()}.
exec_immediate(Sql, Conn) ->
    efirebirdsql_socket:send(Conn,
        efirebirdsql_op:op_exec_immediate(Conn, Sql)),
    case efirebirdsql_op:get_response(Conn) of
    {op_response, _, _} -> ok;
    {error, ErrNo, Msg} -> {error, ErrNo, Msg}
    end.

%% Fetch

-spec fetchone(conn(), stmt()) -> {list() | nil, stmt()}.
fetchone(Conn, Stmt) ->
    Stmt2 = ready_fetch_segment(Conn, Stmt),
    fetchrow(Conn, Stmt2).

fetch_all(_Conn, Rows, nil, Stmt) ->
    {ok, lists:reverse(Rows), Stmt};
fetch_all(Conn, Rows, Row, Stmt) ->
    {NextRow, Stmt2} = fetchone(Conn, Stmt),
    fetch_all(Conn, [Row | Rows], NextRow, Stmt2).

-spec fetchall(conn(), stmt()) -> {ok, list() | nil, stmt()}.
fetchall(_Conn, Stmt) when Stmt#stmt.rows =:= nil ->
    {ok, nil, Stmt};
fetchall(Conn, Stmt) ->
    {NextRow, Stmt2} = fetchone(Conn, Stmt),
    fetch_all(Conn, [], NextRow, Stmt2).

%% Description
-spec description(stmt()) -> list().
description([], XSqlVar) ->
    lists:reverse(XSqlVar);
description(InXSqlVars, XSqlVar) ->
    [H | T] = InXSqlVars,
    description(T, [{H#column.name, H#column.type, H#column.scale,
                      H#column.length, H#column.null_ind} | XSqlVar]).
description(Stmt) ->
    description(Stmt#stmt.xsqlvars, []).

%% Commit and rollback
-spec commit(conn()) -> ok | {error, integer(), binary()}.
commit(Conn) ->
    efirebirdsql_socket:send(Conn,
        efirebirdsql_op:op_commit_retaining(Conn#conn.trans_handle)),
    case efirebirdsql_op:get_response(Conn) of
    {op_response, _, _} -> ok;
    {error, ErrNo, Msg} -> {error, ErrNo, Msg}
    end.

-spec rollback(conn()) -> ok | {error, integer(), binary()}.
rollback(Conn) ->
    efirebirdsql_socket:send(Conn,
        efirebirdsql_op:op_rollback_retaining(Conn#conn.trans_handle)),
    case efirebirdsql_op:get_response(Conn) of
    {op_response, _, _} -> ok;
    {error, ErrNo, Msg} -> {error, ErrNo, Msg}
    end.
