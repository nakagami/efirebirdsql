%%% The MIT License (MIT)
%%% Copyright (c) 2016-2021 Hajime Nakagami<nakagami@gmail.com>

-module(efirebirdsql_op).
-define(DEBUG_FORMAT(X,Y), ok).

-export([op_connect/6, op_attach/2, op_detach/1, op_create/3, op_transaction/2,
    op_allocate_statement/1, op_prepare_statement/3, op_free_statement/2,
    op_execute/3, op_execute2/3, op_exec_immediate/2, op_info_sql/2, op_fetch/2,
    op_commit_retaining/1, op_rollback_retaining/1, convert_row/3,
    get_response/1, get_connect_response/1, get_fetch_response/2,
    get_sql_response/2, get_prepare_statement_response/2]).

-include("efirebirdsql.hrl").

-define(CHARSET, "UTF8").
-define(BUFSIZE, 1024).
-define(INFO_SQL_SELECT_DESCRIBE_VARS, [
    4,      %% isc_info_sql_select
    7,      %% isc_info_sql_describe_vars
    9,      %% isc_info_sql_sqlda_seq
    11,     %% isc_info_sql_type
    12,     %% isc_info_sql_sub_type
    13,     %% isc_info_sql_scale
    14,     %% isc_info_sql_length
    15,     %% isc_info_sql_null_ind,
    16,     %% isc_info_sql_field,
    17,     %% isc_info_sql_relation,
    18,     %% isc_info_sql_owner,
    19,     %% isc_info_sql_alias,
    8       %% isc_info_sql_describe_end
]).

pack_cnct_param(K, V) when is_list(V) ->
    lists:flatten([K, length(V), V]);
pack_cnct_param(K, V) when is_binary(V) ->
    pack_cnct_param(K, binary_to_list(V)).

%%% parameters separate per 254 bytes
pack_specific_data_cnct_param(Acc, _, _, []) ->
    lists:flatten(lists:reverse(Acc));
pack_specific_data_cnct_param(Acc, Idx, K, V) ->
    pack_specific_data_cnct_param(
        [pack_cnct_param(K, [Idx | lists:sublist(V, 1, 254)]) | Acc],
        Idx + 1,
        K,
        if length(V) > 254 -> lists:nthtail(254, V); length(V) =< 254 -> [] end).

pack_specific_data_cnct_param(K, V) ->
    pack_specific_data_cnct_param([], 0, K, V).

-spec uid(string(), string(), integer(), string(), boolean()) -> list().
uid(Host, User, SpecificData, AuthPlugin, WireCrypt) ->
    Data = lists:flatten([
        pack_cnct_param(9, User),                   %% CNCT_login
        pack_cnct_param(8, AuthPlugin),                 %% CNCT_plugin_name
        pack_cnct_param(10, "Srp256,Srp"),              %% CNCT_plugin_list
        pack_specific_data_cnct_param(7,
            efirebirdsql_srp:to_hex(SpecificData)), %% CNCT_specific_data
        pack_cnct_param(11,
            [if WireCrypt=:=true -> 1; WireCrypt =/= true -> 0 end, 0, 0, 0]
        ),  %% CNCT_client_crypt
        pack_cnct_param(1, User),                   %% CNCT_user
        pack_cnct_param(4, Host),                       %% CNCT_host
        pack_cnct_param(6, "")                          %% CNCT_user_verification
    ]),
    efirebirdsql_conv:list_to_xdr_bytes(Data).

convert_scale(Scale) ->
    if
    Scale < 0 -> 256 + Scale;
    Scale >= 0 -> Scale
    end.

calc_blr_item(XSqlVar) ->
    case XSqlVar#column.type of
    varying -> [37 | efirebirdsql_conv:byte2(XSqlVar#column.length)] ++ [7, 0];
    text -> [14 | efirebirdsql_conv:byte2(XSqlVar#column.length)] ++ [7, 0];
    long -> [8, convert_scale(XSqlVar#column.scale), 7, 0];
    short -> [7, convert_scale(XSqlVar#column.scale), 7, 0];
    int64 -> [16,  convert_scale(XSqlVar#column.scale), 7, 0];
    int128 -> [26,  convert_scale(XSqlVar#column.scale), 7, 0];
    quad -> [9, convert_scale(XSqlVar#column.scale), 7, 0];
    double -> [27, 7, 0];
    float -> [10, 7, 0];
    d_float -> [11, 7, 0];
    date -> [12, 7, 0];
    time -> [13, 7, 0];
    timestamp -> [35, 7, 0];
    decimal_fixed -> [26, convert_scale(XSqlVar#column.scale), 7, 0];
    decimal64 -> [24, 7, 0];
    decimal128 -> [25, 7, 0];
    time_tz -> [28, 7, 0];
    timestamp_tz -> [29, 7, 0];
    blob -> [9, 0, 7, 0];
    array -> [9, 0, 7, 0];
    boolean -> [23, 7, 0]
    end.

calc_blr_items([], Blr) ->
    Blr;
calc_blr_items(XSqlVars, Blr) ->
    [H | T] = XSqlVars,
    calc_blr_items(T, Blr ++ calc_blr_item(H)).

calc_blr(XSqlVars) ->
    L = length(XSqlVars) * 2,
    lists:flatten([[5, 2, 4, 0],
        efirebirdsql_conv:byte2(L),
        calc_blr_items(XSqlVars, []),
        [255, 76]]).

%%% create op_connect binary
-spec op_connect(string(), string(), integer(), string(), boolean(), list()) -> binary().
op_connect(Host, User, ClientPublic, AuthPlugin, WireCrypt, Database) ->
    ?DEBUG_FORMAT("op_connect~n", []),
    %% PROTOCOL_VERSION,ArchType(Generic)=1,MinAcceptType=0,MaxAcceptType=4,Weight
    Protocols = if AuthPlugin == "Legacy_Auth" -> [
        [  0,   0,   0, 10, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 2],
        [255, 255, 128, 11, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 4],
        [255, 255, 128, 12, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 6]
    ];
        true -> [
        [  0,   0,   0, 10, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 2],
        [255, 255, 128, 11, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 4],
        [255, 255, 128, 12, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 6],
        [255, 255, 128, 13, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 8]
    ]
    end,
    Buf = [
        efirebirdsql_conv:byte4(op_val(op_connect)),
        efirebirdsql_conv:byte4(op_val(op_attach)),
        efirebirdsql_conv:byte4(3),  %% CONNECT_VERSION
        efirebirdsql_conv:byte4(1),  %% arch_generic,
        efirebirdsql_conv:list_to_xdr_string(Database),
        efirebirdsql_conv:byte4(length(Protocols)),
        uid(Host, User, ClientPublic, AuthPlugin, WireCrypt)],
    list_to_binary([Buf, lists:flatten(Protocols)]).

%%% create op_attach binary
op_attach(Conn, Database) ->
    ?DEBUG_FORMAT("op_attach~n", []),
    Username = Conn#conn.user,
    Dpb = if
        Conn#conn.accept_version >= 13 ->
            AuthData = Conn#conn.auth_data,
            lists:flatten([
                1,                              %% isc_dpb_version = 1
                48, length(?CHARSET), ?CHARSET, %% isc_dpb_lc_ctype = 48
                28, length(Username), Username, %% isc_dpb_user_name 28
                84, length(AuthData), AuthData  %% isc_dpb_specific_auth_data = 84
            ]);
        Conn#conn.accept_version < 13 ->
            Password = Conn#conn.password,
            lists:flatten([
                1,                              %% isc_dpb_version = 1
                48, length(?CHARSET), ?CHARSET, %% isc_dpb_lc_ctype = 48
                28, length(Username), Username, %% isc_dpb_user_name 28
                29, length(Password), Password  %% isc_dpb_password = 29
            ])
        end,
    Dpb2 = case Conn#conn.timezone of
    nil ->
        Dpb;
    TimeZoneBin ->
        TimeZone = binary_to_list(TimeZoneBin),
        lists:flatten([Dpb,
            91, length(TimeZone), TimeZone  %% isc_dpb_session_time_zone = 91
        ])
    end,

    list_to_binary(lists:flatten([
        efirebirdsql_conv:byte4(op_val(op_attach)),
        efirebirdsql_conv:byte4(0),
        efirebirdsql_conv:list_to_xdr_string(Database),
        efirebirdsql_conv:list_to_xdr_bytes(Dpb2)])).

op_detach(DbHandle) ->
    ?DEBUG_FORMAT("op_detatch~n", []),
    list_to_binary([
        efirebirdsql_conv:byte4(op_val(op_detach)),
        efirebirdsql_conv:byte4(DbHandle)]).

%%% create op_connect binary
op_create(Conn, Database, PageSize) ->
    ?DEBUG_FORMAT("op_create~n", []),
    Username = Conn#conn.user,
    Dpb = if
        Conn#conn.accept_version >= 13 ->
            AuthData = Conn#conn.auth_data,
            lists:flatten([
                1,
                68, length(?CHARSET), ?CHARSET, %% isc_dpb_set_db_charset = 68
                48, length(?CHARSET), ?CHARSET, %% isc_dpb_lc_ctype = 48
                28, length(Username), Username, %% isc_dpb_user_name 28
                63, 4, efirebirdsql_conv:byte4(3, little),  %% isc_dpb_sql_dialect = 63
                24, 4, efirebirdsql_conv:byte4(1, little),  %% isc_dpb_force_write = 24
                54, 4, efirebirdsql_conv:byte4(1, little),  %% isc_dpb_overwrite = 54
                84, length(AuthData), AuthData, %% isc_dpb_specific_auth_data = 84
                4, 4, efirebirdsql_conv:byte4(PageSize, little)   %% isc_dpb_page_size = 4
            ]);
        Conn#conn.accept_version < 13 ->
            Password = Conn#conn.password,
            lists:flatten([
                1,
                68, length(?CHARSET), ?CHARSET,   %% isc_dpb_set_db_charset = 68
                48, length(?CHARSET), ?CHARSET,   %% isc_dpb_lc_ctype = 48
                28, length(Username), Username, %% isc_dpb_user_name 28
                29, length(Password), Password, %% isc_dpb_password = 29
                63, 4, efirebirdsql_conv:byte4(3, little),        %% isc_dpb_sql_dialect = 63
                24, 4, efirebirdsql_conv:byte4(1, little),        %% isc_dpb_force_write = 24
                54, 4, efirebirdsql_conv:byte4(1, little),        %% isc_dpb_overwrite = 54
                4, 4, efirebirdsql_conv:byte4(PageSize, little)   %% isc_dpb_page_size = 4
            ])
        end,
    Dpb2 = case Conn#conn.timezone of
    nil ->
        Dpb;
    TimeZoneBin ->
        TimeZone = binary_to_list(TimeZoneBin),
        lists:flatten([Dpb,
            91, length(TimeZone), TimeZone  %% isc_dpb_session_time_zone = 91
        ])
    end,
    list_to_binary(lists:flatten([
        efirebirdsql_conv:byte4(op_val(op_create)),
        efirebirdsql_conv:byte4(0),
        efirebirdsql_conv:list_to_xdr_string(Database),
        efirebirdsql_conv:list_to_xdr_bytes(Dpb2)])).

%%% begin transaction
op_transaction(DbHandle, Tpb) ->
    ?DEBUG_FORMAT("op_transaction~n", []),
    list_to_binary([
        efirebirdsql_conv:byte4(op_val(op_transaction)),
        efirebirdsql_conv:byte4(DbHandle),
        efirebirdsql_conv:list_to_xdr_bytes(Tpb)]).

%%% allocate statement
op_allocate_statement(DbHandle) ->
    ?DEBUG_FORMAT("op_allocate_statement~n", []),
    list_to_binary([
        efirebirdsql_conv:byte4(op_val(op_allocate_statement)),
        efirebirdsql_conv:byte4(DbHandle)]).

%%% prepare statement
op_prepare_statement(TransHandle, StmtHandle, Sql) ->
    ?DEBUG_FORMAT("op_prepare_statement(~p,~p,~p)~n", [TransHandle, StmtHandle, Sql]),
    DescItems = [21 | ?INFO_SQL_SELECT_DESCRIBE_VARS], %% isc_info_sql_stmt_type
    list_to_binary([
        efirebirdsql_conv:byte4(op_val(op_prepare_statement)),
        efirebirdsql_conv:byte4(TransHandle),
        efirebirdsql_conv:byte4(StmtHandle),
        efirebirdsql_conv:byte4(3),
        efirebirdsql_conv:list_to_xdr_string(binary_to_list(Sql)),
        efirebirdsql_conv:list_to_xdr_bytes(DescItems),
        efirebirdsql_conv:byte4(?BUFSIZE)]).

%%% free statement
op_free_statement(StmtHandle, close) ->
    ?DEBUG_FORMAT("op_free_statement(close)~n", []),
    %% DSQL_close = 1
    %% DSQL_drop = 2
    list_to_binary([
        efirebirdsql_conv:byte4(op_val(op_free_statement)),
        efirebirdsql_conv:byte4(StmtHandle),
        efirebirdsql_conv:byte4(1)]);
op_free_statement(StmtHandle, drop) ->
    ?DEBUG_FORMAT("op_free_statement(drop)~n", []),
    %% DSQL_close = 1
    %% DSQL_drop = 2
    list_to_binary([
        efirebirdsql_conv:byte4(op_val(op_free_statement)),
        efirebirdsql_conv:byte4(StmtHandle),
        efirebirdsql_conv:byte4(2)]).

op_execute(Conn, Stmt, Params) ->
    ?DEBUG_FORMAT("op_execute~n", []),
    TransHandle = Conn#conn.trans_handle,
    StmtHandle = Stmt#stmt.stmt_handle,

    if
    length(Params) == 0 ->
        list_to_binary([
            efirebirdsql_conv:byte4(op_val(op_execute)),
            efirebirdsql_conv:byte4(StmtHandle),
            efirebirdsql_conv:byte4(TransHandle),
            efirebirdsql_conv:list_to_xdr_bytes([]),
            efirebirdsql_conv:byte4(0),
            efirebirdsql_conv:byte4(0)]);
    length(Params) > 0 ->
        {Blr, Value} = efirebirdsql_conv:params_to_blr(
            Conn#conn.accept_version, Conn#conn.timezone_id_by_name, Params),
        list_to_binary([
            efirebirdsql_conv:byte4(op_val(op_execute)),
            efirebirdsql_conv:byte4(StmtHandle),
            efirebirdsql_conv:byte4(TransHandle),
            efirebirdsql_conv:list_to_xdr_bytes(Blr),
            efirebirdsql_conv:byte4(0),
            efirebirdsql_conv:byte4(1),
            Value])
    end.

op_execute2(Conn, Stmt, Params) ->
    ?DEBUG_FORMAT("op_execute2~n", []),
    TransHandle = Conn#conn.trans_handle,
    StmtHandle = Stmt#stmt.stmt_handle,
    XSqlVars = Stmt#stmt.xsqlvars,

    OutputBlr = efirebirdsql_conv:list_to_xdr_bytes(calc_blr(XSqlVars)),
    if
    length(Params) == 0 ->
        list_to_binary([
            efirebirdsql_conv:byte4(op_val(op_execute2)),
            efirebirdsql_conv:byte4(StmtHandle),
            efirebirdsql_conv:byte4(TransHandle),
            efirebirdsql_conv:list_to_xdr_bytes([]),
            efirebirdsql_conv:byte4(0),
            efirebirdsql_conv:byte4(0),
            OutputBlr,
            efirebirdsql_conv:byte4(0)]);
    length(Params) > 0 ->
        {Blr, Value} = efirebirdsql_conv:params_to_blr(
            Conn#conn.accept_version, Conn#conn.timezone_id_by_name, Params),
        list_to_binary([
            efirebirdsql_conv:byte4(op_val(op_execute2)),
            efirebirdsql_conv:byte4(StmtHandle),
            efirebirdsql_conv:byte4(TransHandle),
            efirebirdsql_conv:list_to_xdr_bytes(Blr),
            efirebirdsql_conv:byte4(0),
            efirebirdsql_conv:byte4(1),
            Value,
            OutputBlr,
            efirebirdsql_conv:byte4(0)])
    end.

op_exec_immediate(Conn, Sql) ->
    ?DEBUG_FORMAT("op_exec_immediate~n", []),
    DbHandle = Conn#conn.db_handle,
    TransHandle = Conn#conn.trans_handle,

    list_to_binary([
        efirebirdsql_conv:byte4(op_val(op_exec_immediate)),
        efirebirdsql_conv:byte4(TransHandle),
        efirebirdsql_conv:byte4(DbHandle),
        efirebirdsql_conv:byte4(3),     % dialect = 3
        efirebirdsql_conv:list_to_xdr_string(binary_to_list(Sql)),
        efirebirdsql_conv:list_to_xdr_bytes([]),
        efirebirdsql_conv:byte4(?BUFSIZE)]).

op_info_sql(StmtHandle, V) ->
    ?DEBUG_FORMAT("op_info_sql(~p,~p)~n", [StmtHandle, V]),
    list_to_binary([
        efirebirdsql_conv:byte4(op_val(op_info_sql)),
        efirebirdsql_conv:byte4(StmtHandle),
        efirebirdsql_conv:byte4(0),
        efirebirdsql_conv:list_to_xdr_bytes(V),
        efirebirdsql_conv:byte4(?BUFSIZE)]).

op_fetch(StmtHandle, XSqlVars) ->
    ?DEBUG_FORMAT("op_fetch~n", []),
    list_to_binary([
        efirebirdsql_conv:byte4(op_val(op_fetch)),
        efirebirdsql_conv:byte4(StmtHandle),
        efirebirdsql_conv:list_to_xdr_bytes(calc_blr(XSqlVars)),
        efirebirdsql_conv:byte4(0),
        efirebirdsql_conv:byte4(400)]).

%%% commit
op_commit_retaining(TransHandle) ->
    ?DEBUG_FORMAT("op_commit_retaining~n", []),
    list_to_binary([
        efirebirdsql_conv:byte4(op_val(op_commit_retaining)),
        efirebirdsql_conv:byte4(TransHandle)]).

%%% rollback
op_rollback_retaining(TransHandle) ->
    ?DEBUG_FORMAT("op_rollback_retaining~n", []),
    list_to_binary([
        efirebirdsql_conv:byte4(op_val(op_rollback_retaining)),
        efirebirdsql_conv:byte4(TransHandle)]).


%%% blob
op_open_blob(BlobId, TransHandle) ->
    ?DEBUG_FORMAT("op_open_blob~n", []),
    H = list_to_binary([
        efirebirdsql_conv:byte4(op_val(op_open_blob)),
        efirebirdsql_conv:byte4(TransHandle)]),
    <<H/binary, BlobId/binary>>.

op_get_segment(BlobHandle) ->
    ?DEBUG_FORMAT("op_get_segment~n", []),
    list_to_binary([
        efirebirdsql_conv:byte4(op_val(op_get_segment)),
        efirebirdsql_conv:byte4(BlobHandle),
        efirebirdsql_conv:byte4(?BUFSIZE),
        efirebirdsql_conv:byte4(0)]).

op_close_blob(BlobHandle) ->
    ?DEBUG_FORMAT("op_close_blob~n", []),
    list_to_binary([
        efirebirdsql_conv:byte4(op_val(op_close_blob)),
        efirebirdsql_conv:byte4(BlobHandle)]).

op_cont_auth(AuthData, PluginName, PluginNameList, Keys) ->
    ?DEBUG_FORMAT("op_cont_auth~n", []),
    list_to_binary([
        efirebirdsql_conv:byte4(op_val(op_cont_auth)),
        efirebirdsql_conv:list_to_xdr_string(AuthData),
        efirebirdsql_conv:list_to_xdr_string(PluginName),
        efirebirdsql_conv:list_to_xdr_string(PluginNameList),
        efirebirdsql_conv:list_to_xdr_string(Keys)]).

op_crypt() ->
    ?DEBUG_FORMAT("op_crypt~n", []),
    list_to_binary([
        efirebirdsql_conv:byte4(op_val(op_crypt)),
        efirebirdsql_conv:list_to_xdr_string("Arc4"),
        efirebirdsql_conv:list_to_xdr_string("Symmetric")]).

%%% parse status vector
parse_status_vector_integer(Conn) ->
    {ok, <<NumArg:32>>} = efirebirdsql_socket:recv(Conn, 4),
    NumArg.

parse_status_vector_string(Conn) ->
    Len = parse_status_vector_integer(Conn),
    {ok, Bin} = efirebirdsql_socket:recv_align(Conn, Len),
    binary_to_list(Bin).

parse_status_vector_args(Conn, ErrNo, Template, Args) ->
    {ok, <<IscArg:32>>} = efirebirdsql_socket:recv(Conn, 4),
    case IscArg of
    0 ->    %% isc_arg_end
        {Conn, ErrNo, Template, Args};
    1 ->    %% isc_arg_gds
        N = parse_status_vector_integer(Conn),
        Msg = efirebirdsql_errmsgs:get_error_msg(N),
        parse_status_vector_args(Conn, N, [Msg | Template], Args);
    2 ->    %% isc_arg_string
        V = parse_status_vector_string(Conn),
        parse_status_vector_args(Conn, ErrNo, Template, [V | Args]);
    4 ->    %% isc_arg_number
        V = parse_status_vector_integer(Conn),
        parse_status_vector_args(Conn, ErrNo, Template, [integer_to_list(V) | Args]);
    5 ->    %% isc_arg_interpreted
        V = parse_status_vector_string(Conn),
        parse_status_vector_args(Conn, ErrNo, [V | Template], Args);
    19 ->   %% isc_arg_sql_state
        _V = parse_status_vector_string(Conn),
        parse_status_vector_args(Conn, ErrNo, Template, Args)
    end.

get_error_message(Conn) ->
    {_Conn, ErrNo, Msg, Arg} = parse_status_vector_args(Conn, 0, [], []),
    {ErrNo, iolist_to_binary(io_lib:format(lists:flatten(lists:reverse(Msg)), lists:reverse(Arg)))}.

%% recieve and parse response
-spec get_response(conn()) ->
    {op_response, integer(), binary()} |
    {op_fetch_response, integer(), integer()} |
    {op_sql_response, integer()} |
    {error, integer(), binary()}.
get_response(Conn) ->
    ?DEBUG_FORMAT("get_response()~n", []),
    {ok, <<OpCode:32>>} = efirebirdsql_socket:recv(Conn, 4),
    Op = op_name(OpCode),
    case Op of 
    op_response ->
        {ok, <<Handle:32, _ObjectID:64, Len:32>>} = efirebirdsql_socket:recv(Conn, 16),
        Buf = if
            Len =/= 0 ->
                {ok, RecvBuf} = efirebirdsql_socket:recv_align(Conn, Len),
                RecvBuf;
            true ->
                <<>>
            end,
        {ErrNo, Msg} = get_error_message(Conn),
        case Msg of
        <<>> -> {Op, Handle, Buf};
        _ -> {error, ErrNo, Msg}
        end;
    op_fetch_response ->
        {ok, <<Status:32, Count:32>>} = efirebirdsql_socket:recv(Conn, 8),
        {Op, Status, Count};
    op_sql_response ->
        {ok, <<Count:32>>} = efirebirdsql_socket:recv(Conn, 4),
        {Op, Count};
    op_dummy ->
        get_response(Conn);
    _ ->
        {error, 0, <<"Unknown response">>}
    end.

wire_crypt(Conn, SessionKey) ->
    efirebirdsql_socket:send(Conn, op_crypt()),
    C2 = Conn#conn{
        read_state=crypto:crypto_init(rc4, SessionKey, false),
        write_state=crypto:crypto_init(rc4, SessionKey, true)
    },
    {op_response,  _, _} = get_response(C2),
    C2.

get_auth_data([], _Conn) ->
    %% TODO: op_cont_auth
    <<>>;
get_auth_data(Data, _Conn) ->
    Data.

%% recieve and parse connect() response
get_connect_response(op_accept, Conn) ->
    {ok, <<_AcceptVersionMasks:24, AcceptVersion:8,
            _AcceptArchtecture:32, _AcceptType:32>>} = efirebirdsql_socket:recv(Conn, 12),
    {ok, Conn#conn{accept_version=AcceptVersion}};
get_connect_response(Op, Conn) ->
    {ok, <<_AcceptVersionMasks:24, AcceptVersion:8,
            _AcceptArchtecture:32, _AcceptType:32>>} = efirebirdsql_socket:recv(Conn, 12),
    {ok, <<Len1:32>>} = efirebirdsql_socket:recv(Conn, 4),
    {ok, Data} = efirebirdsql_socket:recv_align(Conn, Len1),
    {ok, <<Len2:32>>} = efirebirdsql_socket:recv(Conn, 4),
    {ok, PluginName} = efirebirdsql_socket:recv_align(Conn, Len2),
    {ok, <<IsAuthenticated:32>>} = efirebirdsql_socket:recv(Conn, 4),
    {ok, <<_:32>>} = efirebirdsql_socket:recv(Conn, 4),
    if IsAuthenticated == 0 ->
        ServerAuthData = get_auth_data(Data, Conn),
        case binary_to_list(PluginName) of
        "Srp" ->
            <<SaltLen:16/little-unsigned, Salt:SaltLen/binary, _KeyLen:16, Bin/binary>> = ServerAuthData,
            ServerPublic = binary_to_integer(Bin, 16),
            {AuthData, SessionKey} = efirebirdsql_srp:client_proof(
                Conn#conn.user, Conn#conn.password, Salt,
                Conn#conn.client_public, ServerPublic, Conn#conn.client_private, sha);
        "Srp256" ->
            <<SaltLen:16/little-unsigned, Salt:SaltLen/binary, _KeyLen:16, Bin/binary>> = ServerAuthData,
            ServerPublic = binary_to_integer(Bin, 16),
            {AuthData, SessionKey} = efirebirdsql_srp:client_proof(
                Conn#conn.user, Conn#conn.password, Salt,
                Conn#conn.client_public, ServerPublic, Conn#conn.client_private, sha256);
        _ ->
            AuthData = '',
            SessionKey = ''
        end;
    true ->
        AuthData = '',
        SessionKey = ''
    end,
    C2 = Conn#conn{accept_version=AcceptVersion, auth_data=efirebirdsql_srp:to_hex(AuthData)},
    case Op of
    op_cond_accept ->
        efirebirdsql_socket:send(C2,
            op_cont_auth(C2#conn.auth_data, C2#conn.auth_plugin, C2#conn.auth_plugin, "")),
        {op_response, _, _} = get_response(C2);
    _ -> nil
    end,
    NewConn = case C2#conn.wire_crypt of
    true -> wire_crypt(C2, SessionKey);
    false -> C2
    end,
    {ok, NewConn}.

-spec get_connect_response(conn()) -> {ok, conn()} | {error, binary(), conn()}.
get_connect_response(Conn) ->
    ?DEBUG_FORMAT("get_connect_response()~n", []),
    {ok, <<OpCode:32>>} = efirebirdsql_socket:recv(Conn, 4),
    Op = op_name(OpCode),
    if
        (Op == op_accept) or (Op == op_cond_accept) or (Op == op_accept_data) ->
            get_connect_response(Op, Conn);
        Op == op_dummy ->
            get_connect_response(Conn);
        Op == op_response ->
            {ok, <<_Handle:32, _ObjectID:64, _Len:32>>} = efirebirdsql_socket:recv(Conn, 16),
            {ErrNo, Msg} = get_error_message(Conn),
            {error, ErrNo, Msg, Conn};
        Op == op_reject ->
            {error, <<"Connect rejected">>, Conn};
        true ->
            {error, <<"Unknow connect error">>, Conn}
    end.

%% parse select items.
more_select_describe_vars(Conn, Stmt, Start) ->
    %% isc_info_sql_sqlda_start + INFO_SQL_SELECT_DESCRIBE_VARS
    V = lists:flatten([20, 2, Start rem 256, Start div 256, ?INFO_SQL_SELECT_DESCRIBE_VARS]),
    efirebirdsql_socket:send(Conn, op_info_sql(Stmt#stmt.stmt_handle, V)),
    {op_response, _, Buf} = get_response(Conn),
    <<_:8/binary, DescVars/binary>> = Buf,
    DescVars.

parse_select_item_elem_binary(DescVars) ->
    <<L:16/little, V:L/binary, Rest/binary>> = DescVars,
    {V, Rest}.

parse_select_item_elem_int(DescVars) ->
    {V, Rest} = parse_select_item_elem_binary(DescVars),
    L = size(V) * 8,
    <<Num:L/signed-little>> = V,
    {Num, Rest}.

parse_select_column(Conn, Stmt, Column, DescVars) ->
    %% Parse DescVars and return items info and rest DescVars
    <<IscInfoNum:8, Rest/binary>> = DescVars,
    case isc_info_sql_name(IscInfoNum) of
    isc_info_sql_sqlda_seq ->
        {Num, Rest2} = parse_select_item_elem_int(Rest),
        parse_select_column(Conn, Stmt, Column#column{seq=Num}, Rest2);
    isc_info_sql_type ->
        {Num, Rest2} = parse_select_item_elem_int(Rest),
        parse_select_column(Conn, Stmt, Column#column{type=sql_type(Num)}, Rest2);
    isc_info_sql_sub_type ->
        {_Num, Rest2} = parse_select_item_elem_int(Rest),
        parse_select_column(Conn, Stmt, Column, Rest2);
    isc_info_sql_scale ->
        {Num, Rest2} = parse_select_item_elem_int(Rest),
        parse_select_column(Conn, Stmt, Column#column{scale=Num}, Rest2);
    isc_info_sql_length ->
        {Num, Rest2} = parse_select_item_elem_int(Rest),
        parse_select_column(Conn, Stmt, Column#column{length=Num}, Rest2);
    isc_info_sql_null_ind ->
        {Num, Rest2} = parse_select_item_elem_int(Rest),
        NullInd = if Num =/= 0 -> true; Num =:= 0 -> false end,
        parse_select_column(Conn, Stmt, Column#column{null_ind=NullInd}, Rest2);
    isc_info_sql_field ->
        {_S, Rest2} = parse_select_item_elem_binary(Rest),
        parse_select_column(Conn, Stmt, Column, Rest2);
    isc_info_sql_relation ->
        {_S, Rest2} = parse_select_item_elem_binary(Rest),
        parse_select_column(Conn, Stmt, Column, Rest2);
    isc_info_sql_owner ->
        {_S, Rest2} = parse_select_item_elem_binary(Rest),
        parse_select_column(Conn, Stmt, Column, Rest2);
    isc_info_sql_alias ->
        {S, Rest2} = parse_select_item_elem_binary(Rest),
        parse_select_column(Conn, Stmt, Column#column{name=S}, Rest2);
    isc_info_truncated ->
        Rest2 = more_select_describe_vars(Conn, Stmt, Column#column.seq),
        parse_select_column(Conn, Stmt, Column, Rest2);
    isc_info_sql_describe_end ->
        {Conn, Column, Rest};
    isc_info_end ->
        {Conn, no_more_column}
    end.

parse_select_columns(Conn, Stmt, XSqlVars, DescVars) ->
    case parse_select_column(Conn, Stmt, #column{}, DescVars) of
    {C2, XSqlVar, Rest} ->
        parse_select_columns(C2, Stmt, [XSqlVar | XSqlVars], Rest);
    {C2, no_more_column} ->
        {C2, lists:reverse(XSqlVars)}
    end.

-spec get_prepare_statement_response(conn(), stmt()) -> {ok, stmt()} | {error, integer(), binary()}.
get_prepare_statement_response(Conn, Stmt) ->
    case get_response(Conn) of
    {op_response, _, Buf} ->
        << _21:8, _Len:16, StmtType:32/little, Rest/binary>> = Buf,
        StmtName = isc_info_sql_stmt_name(StmtType),
        Stmt2 = Stmt#stmt{stmt_type=StmtName},
        {_, XSqlVars} = case StmtName of
            isc_info_sql_stmt_select ->
                << _Skip:8/binary, DescVars/binary >> = Rest,
                parse_select_columns(Conn, Stmt2, [], DescVars);
            isc_info_sql_stmt_exec_procedure ->
                << _Skip:8/binary, DescVars/binary >> = Rest,
                parse_select_columns(Conn, Stmt2, [], DescVars);
            _ -> {Conn, []}
            end,
        {ok, Stmt2#stmt{xsqlvars=XSqlVars, rows=[]}};
    {error, ErrNo, Msg} -> {error, ErrNo, Msg}
    end.

get_blob_segment_list(<<>>, SegmentList) ->
    lists:reverse(SegmentList);
get_blob_segment_list(Buf, SegmentList) ->
    <<L:16/little, V:L/binary, Rest/binary>> = Buf,
    get_blob_segment_list(Rest, [V| SegmentList]).

get_blob_segment(Conn, BlobHandle, SegmentList) ->
    efirebirdsql_socket:send(Conn, op_get_segment(BlobHandle)),
    {op_response, F, Buf} = get_response(Conn),
    NewList = lists:flatten([SegmentList, get_blob_segment_list(Buf, [])]),
    case F of
    2 -> NewList;
    _ -> get_blob_segment(Conn, BlobHandle, NewList)
    end.

get_blob_data(Conn, BlobId) ->
    efirebirdsql_socket:send(Conn, op_open_blob(BlobId, Conn#conn.trans_handle)),
    {op_response, BlobHandle, _} = get_response(Conn),
    SegmentList = get_blob_segment(Conn, BlobHandle, []),
    efirebirdsql_socket:send(Conn, op_close_blob(BlobHandle)),
    {op_response, 0, _} = get_response(Conn),
    R = list_to_binary(SegmentList),
    {ok, R}.

convert_raw_value(_Conn, _XSqlVar, nil) ->
    nil;
convert_raw_value(Conn, XSqlVar, RawValue) ->
    ?DEBUG_FORMAT("convert_raw_value() start~n", []),
    CookedValue = case XSqlVar#column.type of
        long ->
            efirebirdsql_conv:parse_number(RawValue, XSqlVar#column.scale);
        short ->
            efirebirdsql_conv:parse_number(RawValue, XSqlVar#column.scale);
        int64 ->
            efirebirdsql_conv:parse_number(RawValue, XSqlVar#column.scale);
        int128 ->
            efirebirdsql_conv:parse_number(RawValue, XSqlVar#column.scale);
        quad ->
            efirebirdsql_conv:parse_number(RawValue, XSqlVar#column.scale);
        double ->
            L = size(RawValue) * 8, <<V:L/float>> = RawValue, V;
        float ->
            L = size(RawValue) * 8, <<V:L/float>> = RawValue, V;
        date ->
            efirebirdsql_conv:parse_date(RawValue);
        time ->
            efirebirdsql_conv:parse_time(RawValue);
        timestamp ->
            efirebirdsql_conv:parse_timestamp(RawValue);
        time_tz ->
            efirebirdsql_conv:parse_time_tz(RawValue, Conn#conn.timezone_name_by_id);
        timestamp_tz ->
            efirebirdsql_conv:parse_timestamp_tz(RawValue, Conn#conn.timezone_name_by_id);
        decimal_fixed ->
            efirebirdsql_decfloat:decimal_fixed_to_decimal(RawValue, XSqlVar#column.scale);
        decimal64 ->
            efirebirdsql_decfloat:decimal64_to_decimal(RawValue);
        decimal128 ->
            efirebirdsql_decfloat:decimal128_to_decimal(RawValue);
        blob ->
            {ok, B} = get_blob_data(Conn, RawValue),
            B;
        boolean ->
            if RawValue =/= <<0,0,0,0>> -> true; true -> false end;
        text ->
            list_to_binary(lists:reverse(lists:dropwhile(fun(32) -> true; (_) -> false end, lists:reverse(binary_to_list(RawValue)))));
        _ ->
            RawValue
        end,
    ?DEBUG_FORMAT("convert_raw_value() end ~p~n", [CookedValue]),
    CookedValue.

convert_row(_Conn, [], [], Converted) ->
    ?DEBUG_FORMAT("convert_row()~n", []),
    lists:reverse(Converted);
convert_row(Conn, XSqlVars, Row, Converted) ->
    [X | XRest] = XSqlVars,
    [R | RRest] = Row,
    V = convert_raw_value(Conn, X, R),
    convert_row(Conn, XRest, RRest, [{X#column.name, V} | Converted]).

convert_row(Conn, XSqlVars, Row) ->
    convert_row(Conn, XSqlVars, Row, []).

get_raw_value(Conn, XSqlVar) ->
    if
        XSqlVar#column.type =:= varying ->
            {ok, <<L:32>>} = efirebirdsql_socket:recv(Conn, 4);
        true ->
            L = case XSqlVar#column.type of
                text -> XSqlVar#column.length;
                long -> 4;
                short -> 4;
                int64 -> 8;
                int128 -> 16;
                quad -> 8;
                double -> 8;
                float -> 4;
                date -> 4;
                time -> 4;
                timestamp -> 8;
                time_tz -> 6;
                timestamp_tz -> 10;
                decimal_fixed -> 16;
                decimal64 -> 8;
                decimal128 ->  16;
                blob -> 8;
                array -> 8;
                boolean -> 1
                end
    end,
    if
        L =:= 0 ->
            "";
        L > 0 ->
            {ok, V} = efirebirdsql_socket:recv_align(Conn, L),
            V
    end.

get_raw_or_null_value(Conn, XSqlVar) ->
    V = get_raw_value(Conn, XSqlVar),
    {ok, NullFlag} = efirebirdsql_socket:recv(Conn, 4),
    case NullFlag of
    <<0,0,0,0>> -> {V, Conn};
    _ -> {nil, Conn}
    end.

get_row(Conn, [], Columns, _NullBitmap, _Idx) ->
    {lists:reverse(Columns), Conn};
get_row(Conn, XSqlVars, Columns, NullBitmap, Idx) ->
    [X | RestVars] = XSqlVars,
    if NullBitmap band (1 bsl Idx) =/= 0 ->
            V = nil;
        true ->
            V = get_raw_value(Conn, X)
        end,
    get_row(Conn, RestVars, [V | Columns], NullBitmap, Idx + 1).

get_row(Conn, [], Columns) ->
    {lists:reverse(Columns), Conn};
get_row(Conn, XSqlVars, Columns) ->
    [X | RestVars] = XSqlVars,
    {V, C2} = get_raw_or_null_value(Conn, X),
    get_row(C2, RestVars, [V | Columns]).

get_fetch_response(_Conn, _Stmt, Status, 0, _XSqlVars, Results) ->
    %% {list_of_response, more_data}
    {lists:reverse(Results), if Status =/= 100 -> true; Status =:= 100 -> false end};
get_fetch_response(Conn, Stmt, _Status, _Count, XSqlVars, Results) ->
    {Row, S3} = if
        Conn#conn.accept_version >= 13 ->
            NullBitmap = efirebirdsql_socket:recv_null_bitmap(Conn, length(Stmt#stmt.xsqlvars)),
            get_row(Conn, XSqlVars, [], NullBitmap, 0);
        Conn#conn.accept_version < 13 ->
            get_row(Conn, XSqlVars, [])
        end,
    NewResults = [Row | Results],
    {ok, <<_:32, NewStatus:32, NewCount:32>>} = efirebirdsql_socket:recv(S3, 12),
    get_fetch_response(Conn, Stmt, NewStatus, NewCount, XSqlVars, NewResults).

-spec get_fetch_response(conn(), stmt()) -> {ok, list(), boolean()}.
get_fetch_response(Conn, Stmt) ->
    {op_fetch_response, Status, Count} = get_response(Conn),
    {Results, MoreData} = get_fetch_response(Conn, Stmt, Status, Count, Stmt#stmt.xsqlvars, []),
    {ok, Results, MoreData}.

get_sql_response(Conn, Stmt) ->
    {op_sql_response, Count} = get_response(Conn),
    case Count of
    0 ->
        {[], conn};
    _ ->
        if
            Conn#conn.accept_version >= 13 ->
                NullBitmap = efirebirdsql_socket:recv_null_bitmap(Conn, length(Stmt#stmt.xsqlvars)),
                get_row(Conn, Stmt#stmt.xsqlvars, [], NullBitmap, 0);
            Conn#conn.accept_version < 13 ->
                get_row(Conn, Stmt#stmt.xsqlvars, [])
        end
    end.

op_name(1) -> op_connect;
op_name(2) -> op_exit;
op_name(3) -> op_accept;
op_name(4) -> op_reject;
op_name(5) -> op_protocol;
op_name(6) -> op_disconnect;
op_name(9) -> op_response;
op_name(19) -> op_attach;
op_name(20) -> op_create;
op_name(21) -> op_detach;
op_name(29) -> op_transaction;
op_name(30) -> op_commit;
op_name(31) -> op_rollback;
op_name(35) -> op_open_blob;
op_name(36) -> op_get_segment;
op_name(37) -> op_put_segment;
op_name(39) -> op_close_blob;
op_name(40) -> op_info_database;
op_name(42) -> op_info_transaction;
op_name(44) -> op_batch_segments;
op_name(48) -> op_que_events;
op_name(49) -> op_cancel_events;
op_name(50) -> op_commit_retaining;
op_name(52) -> op_event;
op_name(53) -> op_connect_request;
op_name(57) -> op_create_blob2;
op_name(62) -> op_allocate_statement;
op_name(63) -> op_execute;
op_name(64) -> op_exec_immediate;
op_name(65) -> op_fetch;
op_name(66) -> op_fetch_response;
op_name(67) -> op_free_statement;
op_name(68) -> op_prepare_statement;
op_name(70) -> op_info_sql;
op_name(71) -> op_dummy;
op_name(76) -> op_execute2;
op_name(78) -> op_sql_response;
op_name(81) -> op_drop_database;
op_name(82) -> op_service_attach;
op_name(83) -> op_service_detach;
op_name(84) -> op_service_info;
op_name(85) -> op_service_start;
op_name(86) -> op_rollback_retaining;
%% FB3
op_name(87) -> op_update_account_info;
op_name(88) -> op_authenticate_user;
op_name(89) -> op_partial;
op_name(90) -> op_trusted_auth;
op_name(91) -> op_cancel;
op_name(92) -> op_cont_auth;
op_name(93) -> op_ping;
op_name(94) -> op_accept_data;
op_name(95) -> op_abort_aux_connection;
op_name(96) -> op_crypt;
op_name(97) -> op_crypt_key_callback;
op_name(98) -> op_cond_accept.

-spec op_val(atom()) -> integer().
op_val(op_connect) -> 1;
op_val(op_exit) -> 2;
op_val(op_accept) -> 3;
op_val(op_reject) -> 4;
op_val(op_protocol) -> 5;
op_val(op_disconnect) -> 6;
op_val(op_response) -> 9;
op_val(op_attach) -> 19;
op_val(op_create) -> 20;
op_val(op_detach) -> 21;
op_val(op_transaction) -> 29;
op_val(op_commit) -> 30;
op_val(op_rollback) -> 31;
op_val(op_open_blob) -> 35;
op_val(op_get_segment) -> 36;
op_val(op_put_segment) -> 37;
op_val(op_close_blob) -> 39;
op_val(op_info_database) -> 40;
op_val(op_info_transaction) -> 42;
op_val(op_batch_segments) -> 44;
op_val(op_que_events) -> 48;
op_val(op_cancel_events) -> 49;
op_val(op_commit_retaining) -> 50;
op_val(op_event) -> 52;
op_val(op_connect_request) -> 53;
op_val(op_create_blob2) -> 57;
op_val(op_allocate_statement) -> 62;
op_val(op_execute) -> 63;
op_val(op_exec_immediate) -> 64;
op_val(op_fetch) -> 65;
op_val(op_fetch_response) -> 66;
op_val(op_free_statement) -> 67;
op_val(op_prepare_statement) -> 68;
op_val(op_info_sql) -> 70;
op_val(op_dummy) -> 71;
op_val(op_execute2) -> 76;
op_val(op_sql_response) -> 78;
op_val(op_drop_database) -> 81;
op_val(op_service_attach) -> 82;
op_val(op_service_detach) -> 83;
op_val(op_service_info) -> 84;
op_val(op_service_start) -> 85;
op_val(op_rollback_retaining) -> 86;
%% FB3
op_val(op_update_account_info) -> 87;
op_val(op_authenticate_user) -> 88;
op_val(op_partial) -> 89;
op_val(op_trusted_auth) -> 90;
op_val(op_cancel) -> 91;
op_val(op_cont_auth) -> 92;
op_val(op_ping) -> 93;
op_val(op_accept_data) -> 94;
op_val(op_abort_aux_connection) -> 95;
op_val(op_crypt) -> 96;
op_val(op_crypt_key_callback) -> 97;
op_val(op_cond_accept) -> 98.

sql_type(X) when X rem 2 == 1 -> sql_type(X band 16#FFFE);
sql_type(452) -> text;
sql_type(448) -> varying;
sql_type(500) -> short;
sql_type(496) -> long;
sql_type(482) -> float;
sql_type(480) -> double;
sql_type(530) -> d_float;
sql_type(510) -> timestamp;
sql_type(520) -> blob;
sql_type(540) -> array;
sql_type(550) -> quad;
sql_type(560) -> time;
sql_type(570) -> date;
sql_type(580) -> int64;
sql_type(32752) -> int128;
sql_type(32754) -> timestamp_tz;
sql_type(32756) -> time_tz;
sql_type(32758) -> decimal_fixed;   %% OBSOLATED
sql_type(32760) -> decimal64;
sql_type(32762) -> decimal128;
sql_type(32764) -> boolean;
sql_type(32766) -> nil.

isc_info_sql_name(Num) ->
    lists:nth(Num, [
    isc_info_end, isc_info_truncated, isc_info_error, isc_info_sql_select,
    isc_info_sql_bind, isc_info_sql_num_variables, isc_info_sql_describe_vars,
    isc_info_sql_describe_end, isc_info_sql_sqlda_seq, isc_info_sql_message_seq,
    isc_info_sql_type, isc_info_sql_sub_type, isc_info_sql_scale,
    isc_info_sql_length, isc_info_sql_null_ind, isc_info_sql_field,
    isc_info_sql_relation, isc_info_sql_owner, isc_info_sql_alias,
    isc_info_sql_sqlda_start, isc_info_sql_stmt_type, isc_info_sql_get_plan,
    isc_info_sql_records, isc_info_sql_batch_fetch,
    isc_info_sql_relation_alias]).

isc_info_sql_stmt_name(Num) ->
    lists:nth(Num, [
    isc_info_sql_stmt_select, isc_info_sql_stmt_insert,
    isc_info_sql_stmt_update, isc_info_sql_stmt_delete, isc_info_sql_stmt_ddl,
    isc_info_sql_stmt_get_segment, isc_info_sql_stmt_put_segment,
    isc_info_sql_stmt_exec_procedure, isc_info_sql_stmt_start_trans,
    isc_info_sql_stmt_commit, isc_info_sql_stmt_rollback,
    isc_info_sql_stmt_select_for_upd, isc_info_sql_stmt_set_generator,
    isc_info_sql_stmt_savepoint]).

