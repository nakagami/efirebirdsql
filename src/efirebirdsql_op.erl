%%% The MIT License (MIT)
%%% Copyright (c) 2016 Hajime Nakagami<nakagami@gmail.com>

-module(efirebirdsql_op).

-export([op_name/1, op_val/1, op_connect/4,
    get_response/1]).
-compile([export_all]).

-include("efirebirdsql.hrl").

-define(BUFSIZE, 1024).

%%% skip 4 byte alignment socket stream
skip4(Sock, Len) ->
    case Len rem 4 of
        0 -> {ok};
        1 -> gen_tcp:recv(Sock, 3);
        2 -> gen_tcp:recv(Sock, 2);
        3 -> gen_tcp:recv(Sock, 1)
    end.

%%% big endian number list fill 4 byte alignment
byte4(N, big) ->
    LB = binary:encode_unsigned(N, big),
    LB4 = case size(LB) of
            1 -> << <<0,0,0>>/binary, LB/binary >>;
            2 -> << <<0,0>>/binary, LB/binary >>;
            3 -> << <<0>>/binary, LB/binary >>;
            4 -> LB
        end,
    binary_to_list(LB4);
byte4(N, little) ->
    LB = binary:encode_unsigned(N, little),
    LB4 = case size(LB) of
            1 -> << LB/binary, <<0,0,0>>/binary >>;
            2 -> << LB/binary, <<0,0>>/binary >>;
            3 -> << LB/binary, <<0>>/binary >>;
            4 -> LB
        end,
    binary_to_list(LB4).

byte4(N) ->
    byte4(N, big).

%%% 4 byte padding
pad4(L) ->
    case length(lists:flatten(L)) rem 4 of
        0 -> [];
        1 -> [0, 0, 0];
        2 -> [0, 0];
        3 -> [0]
    end.

list_to_xdr_string(L) ->
    lists:flatten([byte4(length(L)), L, pad4(L)]).

list_to_xdr_bytes(L) ->
    list_to_xdr_string(L).

uid(Host, Username) ->
    Data = lists:flatten([
        1,  %% CNCT_user
        length(Username),
        Username,
        4,  %% CNCT_host
        length(Host),
        Host]),
    list_to_xdr_bytes(Data).

%%% create op_connect binary
op_connect(Host, Username, _Password, Database) ->
    %% PROTOCOL_VERSION,ArchType(Generic),MinAcceptType,MaxAcceptType,Weight
    Protocols = lists:flatten(
        [byte4(10), byte4(1), byte4(0), byte4(3), byte4(2)]),
    Buf = [
        byte4(op_val(op_connect)),
        byte4(op_val(op_attach)),
        byte4(3),  %% CONNECT_VERSION
        byte4(1),  %% arch_generic,
        list_to_xdr_string(Database),
        byte4(1),  %% Count of acceptable protocols (VERSION 10 only)
        uid(Host, Username)],
    list_to_binary([Buf, Protocols]).

%%% create op_attach binary
op_attach(Username, Password, Database) ->
    Charset = "UTF8",
    Dpb = lists:flatten([
        1,
        48, length(Charset), Charset,    %% isc_dpb_lc_ctype = 48
        28, length(Username), Username,  %% isc_dpb_user_name 28
        29, length(Password), Password   %% isc_dpb_password = 29
    ]),
    list_to_binary(lists:flatten([
        byte4(op_val(op_attach)),
        byte4(0),
        list_to_xdr_string(Database),
        list_to_xdr_bytes(Dpb)])).


%%% create op_connect binary
op_create(Username, Password, Database, PageSize) ->
    Charset = "UTF8",
    Dpb = lists:flatten([
        1,
        68, length(Charset), Charset,   %% isc_dpb_set_db_charset = 68
        48, length(Charset), Charset,   %% isc_dpb_lc_ctype = 48
        28, length(Username), Username, %% isc_dpb_user_name 28
        29, length(Password), Password, %% isc_dpb_password = 29
        63, 4, byte4(3, little),        %% isc_dpb_sql_dialect = 63
        24, 4, byte4(1, little),        %% isc_dpb_force_write = 24
        54, 4, byte4(1, little),        %% isc_dpb_overwrite = 54
        4, 4, byte4(PageSize, little)   %% isc_dpb_page_size = 4
    ]),
    list_to_binary(lists:flatten([
        byte4(op_val(op_create)),
        byte4(0),
        list_to_xdr_string(Database),
        list_to_xdr_bytes(Dpb)])).


%%% begin transaction
op_transaction(DbHandle, Tpb) ->
    list_to_binary([
        byte4(op_val(op_transaction)),
        byte4(DbHandle),
        list_to_xdr_bytes(Tpb)]).

%%% allocate statement
op_allocate_statement(DbHandle) ->
    list_to_binary([byte4(op_val(op_allocate_statement)), byte4(DbHandle)]).

%%% prepare statement
op_prepare_statement(TransHandle, StmtHandle, Sql) ->
    DescItems = [
        21,     %% isc_info_sql_stmt_type
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
        ],
    list_to_binary([
        byte4(op_val(op_prepare_statement)),
        byte4(TransHandle),
        byte4(StmtHandle),
        byte4(3),
        list_to_xdr_string(binary_to_list(Sql)),
        list_to_xdr_bytes(DescItems),
        byte4(?BUFSIZE)]).


%%% commit
op_commit_retaining(TransHandle) ->
    list_to_binary([byte4(op_val(op_commit_retaining)), byte4(TransHandle)]).

%%% parse status vector
parse_status_vector_integer(Sock) ->
    {ok, <<NumArg:32>>} = gen_tcp:recv(Sock, 4),
    NumArg.

parse_status_vector_string(Sock) ->
    Len = parse_status_vector_integer(Sock),
    {ok, Bin} = gen_tcp:recv(Sock, Len),
    skip4(Sock, Len),
    binary_to_list(Bin).

parse_status_vector_args(Sock, Args) ->
    %% TODO: convert status vector to error message.
    {ok, <<IscArg:32>>} = gen_tcp:recv(Sock, 4),
    case IscArg of
    0 ->    %% isc_arg_end
        Args;
    1 ->    %% isc_arg_gds
        parse_status_vector_args(Sock, [parse_status_vector_integer(Sock) | Args]);
    2 ->    %% isc_arg_string
        parse_status_vector_args(Sock, [parse_status_vector_string(Sock) | Args]);
    4 ->    %% isc_arg_number
        parse_status_vector_args(Sock, [parse_status_vector_integer(Sock) | Args]);
    5 ->    %% isc_arg_interpreted
        parse_status_vector_args(Sock, [parse_status_vector_string(Sock) | Args]);
    19 ->   %% isc_arg_sql_state
        parse_status_vector_args(Sock, [parse_status_vector_string(Sock) | Args])
    end.

parse_status_vector(Sock) ->
    lists:reverse(parse_status_vector_args(Sock, [])).

parse_select_item(Sock) ->
    #column{}.

parse_select_items(Sock) ->
    [].

%% recieve and parse response
get_response(Sock) ->
    {ok, <<OpCode:32>>} = gen_tcp:recv(Sock, 4),
    case op_name(OpCode) of
        op_response ->
            {ok, <<Handle:32, _ObjectID:64, Len:32>>} = gen_tcp:recv(Sock, 16),
            _Buf = if
                Len =/= 0 ->
                    {ok, Buf} = gen_tcp:recv(Sock, Len),
                    skip4(Sock, Len),
                    Buf;
                true -> []
            end,
            R = parse_status_vector(Sock),
            case R of
                [0] -> {op_response, {ok, Handle}};
                _ -> {op_response, {error, R}}
            end;
        op_accept ->
            {ok, <<_AcceptVersionMasks:24, AcceptVersion:8,
                    _AcceptArchtecture:32, _AcceptType:32>>} = gen_tcp:recv(Sock, 12),
            {op_accept, AcceptVersion};
        op_reject ->
            op_reject;
        op_dummy ->
            get_response(Sock);
        true ->
            {error, "response error"}
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


