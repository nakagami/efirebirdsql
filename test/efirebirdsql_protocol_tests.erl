%%% The MIT License (MIT)
%%% Copyright (c) 2019 Hajime Nakagami<nakagami@gmail.com>

-module(efirebirdsql_protocol_tests).

-include_lib("eunit/include/eunit.hrl").
-include("efirebirdsql.hrl").

tmp_dbname() ->
    lists:flatten(io_lib:format("/tmp/~p.fdb", [erlang:system_time()])).

protocol_test() ->
    {ok, Conn} = efirebirdsql_protocol:connect(
        "localhost", os:getenv("ISC_USER", "sysdba"), os:getenv("ISC_PASSWORD","masterkey"), tmp_dbname(),
        [{createdb, true}, {auth_plugin, "Srp"}]),
    ?assertEqual(Conn#conn.auto_commit, true),

    {ok, C} = efirebirdsql_protocol:begin_transaction(true, Conn),
    {ok, Stmt} = efirebirdsql_protocol:allocate_statement(C),

    {ok, Stmt2} = efirebirdsql_protocol:prepare_statement(
        <<"SELECT rdb$relation_name, rdb$owner_name FROM rdb$relations WHERE rdb$system_flag=?">>, C, Stmt),
    {ok, Stmt3} = efirebirdsql_protocol:execute(C, Stmt2, [1]),
    _Description = efirebirdsql_protocol:description(Stmt3),

    {ok, Stmt4} = efirebirdsql_protocol:prepare_statement(
        <<"SELECT RDB$RELATION_ID, RDB$EXTERNAL_FILE FROM rdb$relations WHERE rdb$system_flag=?">>, C, Stmt3),
    {ok, Stmt5} = efirebirdsql_protocol:execute(C, Stmt4, [1]),

    {ok, _Rows, Stmt6} = efirebirdsql_protocol:fetchall(C, Stmt5),
    ?assertEqual(
        efirebirdsql_protocol:columns(Stmt6),

        [{<<"RDB$RELATION_ID">>,short,0,2,true},
         {<<"RDB$EXTERNAL_FILE">>,varying,0,255,true}]
    ),

    {ok, Stmt7} = efirebirdsql_protocol:prepare_statement(<<"
        CREATE TABLE foo (
            a INTEGER NOT NULL,
            b VARCHAR(30) NOT NULL UNIQUE,
            c VARCHAR(1024),
            d DECIMAL(16,3) DEFAULT -0.123,
            e DATE DEFAULT '1967-08-11',
            f TIMESTAMP DEFAULT '1967-08-11 23:45:01',
            g TIME DEFAULT '23:45:01',
            h0 BLOB SUB_TYPE 0,
            h1 BLOB SUB_TYPE 1,
            i DOUBLE PRECISION DEFAULT 1.0,
            j FLOAT DEFAULT 2.0,
            PRIMARY KEY (a),
            CONSTRAINT CHECK_A CHECK (a <> 0)
        )">>, C, Stmt6),
    {ok, Stmt8} = efirebirdsql_protocol:execute(C, Stmt7, []),

    {ok, nil, Stmt9} = efirebirdsql_protocol:fetchall(C, Stmt8),

    {ok, Stmt10} = efirebirdsql_protocol:prepare_statement(
        <<"INSERT INTO foo(a, b) VALUES(?, ?)">>, C, Stmt9),
    {ok, Stmt11} = efirebirdsql_protocol:execute(C, Stmt10, [1, "b"]),
    ?assertEqual(Stmt11#stmt.rows, nil),
    {ok, 1} = efirebirdsql_protocol:rowcount(C, Stmt11),

    {ok, Stmt12} = efirebirdsql_protocol:prepare_statement(
        <<"SELECT * FROM foo WHERE f = ?">>, C, Stmt11),
    {ok, Stmt13} = efirebirdsql_protocol:execute(C, Stmt12, [{{1967, 8, 11}, {23, 45, 1, 0}}]),
    {ok, Rows, Stmt14} = efirebirdsql_protocol:fetchall(C, Stmt13),
    ?assertEqual(length(Rows),  1),
    {ok, 1} = efirebirdsql_protocol:rowcount(C, Stmt14),

    {ok, Stmt15} = efirebirdsql_protocol:prepare_statement(
        <<"UPDATE foo SET b=? WHERE a=?">>, C, Stmt14),
    {ok, Stmt16} = efirebirdsql_protocol:execute(C, Stmt15, ["c", 1]),
    {ok, 1} = efirebirdsql_protocol:rowcount(C, Stmt16),

    {ok, Stmt17} = efirebirdsql_protocol:unallocate_statement(<<"SELECT * FROM foo">>),
    {ok, Stmt18} = efirebirdsql_protocol:execute(C, Stmt17, []),
    ?assertEqual(Stmt18#stmt.closed, false),
    {ok, Rows2, Stmt19} = efirebirdsql_protocol:fetchall(C, Stmt18),
    ?assertEqual(Stmt19#stmt.closed, true),
    ?assertEqual(length(Rows2),  1),
    {ok, 1} = efirebirdsql_protocol:rowcount(C, Stmt19),

    {ok, Stmt20} = efirebirdsql_protocol:free_statement(C, Stmt19, drop),
    ?assertEqual(Stmt20#stmt.stmt_handle, nil),
    ?assertEqual(Stmt20#stmt.sql, <<"SELECT * FROM foo">>),
    {ok, Stmt21} = efirebirdsql_protocol:execute(C, Stmt20, []),
    ?assertEqual(Stmt21#stmt.closed, false),
    {ok, Rows3, Stmt22} = efirebirdsql_protocol:fetchall(C, Stmt21),
    ?assertEqual(Stmt22#stmt.closed, true),
    ?assertEqual(Rows2, Rows3),
    {ok, 1} = efirebirdsql_protocol:rowcount(C, Stmt22),

    ok = efirebirdsql_protocol:rollback_retaining(C),
    {ok, _} = efirebirdsql_protocol:close(C).


connect_test(_DbName, 0) ->
    ok;
connect_test(DbName, Count) ->
    {ok, C} = efirebirdsql_protocol:connect(
        "localhost", os:getenv("ISC_USER", "sysdba"), os:getenv("ISC_PASSWORD", "masterkey"), DbName,
        [{auth_plugin, "Srp"}]),
    efirebirdsql_protocol:close(C),
    connect_test(DbName, Count-1).
connect_test() ->
    DbName = tmp_dbname(),
    {ok, C} = efirebirdsql_protocol:connect(
        "localhost", os:getenv("ISC_USER", "sysdba"), os:getenv("ISC_PASSWORD", "masterkey"), DbName,
        [{createdb, true}, {auth_plugin, "Srp"}]),
    efirebirdsql_protocol:close(C),
    connect_test(DbName, 10).

connect_error_test() ->
    {error, ErrNo, Reason, _Conn} = efirebirdsql_protocol:connect("localhost", os:getenv("ISC_USER", "sysdba"), os:getenv("ISC_PASSWORD", "masterkey"), "something_wrong_database", []),
    ?assertEqual(ErrNo, 335544734),
    ?assertNotEqual(Reason, nil).

lock_timeout_tpb_test() ->
    %% nil (default): no isc_tpb_lock_timeout, historical behavior preserved.
    ?assertEqual([3, 9, 6, 15, 17], efirebirdsql_protocol:tpb(false, nil)),
    ?assertEqual([3, 9, 6, 15, 17, 16], efirebirdsql_protocol:tpb(true, nil)),
    %% integer seconds: appends isc_tpb_lock_timeout (21) + length 4 + value (little-endian).
    ?assertEqual([21, 4, 12, 0, 0, 0], lists:nthtail(5, efirebirdsql_protocol:tpb(false, 12))),
    ?assertEqual([21, 4, 12, 0, 0, 0], lists:nthtail(6, efirebirdsql_protocol:tpb(true, 12))),
    %% values >255 are encoded across the 4 little-endian bytes (e.g. 300 = 16#012C).
    ?assertEqual([21, 4, 16#2C, 16#01, 0, 0], lists:nthtail(5, efirebirdsql_protocol:tpb(false, 300))),
    %% read committed base is preserved (version3, write, wait, read_committed, rec_version)
    ?assertEqual([3, 9, 6, 15, 17], lists:sublist(efirebirdsql_protocol:tpb(false, 12), 5)).

sock_options_test() ->
    %% Default: keepalive enabled so the OS can reap a silently dead peer, and
    %% no send_timeout (historical behavior preserved).
    Default = efirebirdsql_protocol:sock_options([]),
    ?assert(lists:member({keepalive, true}, Default)),
    ?assertNot(lists:keymember(send_timeout, 1, Default)),
    ?assertNot(lists:keymember(send_timeout_close, 1, Default)),
    %% Base options are still present.
    ?assert(lists:member({active, false}, Default)),
    ?assert(lists:member({packet, raw}, Default)),
    ?assert(lists:member(binary, Default)),

    %% keepalive can be turned off explicitly.
    Off = efirebirdsql_protocol:sock_options([{keepalive, false}]),
    ?assert(lists:member({keepalive, false}, Off)),

    %% send_timeout (integer ms) appends send_timeout + send_timeout_close, so a
    %% stuck send fails fast and closes the socket instead of blocking.
    WithTimeout = efirebirdsql_protocol:sock_options([{send_timeout, 15000}]),
    ?assert(lists:member({send_timeout, 15000}, WithTimeout)),
    ?assert(lists:member({send_timeout_close, true}, WithTimeout)),
    ?assert(lists:member({keepalive, true}, WithTimeout)),

    %% A non-integer send_timeout is ignored (stays disabled).
    Ignored = efirebirdsql_protocol:sock_options([{send_timeout, nil}]),
    ?assertNot(lists:keymember(send_timeout, 1, Ignored)).

%% Repeated connections with valid credentials must never be refused. Each one
%% draws fresh ephemeral SRP keys, and roughly one in 128 used to produce a
%% public value shorter than the modulus, which the padded serialization turned
%% into a spurious 335544472.
repeated_connect_never_fails_test_() ->
    {timeout, 300, fun() ->
        DbName = tmp_dbname(),
        {ok, Conn} = efirebirdsql_protocol:connect(
            "localhost", os:getenv("ISC_USER", "sysdba"), os:getenv("ISC_PASSWORD", "masterkey"),
            DbName, [{createdb, true}]),
        {ok, _} = efirebirdsql_protocol:close(Conn),
        lists:foreach(fun(Plugin) ->
            lists:foreach(fun(_) ->
                case efirebirdsql_protocol:connect(
                        "localhost", os:getenv("ISC_USER", "sysdba"),
                        os:getenv("ISC_PASSWORD", "masterkey"), DbName,
                        [{auth_plugin, Plugin}]) of
                    {ok, C} -> {ok, _} = efirebirdsql_protocol:close(C);
                    Other -> ?assert({unexpected_connect_result, Plugin, Other} =:= ok)
                end
            end, lists:seq(1, 500))
        end, ["Srp", "Srp256"])
    end}.

%% isc_login: "Your user name and password are not defined..."
-define(ISC_LOGIN, 335544472).

%% A rejected password is a normal protocol answer, not a driver failure. The
%% response to op_cont_auth used to be strict matched against op_response, so a
%% refusal raised badmatch inside efirebirdsql_op and killed the caller (under
%% DBConnection, a gen_statem crash report) instead of returning an error.
%% The refusal reaches the client in one of two shapes, and both must come back
%% the same way: an op_response with the status vector when the server has a
%% single auth plugin, or another op_cont_auth asking for the next plugin when
%% AuthServer lists several, as attic/firebird.conf does.
wrong_password_returns_error_test() ->
    lists:foreach(fun(Plugin) ->
        Result = efirebirdsql_protocol:connect(
            "localhost",
            os:getenv("ISC_USER", "sysdba"),
            "deliberately-wrong-password",
            tmp_dbname(),
            [{auth_plugin, Plugin}]),
        ?assertMatch({error, ?ISC_LOGIN, _, _}, Result),
        {error, _, Reason, Conn} = Result,
        ?assert(is_binary(Reason)),
        %% a failed connect must not hand back a live socket
        ?assertEqual(undefined, Conn#conn.sock)
    end, ["Srp", "Srp256"]).

%% An unknown user is refused earlier in the handshake, before there is a proof
%% to check: the server answers the op_connect itself with op_response, or it
%% sends a continuation with an empty challenge. get_connect_response/1 already
%% built a four element error for the first, but connect_database/5 only matched
%% the three element shape, so that path ended in case_clause; the second one
%% ended in a badmatch on a salt that never arrived.
unknown_user_returns_error_test() ->
    Result = efirebirdsql_protocol:connect(
        "localhost",
        "efirebirdsql_no_such_user",
        "whatever",
        tmp_dbname(),
        [{auth_plugin, "Srp"}]),
    ?assertMatch({error, ?ISC_LOGIN, _, _}, Result),
    {error, _, Reason, Conn} = Result,
    ?assert(is_binary(Reason)),
    ?assertEqual(undefined, Conn#conn.sock).
