%%% The MIT License (MIT)
%%% Copyright (c) 2015,2019 Hajime Nakagami<nakagami@gmail.com>

-module(efirebirdsql_srp_tests).

-include_lib("eunit/include/eunit.hrl").

random_test() ->
    Username = "SYSDBA",
    Password = "masterkey",
    Salt = efirebirdsql_srp:get_salt(),
    ClientPrivate = efirebirdsql_srp:get_private_key(),
    ClientPublic = efirebirdsql_srp:client_public(ClientPrivate),
    V = efirebirdsql_srp:get_verifier(Username, Password, Salt),

    ServerPrivate = efirebirdsql_srp:get_private_key(),
    ServerPublic = efirebirdsql_srp:server_public(V, ServerPrivate),
    ServerSession = efirebirdsql_srp:server_session(
        Username, Password, Salt, ClientPublic, ServerPublic, ServerPrivate),
    {_M, ClientSession} = efirebirdsql_srp:client_proof(
        Username, Password, Salt, ClientPublic, ServerPublic, ClientPrivate, sha),
    ?assertEqual(ServerSession, ClientSession).

bin_hex_test() ->
    ?assertEqual(efirebirdsql_srp:to_hex(<<1,2,3,255>>), "010203FF"),
    ?assertEqual(efirebirdsql_srp:to_hex([1,2,3,255]), "010203FF").

client_public_test() ->
    Private = 16#60975527035CF2AD1989806F0407210BC81EDC04E2762A56AFD529DDDA2D4393,
    Public = efirebirdsql_srp:client_public(Private),
    ?assertEqual(
        Public,
        16#712C5F8A2DB82464C4D640AE971025AA50AB64906D4F044F822E8AF8A58ADABBDBE1EFABA00BCCD4CDAA8A955BC43C3600BEAB9EBB9BD41ACC56E37F1A48F17293F24E876B53EEA6A60712D3F943769056B63202416827B400E162A8C0938D482274307585E0BC1D9DD52EFA7330B28E41B7CFCEFD9E8523FD11440EE5DE93A8
    ),
    SpecificData = efirebirdsql_srp:to_hex(Public),
    ?assertEqual(
        SpecificData,
        "712C5F8A2DB82464C4D640AE971025AA50AB64906D4F044F822E8AF8A58ADABBDBE1EFABA00BCCD4CDAA8A955BC43C3600BEAB9EBB9BD41ACC56E37F1A48F17293F24E876B53EEA6A60712D3F943769056B63202416827B400E162A8C0938D482274307585E0BC1D9DD52EFA7330B28E41B7CFCEFD9E8523FD11440EE5DE93A8"
    ).

srp_sha1_test() ->
    Username = "SYSDBA",
    Password = "masterkey",
    Salt = efirebirdsql_srp:get_debug_salt(),
    ClientPrivate = efirebirdsql_srp:get_debug_private_key(),
    ClientPublic = efirebirdsql_srp:client_public(ClientPrivate),
    V = efirebirdsql_srp:get_verifier(Username, Password, Salt),

    ServerPrivate = efirebirdsql_srp:get_debug_private_key(),
    ServerPublic = efirebirdsql_srp:server_public(V, ServerPrivate),
    ServerSession = efirebirdsql_srp:server_session(
        Username, Password, Salt, ClientPublic, ServerPublic, ServerPrivate),
    {M, ClientSession} = efirebirdsql_srp:client_proof(
        Username, Password, Salt, ClientPublic, ServerPublic, ClientPrivate, sha),
    ?assertEqual(ServerSession, ClientSession),
    ?assertEqual(efirebirdsql_srp:to_hex(M), "8C12324BB6E9E683A3EE62E13905B95D69F028A9").

srp_sha256_test() ->
    Username = "SYSDBA",
    Password = "masterkey",
    Salt = efirebirdsql_srp:get_debug_salt(),
    ClientPrivate = efirebirdsql_srp:get_debug_private_key(),
    ClientPublic = efirebirdsql_srp:client_public(ClientPrivate),
    V = efirebirdsql_srp:get_verifier(Username, Password, Salt),

    ServerPrivate = efirebirdsql_srp:get_debug_private_key(),
    ServerPublic = efirebirdsql_srp:server_public(V, ServerPrivate),
    ServerSession = efirebirdsql_srp:server_session(
        Username, Password, Salt, ClientPublic, ServerPublic, ServerPrivate),
    {M, ClientSession} = efirebirdsql_srp:client_proof(
        Username, Password, Salt, ClientPublic, ServerPublic, ClientPrivate, sha256),
    ?assertEqual(ServerSession, ClientSession),
    ?assertEqual(efirebirdsql_srp:to_hex(M), "4675C18056C04B00CC2B991662324C22C6F08BB90BEB3677416B03469A770308").
