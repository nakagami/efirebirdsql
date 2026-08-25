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


%% Firebird serializes the SRP public values A and B as minimal big-endian
%% magnitudes. Padding them to the key size changes u, S, K and M, so a
%% connection whose ephemeral A or B happens to be shorter than 128 bytes is
%% rejected with 335544472 even when the password is correct.
%% Small private keys make that case deterministic: g^a stays far below the
%% modulus, so A is 1, 26 or 127 bytes instead of 128.
srp_short_public_value_vectors_test() ->
    Username = "SYSDBA",
    Password = "masterkey",
    Salt = binary:copy(<<7>>, 32),
    Vectors = [
        %% {a, b, byte_size(A), byte_size(B), K, M with sha, M with sha256}
        {1008, 200, 127, 128,
         "3EDFDE5AAC586DD72DC831D9411CFAA2D68DCCC2",
         "57E46F46C693D3B7968FFD2FB970ADC77F50B215",
         "E297CAD169014290F21EC830F083559A52A3E44283C457F6960C0B39F60CA3FD"},
        {200, 1630, 26, 127,
         "5A0521BB4C32037C5BE3260969951D5A4978886A",
         "85977BF41CD70604F23B3586AAE10357DAF8C906",
         "4604B9BFD39C20B8B27737563ED77F6B3277B461A99579BB51D1AE66C6A514B4"},
        {3, 47, 1, 128,
         "00085548093EC9A92174A6B36BE20956F45A0D4E",
         "BD2FDF3925D34BB711BE68D6FB6C04E89A8868EB",
         "2415E0239D695C51016421095699E2C49BEA4206AA3A76B049C2A4615EA431AB"},
        {1, 1, 1, 128,
         "A103D971329DF14055C71F49A078DCD7D3675A95",
         "5ECB8D916406746A3E4DD7D139E662287471D841",
         "6E03DA3353B4196FCD1FF2079CC3E04E724531D81C965C990FBBCE83D3CBB58F"}
    ],
    lists:foreach(fun({ClientPrivate, ServerPrivate, ALen, BLen, K, MSha, MSha256}) ->
        ClientPublic = efirebirdsql_srp:client_public(ClientPrivate),
        V = efirebirdsql_srp:get_verifier(Username, Password, Salt),
        ServerPublic = efirebirdsql_srp:server_public(V, ServerPrivate),

        %% guard the fixture itself: these vectors are only meaningful while at
        %% least one public value is shorter than the 128 byte modulus
        ?assertEqual(ALen, byte_size(binary:encode_unsigned(ClientPublic))),
        ?assertEqual(BLen, byte_size(binary:encode_unsigned(ServerPublic))),

        {M1, SessionKey} = efirebirdsql_srp:client_proof(
            Username, Password, Salt, ClientPublic, ServerPublic, ClientPrivate, sha),
        {M2, SessionKey} = efirebirdsql_srp:client_proof(
            Username, Password, Salt, ClientPublic, ServerPublic, ClientPrivate, sha256),

        ?assertEqual(K, efirebirdsql_srp:to_hex(SessionKey)),
        ?assertEqual(MSha, efirebirdsql_srp:to_hex(M1)),
        ?assertEqual(MSha256, efirebirdsql_srp:to_hex(M2)),

        %% the session key is a raw SHA-1 digest: a leading zero byte is part of
        %% it and must never be trimmed by an integer round trip
        ?assertEqual(20, byte_size(SessionKey)),

        %% and the server side must agree, otherwise both sides share the bug
        ServerSession = efirebirdsql_srp:server_session(
            Username, Password, Salt, ClientPublic, ServerPublic, ServerPrivate),
        ?assertEqual(SessionKey, ServerSession)
    end, Vectors).

%% One of the vectors above derives a session key whose first byte is zero.
srp_session_key_keeps_leading_zero_test() ->
    Username = "SYSDBA",
    Password = "masterkey",
    Salt = binary:copy(<<7>>, 32),
    ClientPrivate = 3,
    ClientPublic = efirebirdsql_srp:client_public(ClientPrivate),
    V = efirebirdsql_srp:get_verifier(Username, Password, Salt),
    ServerPublic = efirebirdsql_srp:server_public(V, 47),
    {_M, SessionKey} = efirebirdsql_srp:client_proof(
        Username, Password, Salt, ClientPublic, ServerPublic, ClientPrivate, sha),
    ?assertMatch(<<0, _/binary>>, SessionKey),
    ?assertEqual(20, byte_size(SessionKey)).
