%%% The MIT License (MIT)
%%% Copyright (c) 2016 Hajime Nakagami<nakagami@gmail.com>

-module(efirebirdsql_srp).

-compile([export_all]).

%% get Random Binary
getRandom(Length) ->
    case Length of
    0 -> [];
    _ -> [random:uniform(256)-1|getRandom(Length - 1)]
    end.

%% N: A large prime
getPrime() ->
    <<16#E67D2E994B2F900C3F41F08F5BB2627ED0D49EE1FE767A52EFCD565CD6E768812C3E1E9CE8F0A8BEA6CB13CD29DDEBF7A96D4A93B55D488DF099A15C89DCB0640738EB2CBDD9A8F7BAB561AB1B0DC1C6CDABF303264A08D1BCA932D1F1EE428B619D970F342ABA9A65793B8B2F041AE5364350C16F735F56ECBCA87BD57B29E7:256>>.

%% g: A generator modulo N
getGenerator() -> <<2>>.

%% srp version 6
getVersion() -> '6'.

% TODO:fix me
% randomly generated 32 byte number
getClientPrivate() ->
    <<16#C49F832EE8D67ECF9E7F2785EB0622D8B3FE2344C00F96E1AEF4103CA44D51F9:256>>.

% TODO:fix me
% randomly generated 32 byte number
getServerPrivate() ->
    <<16#C49F832EE8D67ECF9E7F2785EB0622D8B3FE2344C00F96E1AEF4103CA44D51F9:256>>.


%%  k = Multiplier parameter (k = H(N, g) in SRP-6a)
getK() -> 1277432915985975349439481660349303019122249719989.

%% x = H(salt, H(username, :, password))
getDerivedKey(Username, Password, Salt) ->
    crypto:hash(sha, [Salt, crypto:hash(sha, [Username, <<$:>>, Password])]).


%% v = g^x
getVerifier(Username, Password, Salt) ->
    Generator = getGenerator(),
    Prime = getPrime(),
    DerivedKey = getDerivedKey(Username, Password, Salt),
    crypto:mod_pow(Generator, DerivedKey, Prime).

%% client public key
getClientPublic() ->
    PrivateKey = getClientPrivate(),
    Generator = getGenerator(),
    Prime = getPrime(),
    Version = getVersion(),
    {Pub, PrivateKey} = crypto:generate_key(srp, {user, [Generator, Prime, Version]}, PrivateKey),
    Pub.

%% server public key
getServerPublic(Username, Password, Salt) ->
    PrivateKey = getServerPrivate(),
    Generator = getGenerator(),
    Prime = getPrime(),
    Version = getVersion(),
    Verifier = getVerifier(Username, Password, Salt),
    {Pub, PrivateKey} = crypto:generate_key(srp, {host, [Verifier, Generator, Prime, Version]}, PrivateKey),
    Pub.

%% client session key
computeClientKey(Username, Password, Salt) ->
    ServerPublic = getServerPublic(Username, Password, Salt),
    ClientPrivate = getClientPrivate(),
    ClientPublic = getClientPublic(),
    Generator = getGenerator(),
    Prime = getPrime(),
    Version = getVersion(),
    DerivedKey = getDerivedKey(Username, Password, Salt),
    crypto:compute_key(srp, ServerPublic, {ClientPublic, ClientPrivate}, {user, [DerivedKey, Prime, Generator, Version]}).

%% server session key
computeServerKey(Username, Password, Salt) ->
    ClientPublic = getClientPublic(),
    ServerPrivate = getServerPrivate(),
    ServerPublic = getServerPublic(Username, Password, Salt),
    Prime = getPrime(),
    Version = getVersion(),
    Verifier = getVerifier(Username, Password, Salt),
    crypto:compute_key(srp, ClientPublic, {ServerPublic, ServerPrivate}, {host, [Verifier, Prime, Version]}).

