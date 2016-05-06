%%% The MIT License (MIT)
%%% Copyright (c) 2016 Hajime Nakagami<nakagami@gmail.com>

-module(efirebirdsql_srp).

-export([get_client_session_key/3, get_server_session_key/3, get_salt/0]).

-spec get_random_list(non_neg_integer()) -> list().
get_random_list(Length) ->
    case Length of
    0 -> [];
    _ -> [random:uniform(256)-1|get_random_list(Length - 1)]
    end.

%% get Random Binary
-spec get_random(non_neg_integer()) -> binary().
get_random(Length) -> list_to_binary(get_random_list(Length)).

%% N: A large prime
get_prime() ->
    <<16#E67D2E994B2F900C3F41F08F5BB2627ED0D49EE1FE767A52EFCD565CD6E768812C3E1E9CE8F0A8BEA6CB13CD29DDEBF7A96D4A93B55D488DF099A15C89DCB0640738EB2CBDD9A8F7BAB561AB1B0DC1C6CDABF303264A08D1BCA932D1F1EE428B619D970F342ABA9A65793B8B2F041AE5364350C16F735F56ECBCA87BD57B29E7:256>>.

%% g: A generator modulo N
get_generator() -> <<2>>.

%% srp version 6
get_version() -> '6'.

%% x = H(salt, H(username, :, password))
get_user_hash(Username, Password, Salt) ->
    crypto:hash(sha, [Salt, crypto:hash(sha, [Username, <<$:>>, Password])]).

%% v = g^x
get_verifier(Username, Password, Salt) ->
    Generator = get_generator(),
    Prime = get_prime(),
    DerivedKey = get_user_hash(Username, Password, Salt),
    crypto:mod_pow(Generator, DerivedKey, Prime).

%% salt
get_salt() -> get_random(32).

%% client {Public, Private} keys
-spec get_client_keys() -> {binary(), binary()}.
get_client_keys() ->
    PrivateKey = get_random(16),
    Generator = get_generator(),
    Prime = get_prime(),
    Version = get_version(),
    crypto:generate_key(srp, {user, [Generator, Prime, Version]}, PrivateKey).

%% server {Public, Private} keys
-spec get_server_keys(binary(), binary(), binary()) -> {binary(), binary()}.
get_server_keys(Username, Password, Salt) ->
    PrivateKey = get_random(16),
    Generator = get_generator(),
    Prime = get_prime(),
    Version = get_version(),
    Verifier = get_verifier(Username, Password, Salt),
    crypto:generate_key(srp, {host, [Verifier, Generator, Prime, Version]}, PrivateKey).

%% client session key
-spec get_client_session_key(list(), list(), binary()) -> binary().
get_client_session_key(Username, Password, Salt) ->
    User = list_to_binary(Username),
    Pass = list_to_binary(Password),
    {ServerPublic, _} = get_server_keys(User, Pass, Salt),
    {ClientPublic, ClientPrivate} = get_client_keys(),
    Generator = get_generator(),
    Prime = get_prime(),
    Version = get_version(),
    DerivedKey = get_user_hash(User, Pass, Salt),
    crypto:compute_key(srp, ServerPublic, {ClientPublic, ClientPrivate}, {user, [DerivedKey, Prime, Generator, Version]}).

%% server session key
-spec get_server_session_key(list(), list(), binary()) -> binary().
get_server_session_key(Username, Password, Salt) ->
    User = list_to_binary(Username),
    Pass = list_to_binary(Password),
    {ClientPublic, _}  = get_client_keys(),
    {ServerPublic, ServerPrivate} = get_server_keys(User, Pass, Salt),
    Prime = get_prime(),
    Version = get_version(),
    Verifier = get_verifier(User, Pass, Salt),
    crypto:compute_key(srp, ClientPublic, {ServerPublic, ServerPrivate}, {host, [Verifier, Prime, Version]}).

