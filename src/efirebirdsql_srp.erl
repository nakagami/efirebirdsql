%%% The MIT License (MIT)
%%% Copyright (c) 2016 Hajime Nakagami<nakagami@gmail.com>

-module(efirebirdsql_srp).

-export([get_verifier/3, client_proof/6, client_session/6, server_session/6, get_salt/0, client_seed/0, server_seed/1]).

-spec int_to_bin(integer()) -> binary().
int_to_bin(Int) ->
    Len0 = length(erlang:integer_to_list(Int, 16)),
    Len1 = Len0 + (Len0 rem 2),
    Bits = Len1 * 4,
    <<Int:Bits>>.

-spec bin_to_int(binary()) -> integer().
bin_to_int(Bin) ->
    Bits = byte_size(Bin) * 8,
    <<Val:Bits>> = Bin,
    Val.

%% N: A large prime
get_prime() ->
    16#E67D2E994B2F900C3F41F08F5BB2627ED0D49EE1FE767A52EFCD565CD6E768812C3E1E9CE8F0A8BEA6CB13CD29DDEBF7A96D4A93B55D488DF099A15C89DCB0640738EB2CBDD9A8F7BAB561AB1B0DC1C6CDABF303264A08D1BCA932D1F1EE428B619D970F342ABA9A65793B8B2F041AE5364350C16F735F56ECBCA87BD57B29E7.

%% g: A generator modulo N
get_generator() -> 2.

%% get key size (bits)
get_key_size() -> 128.

%% k = H(N, g): Multiplier parameter
-spec get_k() -> integer().
get_k() ->
    bin_to_int(crypto:hash(sha, [int_to_bin(get_prime()), int_to_bin(get_generator())])).


%% x = H(salt, H(username, :, password))
-spec get_user_hash(binary(), binary(), binary()) -> binary().
get_user_hash(User, Pass, Salt) ->
    crypto:hash(sha, [Salt, crypto:hash(sha, [User, <<$:>>, Pass])]).

%% v = g^x
-spec get_verifier(list(), list(), binary()) -> binary().
get_verifier(Username, Password, Salt) ->
    User = list_to_binary(Username),
    Pass = list_to_binary(Password),
    DerivedKey = get_user_hash(User, Pass, Salt),
    crypto:mod_pow(get_generator(), DerivedKey, get_prime()).

%% salt 32 bytes binary
-spec get_salt() -> binary().
get_salt() -> int_to_bin(random:uniform(1 bsl (8*32)) - 1).

%% client {Public, Private} keys
-spec client_seed() -> {PublicKey::integer(), PrivateKey::integer()}.
client_seed() ->
    PrivateKey = random:uniform(1 bsl get_key_size()) -1,
    PublicKey = bin_to_int(crypto:mod_pow(get_generator(), PrivateKey, get_prime())),
    {PublicKey, PrivateKey}.

%% server {Public, Private} keys
-spec server_seed(binary()) -> {PublicKey:: integer(), PrivateKey::integer()}.
server_seed(V) ->
    PrivateKey = random:uniform(1 bsl get_key_size()) -1,
    GB = bin_to_int(crypto:mod_pow(get_generator(), PrivateKey, get_prime())),
    KV = (get_k() * bin_to_int(V)) rem get_prime(),
    PublicKey = (KV + GB) rem get_prime(),
    {PublicKey, PrivateKey}.

%% client session key
-spec client_session(list(), list(), binary(), integer(), integer(), integer()) -> binary().
client_session(Username, Password, Salt, ClientPublic, ServerPublic, ClientPrivate) ->
    User = list_to_binary(Username),
    Pass = list_to_binary(Password),
    U = bin_to_int(crypto:hash(sha, [int_to_bin(ClientPublic), int_to_bin(ServerPublic)])),
    X = get_user_hash(User, Pass, Salt),
    GX = bin_to_int(crypto:mod_pow(get_generator(), X, get_prime())),
    KGX = (get_k() * GX) rem get_prime(),
    Diff = (ServerPublic - KGX) rem get_prime(),
    UX = (U * bin_to_int(X)) rem get_prime(),
    AUX = (ClientPrivate * UX) rem get_prime(),
    SessionSecret = crypto:mod_pow(Diff, AUX, get_prime()),
    crypto:hash(sha, SessionSecret).

%% server session key
-spec server_session(list(), list(), binary(), integer(), integer(), integer()) -> binary().
server_session(Username, Password, Salt, ClientPublic, ServerPublic, ServerPrivate) ->
    U = bin_to_int(crypto:hash(sha, [int_to_bin(ClientPublic), int_to_bin(ServerPublic)])),
    V = get_verifier(Username, Password, Salt),
    VU = bin_to_int(crypto:mod_pow(V, U, get_prime())),
    AVU = (ClientPublic * VU) rem get_prime(),
    SessionSecret = crypto:mod_pow(AVU, ServerPrivate, get_prime()),
    crypto:hash(sha, SessionSecret).

-spec client_proof(list(), list(), binary(), integer(), integer(), integer()) -> {binary(), binary()}.
client_proof(Username, Password, Salt, ClientPublic, ServerPublic, ClientPrivate)
->
    User = list_to_binary(Username),
    K = client_session(Username, Password, Salt, ClientPublic, ServerPublic, ClientPrivate),
    N1 = bin_to_int(crypto:hash(sha, int_to_bin(get_prime()))),
    N2 = bin_to_int(crypto:hash(sha, int_to_bin(get_generator()))),
    M = crypto:hash(sha, [crypto:mod_pow(N1, N2, get_prime()), crypto:hash(sha, User), Salt, ClientPublic, ServerPublic, K]),
    {M, K}.

