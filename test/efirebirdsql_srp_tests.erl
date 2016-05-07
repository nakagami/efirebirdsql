%%% The MIT License (MIT)
%%% Copyright (c) 2015 Hajime Nakagami<nakagami@gmail.com>

-module(efirebirdsql_srp_tests).

-compile([export_all]).

-include_lib("eunit/include/eunit.hrl").

random_test() ->
    Username = "SYSDBA",
    Password = "masterkey",
    Salt = efirebirdsql_srp:get_salt(),
    {ClientPublic, ClientPrivate} = efirebirdsql_srp:client_seed(),
    V = efirebirdsql_srp:get_verifier(Username, Password, Salt),
    {ServerPublic, ServerPrivate} = efirebirdsql_srp:server_seed(V),
    ServerSession = efirebirdsql_srp:server_session(
        Username, Password, Salt, ClientPublic, ServerPublic, ServerPrivate),
    {M, ClientSession} = efirebirdsql_srp:client_proof(
        Username, Password, Salt, ClientPublic, ServerPublic, ClientPrivate),
    ?assertEqual(ServerSession, ClientSession).
