%%% The MIT License (MIT)
%%% Copyright (c) 2015 Hajime Nakagami<nakagami@gmail.com>

-module(efirebirdsql_srp_tests).

-compile([export_all]).

-include_lib("eunit/include/eunit.hrl").

random_test() ->
    Salt = efirebirdsql_srp:get_salt(),
    ?assertEqual(
        efirebirdsql_srp:get_client_session_key("sysdba", "masterkey", Salt),
        efirebirdsql_srp:get_server_session_key("sysdba", "masterkey", Salt)).
