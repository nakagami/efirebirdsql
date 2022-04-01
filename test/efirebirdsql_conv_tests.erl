%%% The MIT License (MIT)
%%% Copyright (c) 2021 Hajime Nakagami<nakagami@gmail.com>

-module(efirebirdsql_conv_tests).

-include_lib("eunit/include/eunit.hrl").

params_to_blr_test() ->
    {_, Value} = efirebirdsql_conv:params_to_blr(13, maps:new(), [nil, nil, nil]),
    ?assertEqual([7, 0, 0, 0], Value),
    {_, Value1} = efirebirdsql_conv:params_to_blr(13, maps:new(), [nil, "foo", nil]),
    ?assertEqual([5, 0, 0, 0, $f, $o, $o, 0], Value1),
    {_, Value2} = efirebirdsql_conv:params_to_blr(13, maps:new(), ["foo", nil]),
    ?assertEqual([2, 0, 0, 0, $f, $o, $o, 0], Value2).
