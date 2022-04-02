%%% The MIT License (MIT)
%%% Copyright (c) 2021 Hajime Nakagami<nakagami@gmail.com>

-module(efirebirdsql_conv_tests).

-include_lib("eunit/include/eunit.hrl").

params_to_blr_test() ->
    {Blr1, Value1} = efirebirdsql_conv:params_to_blr(13, maps:new(), [nil]),
    ?assertEqual("0502040002000E00000700FF4C", efirebirdsql_srp:to_hex(Blr1)),
    ?assertEqual("01000000", efirebirdsql_srp:to_hex(Value1)),
    {Blr2, Value2} = efirebirdsql_conv:params_to_blr(13, maps:new(), ["foo", 1]),
    ?assertEqual("0502040004000E0300070008000700FF4C", efirebirdsql_srp:to_hex(Blr2)),
    ?assertEqual("00000000666F6F0000000001", efirebirdsql_srp:to_hex(Value2)),
    {Blr3, Value3} = efirebirdsql_conv:params_to_blr(13, maps:new(), [nil, nil, nil]),
    ?assertEqual("0502040006000E000007000E000007000E00000700FF4C", efirebirdsql_srp:to_hex(Blr3)),
    ?assertEqual("07000000", efirebirdsql_srp:to_hex(Value3)),
    {Blr4, Value4} = efirebirdsql_conv:params_to_blr(13, maps:new(), [nil, "foo", nil]),
    ?assertEqual("0502040006000E000007000E030007000E00000700FF4C", efirebirdsql_srp:to_hex(Blr4)),
    ?assertEqual("05000000666F6F00", efirebirdsql_srp:to_hex(Value4)),
    {Blr5, Value5} = efirebirdsql_conv:params_to_blr(13, maps:new(), ["foo", nil]),
    ?assertEqual("0502040004000E030007000E00000700FF4C", efirebirdsql_srp:to_hex(Blr5)),
    ?assertEqual("02000000666F6F00", efirebirdsql_srp:to_hex(Value5)).
