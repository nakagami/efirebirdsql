%%% The MIT License (MIT)
%%% Copyright (c) 2021 Hajime Nakagami<nakagami@gmail.com>

-module(efirebirdsql_conv_tests).

-include_lib("eunit/include/eunit.hrl").

params_to_blr_test() ->
    % Firebird 2.5
    {Blr, Value} = efirebirdsql_conv:params_to_blr(11, maps:new(), [nil]),
    ?assertEqual("0502040002000E00000700FF4C", efirebirdsql_srp:to_hex(Blr)),
    ?assertEqual("FFFFFFFF", efirebirdsql_srp:to_hex(Value)),

    % Firebird 3+
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

%% Wire representation of a date parameter: the 4 byte day count that follows
%% the null bitmap, which is the same encoding the server sends back for a
%% DATE column.
date_wire_bytes(Date) ->
    {_Blr, Value} = efirebirdsql_conv:params_to_blr(13, maps:new(), [Date]),
    <<_NullBitmap:4/binary, DayCount:4/binary>> = list_to_binary(Value),
    DayCount.

parse_date_test() ->
    %% Dates on or after the modified Julian date epoch (1858-11-17) travel as
    %% a positive day count and always round tripped.
    ?assertEqual({1858, 11, 17}, efirebirdsql_conv:parse_date(date_wire_bytes({1858, 11, 17}))),
    ?assertEqual({1967, 8, 11}, efirebirdsql_conv:parse_date(date_wire_bytes({1967, 8, 11}))),
    ?assertEqual({2021, 1, 1}, efirebirdsql_conv:parse_date(date_wire_bytes({2021, 1, 1}))),

    %% Older dates travel as a negative day count. Reading it unsigned turned
    %% them into a huge positive offset, e.g. 0100-01-01 came back as
    %% {11759321, 1, 21}.
    ?assertEqual({1858, 11, 16}, efirebirdsql_conv:parse_date(date_wire_bytes({1858, 11, 16}))),
    ?assertEqual({1800, 1, 1}, efirebirdsql_conv:parse_date(date_wire_bytes({1800, 1, 1}))),
    ?assertEqual({100, 1, 1}, efirebirdsql_conv:parse_date(date_wire_bytes({100, 1, 1}))).

parse_timestamp_test() ->
    %% parse_timestamp/1 delegates the date half to parse_date/1, so a pre epoch
    %% timestamp only reads back correctly once the day count is signed.
    Midnight = <<0, 0, 0, 0>>,
    Old = <<(date_wire_bytes({100, 1, 1}))/binary, Midnight/binary>>,
    ?assertEqual({{100, 1, 1}, {0, 0, 0, 0}}, efirebirdsql_conv:parse_timestamp(Old)).

float_param_to_blr_test() ->
    %% A float travels as blr_double (27) with its 8 IEEE 754 bytes, not as
    %% text: 0.09 is 16#3FB70A3D70A3D70A.
    {Blr, Value} = efirebirdsql_conv:params_to_blr(13, maps:new(), [0.09]),
    ?assertEqual("0502040002001B0700FF4C", efirebirdsql_srp:to_hex(Blr)),
    ?assertEqual("000000003FB70A3D70A3D70A", efirebirdsql_srp:to_hex(Value)),

    % Firebird 2.5: the null indicator follows the value
    {Blr1, Value1} = efirebirdsql_conv:params_to_blr(11, maps:new(), [-0.09]),
    ?assertEqual("0502040002001B0700FF4C", efirebirdsql_srp:to_hex(Blr1)),
    ?assertEqual("BFB70A3D70A3D70A00000000", efirebirdsql_srp:to_hex(Value1)),

    %% Mixed with other parameters and a null
    {Blr2, Value2} = efirebirdsql_conv:params_to_blr(13, maps:new(), [1, 2.5, nil]),
    ?assertEqual("050204000600080007001B07000E00000700FF4C", efirebirdsql_srp:to_hex(Blr2)),
    ?assertEqual("04000000000000014004000000000000", efirebirdsql_srp:to_hex(Value2)).
