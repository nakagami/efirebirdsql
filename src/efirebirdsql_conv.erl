%%% The MIT License (MIT)
%%% Copyright (c) 2016 Hajime Nakagami<nakagami@gmail.com>

-module(efirebirdsql_conv).

-export([byte2/1, byte4/2, byte4/1,
    list_to_xdr_string/1, list_to_xdr_bytes/1,
    parse_date/1, parse_time/1, parse_timestamp/1, parse_number/2]).

%%% little endian 2byte
byte2(N) ->
    LB = binary:encode_unsigned(N, little),
    LB2 = case size(LB) of
            1 -> << LB/binary, <<0>>/binary >>;
            2 -> LB
        end,
    binary_to_list(LB2).

%%% big endian number list fill 4 byte alignment
byte4(N, big) ->
    LB = binary:encode_unsigned(N, big),
    LB4 = case size(LB) of
            1 -> << <<0,0,0>>/binary, LB/binary >>;
            2 -> << <<0,0>>/binary, LB/binary >>;
            3 -> << <<0>>/binary, LB/binary >>;
            4 -> LB
        end,
    binary_to_list(LB4);
byte4(N, little) ->
    LB = binary:encode_unsigned(N, little),
    LB4 = case size(LB) of
            1 -> << LB/binary, <<0,0,0>>/binary >>;
            2 -> << LB/binary, <<0,0>>/binary >>;
            3 -> << LB/binary, <<0>>/binary >>;
            4 -> LB
        end,
    binary_to_list(LB4).

byte4(N) ->
    byte4(N, big).

%%% 4 byte padding
pad4(L) ->
    case length(lists:flatten(L)) rem 4 of
        0 -> [];
        1 -> [0, 0, 0];
        2 -> [0, 0];
        3 -> [0]
    end.

list_to_xdr_string(L) ->
    lists:flatten([byte4(length(L)), L, pad4(L)]).

list_to_xdr_bytes(L) ->
    list_to_xdr_string(L).

parse_date(RawValue) ->
    L = size(RawValue) * 8,
    <<Num:L>> = RawValue,
    NDay1 = Num + 678882,
    Century = (4 * NDay1 -1) div 146097,
    NDay2 = 4 * NDay1 - 1 -  146097 * Century,
    Day1 = NDay2 div 4,

    NDay3 = (4 * Day1 + 3) div 1461,
    Day2 = 4 * Day1 + 3 - 1461 * NDay3,
    Day3 = (Day2 + 4) div 4,

    Month1 = (5 * Day3 - 3) div 153,
    Day4 = 5 * Day3 - 3 - 153 * Month1,
    Day5 = (Day4 + 5) div 5,
    Year1 = 100 * Century + NDay3,
    Month2 = if Month1 < 10 -> Month1 + 3; true -> Month1 - 9 end,
    Year2 = if Month1 < 10 -> Year1; true -> Year1 + 1 end,
    {Year2, Month2, Day5}.

parse_time(RawValue) ->
    L = size(RawValue) * 8,
    <<N:L>> = RawValue,
    S = N div 10000,
    M = S div 60,
    H = M div 60,
    {H, M rem 60, S rem 60, (N rem 10000) * 100}.

parse_timestamp(RawValue) ->
    <<YMD:4/binary, HMS:4/binary>> = RawValue,
    {parse_date(YMD), parse_time(HMS)}.

parse_number(RawValue, Scale) when Scale =:= 0  ->
    L = size(RawValue) * 8,
    <<V:L/integer>> = RawValue,
    V;
parse_number(RawValue, Scale) ->
    L = size(RawValue) * 8,
    <<V:L/integer>> = RawValue,
    V * math:pow(10, Scale).

