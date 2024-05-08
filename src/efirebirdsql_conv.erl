%%% The MIT License (MIT)
%%% Copyright (c) 2016-2021 Hajime Nakagami<nakagami@gmail.com>

-module(efirebirdsql_conv).

-export([byte2/1, byte4/2, byte4/1, pad4/1, list_to_xdr_string/1,
    list_to_xdr_bytes/1, parse_date/1,
    parse_time/1, parse_timestamp/1, parse_time_tz/1, parse_timestamp_tz/1,
    parse_number/2, parse_number/3, params_to_blr/3]).

%%% little endian 2byte
-spec byte2(integer()) -> list().
byte2(N) ->
    LB = binary:encode_unsigned(N, little),
    LB2 = case size(LB) of
    1 -> << LB/binary, <<0>>/binary >>;
    2 -> LB
    end,
    binary_to_list(LB2).

%%% big endian number list fill 4 byte alignment
-spec byte4(integer(), atom()) -> list().
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

%%% big endian number list fill 8 byte alignment
-spec byte8(integer(), atom()) -> list().
byte8(N, big) ->
    LB = binary:encode_unsigned(N, big),
    LB8 = case size(LB) of
    1 -> << <<0,0,0,0,0,0,0>>/binary, LB/binary >>;
    2 -> << <<0,0,0,0,0,0>>/binary, LB/binary >>;
    3 -> << <<0,0,0,0,0>>/binary, LB/binary >>;
    4 -> << <<0,0,0,0>>/binary, LB/binary >>;
    5 -> << <<0,0,0>>/binary, LB/binary >>;
    6 -> << <<0,0>>/binary, LB/binary >>;
    7 -> << <<0>>/binary, LB/binary >>;
    8 -> LB
    end,
    binary_to_list(LB8).

byte8(N) ->
    byte8(N, big).

%%% 4 byte padding
-spec pad4(list()) -> list().
pad4(L) ->
    case length(lists:flatten(L)) rem 4 of
        0 -> [];
        1 -> [0, 0, 0];
        2 -> [0, 0];
        3 -> [0]
    end.

-spec list_to_xdr_string(list()) -> list().
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
    <<YMD:4/binary, HMSN:4/binary>> = RawValue,
    {parse_date(YMD), parse_time(HMSN)}.

-spec parse_time_tz(binary()) -> {{integer(), integer(), integer(), integer()}, binary(), binary()}.
parse_time_tz(RawValue) ->
    <<HMSN:4/binary, TimeZoneID:16, OffsetID:16>> = RawValue,
    TimeZone = maps:get(TimeZoneID, efirebirdsql_tz_map:timezone_name_by_id()),
    Offset = maps:get(OffsetID, efirebirdsql_tz_map:timezone_name_by_id()),
    {parse_time(HMSN), TimeZone, Offset}.

-spec parse_timestamp_tz(binary()) -> {{integer(), integer(), integer()}, {integer(), integer(), integer(), integer()}, binary(), binary()}.
parse_timestamp_tz(RawValue) ->
    <<YMD:4/binary, HMSN:4/binary, TimeZoneID:16, OffsetID:16>> = RawValue,
    TimeZone = maps:get(TimeZoneID, efirebirdsql_tz_map:timezone_name_by_id()),
    Offset = maps:get(OffsetID, efirebirdsql_tz_map:timezone_name_by_id()),
    {parse_date(YMD), parse_time(HMSN), TimeZone, Offset}.

fill0(S, 0) -> S;
fill0(S, N) -> fill0([48 | S], N-1).
to_decimal(N, Scale) when N < 0 ->
    lists:flatten(["-", to_decimal(-N, Scale)]);
to_decimal(N, Scale) when N >= 0 ->
    Shift = if Scale < 0 -> -Scale; Scale >= 0 -> 0 end,
    V = if Scale =< 0 -> N; Scale > 0 -> N * trunc(math:pow(10, Scale)) end,
    S = integer_to_list(V),
    S2 = if length(S) =< Shift ->
        fill0(S, Shift - length(S) + 1);
        length(S) > Shift -> S
        end,
    {I, F} = lists:split(length(S2) - Shift, S2),
    lists:flatten([I, ".", F]).

pow10(N) -> pow10(10, N).
pow10(V, 0) -> V;
pow10(V, N) -> pow10(V * 10, N-1).

parse_number(RawValue, Scale) when Scale =:= 0  ->
    L = size(RawValue) * 8,
    <<V:L/signed-integer>> = RawValue,
    V;
parse_number(RawValue, Scale) when Scale > 0 ->
    L = size(RawValue) * 8,
    <<V:L/signed-integer>> = RawValue,
    integer_to_list(V * pow10(Scale));
parse_number(RawValue, Scale) when Scale < 0 ->
    L = size(RawValue) * 8,
    <<V:L/signed-integer>> = RawValue,
    to_decimal(V, Scale).
parse_number(Sign, V, Scale) ->
    case Sign of
        0 -> to_decimal(V, Scale);
        1 -> to_decimal(-V, Scale)
    end.

%% Convert execute() parameters to BLR and values.
param_to_date(Year, Month, Day) ->
    I = Month + 9,
    JY = Year + I div 12 - 1,
    JM = I rem 12,
    C = JY div 100,
    JY2 = JY - 100 * C,
    J = (146097 * C) div 4 + (1461 * JY2) div 4 + (153 * JM + 2) div 5 + Day - 678882,
    byte4(J).

param_to_time(Hour, Minute, Second, Microsecond) ->
    byte4((Hour*3600 + Minute*60 + Second) * 10000 + Microsecond div 100).

param_to_blr(V, _) when is_integer(V) and (V =< 16#7FFFFFFF) and (V >= -16#80000000) ->
    {[8, 0, 7, 0], byte4(V)};
param_to_blr(V, _) when is_integer(V)  ->
    {[16, 0, 7, 0], byte8(V)};
param_to_blr(V, _) when is_binary(V) ->
    B = binary_to_list(V),
    {lists:flatten([14, byte2(length(B)), 7, 0]),
        lists:flatten([B, pad4(B)])};
param_to_blr(V, TimeZoneIdByName) when is_list(V) ->
    %% decimal
    param_to_blr(list_to_binary(V), TimeZoneIdByName);
param_to_blr(V, TimeZoneIdByName) when is_float(V) ->
    %% float
    param_to_blr(float_to_binary(V), TimeZoneIdByName);
param_to_blr({{Hour, Minute, Second, Microsecond}, TimeZone}, TimeZoneIdByName) ->
    %% time with timezone
    TimeZoneId = maps:get(TimeZone, TimeZoneIdByName),
    {[28, 7, 0], lists:flatten(
        [param_to_time(Hour, Minute, Second, Microsecond), byte4(TimeZoneId, big)]
    )};
param_to_blr({{Year, Month, Day}, {Hour, Minute, Second, Microsecond}, TimeZone}, TimeZoneIdByName) ->
    %% timestamp with timezone
    TimeZoneId = maps:get(TimeZone, TimeZoneIdByName),
    {[29, 7, 0], lists:flatten([param_to_date(Year, Month, Day),
        param_to_time(Hour, Minute, Second, Microsecond), byte4(TimeZoneId, big)])};
param_to_blr({Year, Month, Day}, _) ->
    %% date
    {[12, 7, 0], lists:flatten(param_to_date(Year, Month, Day))};
param_to_blr({Hour, Minute, Second, Microsecond}, _) ->
    %% time
    {[13, 7, 0], lists:flatten(param_to_time(Hour, Minute, Second, Microsecond))};
param_to_blr({{Year, Month, Day}, {Hour, Minute, Second, Microsecond}}, _) ->
    %% timestamp
    {[35, 7, 0], lists:flatten([param_to_date(Year, Month, Day),
        param_to_time(Hour, Minute, Second, Microsecond)])};
param_to_blr(true, _) ->
    {[23, 7, 0], [1, 0, 0, 0]};
param_to_blr(false, _) ->
    {[23, 7, 0], [0, 0, 0, 0]}.

null_indicator_bits([], _Idx, Indicator) ->
    Indicator;
null_indicator_bits(Params, Idx, Indicator) ->
    [X | RestParams] = Params,
    V = if X =:= nil -> (1 bsl Idx); X =/= nil -> 0 end,
    null_indicator_bits(RestParams, Idx + 1, Indicator + V).

null_indicator_bits(Params) ->
    null_indicator_bits(Params, 0, 0).

null_bitmap(Params) ->
    Bitmap = null_indicator_bits(Params),
    Len = if
        length(Params) rem 8 =:= 0 ->
        length(Params) div 8;
        length(Params) rem 8 =/= 0 ->
        length(Params) div 8  + 1
        end,
    L = binary_to_list(<<Bitmap:Len/little-unit:8>>),
    L ++ pad4(L).

params_to_blr(_AcceptVersion, _TimeZoneIdByName, [], Blr, Value) ->
    {lists:reverse(Blr), lists:reverse(Value)};
params_to_blr(AcceptVersion, TimeZoneIdByName, Params, Blr, Value) ->
    [V | RestParams] = Params,

    {NewBlr, NewValue} = if
        AcceptVersion >= 13 ->
            if
            V =:= nil -> {[14, 0, 0, 7, 0], []};
            V =/= nil -> param_to_blr(V, TimeZoneIdByName)
            end;
        AcceptVersion < 13 ->
            if
            V =:= nil ->
                {[14, 0, 0, 7, 0], [255, 255, 255, 255]};
            V =/= nil ->
                {B, V2} = param_to_blr(V, TimeZoneIdByName),
                {B, V2 ++ [0, 0, 0, 0]}
            end
        end,

    params_to_blr(AcceptVersion, TimeZoneIdByName, RestParams, [NewBlr | Blr], [NewValue | Value]).

params_to_blr(AcceptVersion, TimeZoneIdByName, Params) ->
    {BlrBody, Value} = params_to_blr(AcceptVersion, TimeZoneIdByName, Params, [], []),
    L = length(Params) * 2,
    Blr = lists:flatten([[5, 2, 4, 0], byte2(L), BlrBody, [255, 76]]),
    NullBitmap = if
        AcceptVersion >= 13 -> null_bitmap(Params);
        AcceptVersion < 13 -> []
        end,
    {Blr, lists:flatten(NullBitmap ++ Value)}.
