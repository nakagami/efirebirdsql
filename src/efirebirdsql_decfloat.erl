%%% The MIT License (MIT)
%%% Copyright (c) 2018 Hajime Nakagami<nakagami@gmail.com>

-module(efirebirdsql_decfloat).

-export([decimal_fixed_to_decimal/2, decimal64_to_decimal/1, decimal128_to_decimal/1]).

-spec dpd_to_int(integer()) -> integer().
dpd_to_int(Dpd) ->
    B0 = if Dpd band 2#0000000001 =/= 0 -> 1; true -> 0 end,
    B1 = if Dpd band 2#0000000010 =/= 0 -> 1; true -> 0 end,
    B2 = if Dpd band 2#0000000100 =/= 0 -> 1; true -> 0 end,
    B3 = if Dpd band 2#0000001000 =/= 0 -> 1; true -> 0 end,
    B4 = if Dpd band 2#0000010000 =/= 0 -> 1; true -> 0 end,
    B5 = if Dpd band 2#0000100000 =/= 0 -> 1; true -> 0 end,
    B6 = if Dpd band 2#0001000000 =/= 0 -> 1; true -> 0 end,
    B7 = if Dpd band 2#0010000000 =/= 0 -> 1; true -> 0 end,
    B8 = if Dpd band 2#0100000000 =/= 0 -> 1; true -> 0 end,
    B9 = if Dpd band 2#1000000000 =/= 0 -> 1; true -> 0 end,
    D = if
        B3 =:= 0 -> [
            B2 * 4 + B1 * 2 + B0,
            B6 * 4 + B5 * 2 + B4,
            B9 * 4 + B8 * 2 + B7
        ];
        {B3, B2, B1} =:= {1, 0, 0} -> [
            8 + B0,
            B6 * 4 + B5 * 2 + B4,
            B9 * 4 + B8 * 2 + B7
        ];
        {B3, B2, B1} =:= {1, 0, 1} -> [
            B6 * 4 + B5 * 2 + B0,
            8 + B4,
            B9 * 4 + B8 * 2 + B7
        ];
        {B3, B2, B1} =:= {1, 1, 0} -> [
            B9 * 4 + B8 * 2 + B0,
            B6 * 4 + B5 * 2 + B4,
            8 + B7
        ];
        {B6, B5, B3, B2, B1} =:= {0, 0, 1, 1, 1} -> [
            B9 * 4 + B8 * 2 + B0,
            8 + B4,
            8 + B7
        ];
        {B6, B5, B3, B2, B1} =:= {0, 1, 1, 1, 1} -> [
            8 + B0,
            B9 * 4 + B8 * 2 + B4,
            8 + B7
        ];
        {B6, B5, B3, B2, B1} =:= {1, 0, 1, 1, 1} -> [
            8 + B0,
            8 + B4,
            B9 * 4 + B8 * 2 + B7
        ];
        {B6, B5, B3, B2, B1} =:= {1, 1, 1, 1, 1} -> [
            8 + B0,
            8 + B4,
            8 + B7
        ]
        end,
    lists:nth(3, D) * 100 + lists:nth(2, D) * 10 + lists:nth(1, D).

-spec calc_significand_segments(integer(), list()) -> integer().
calc_significand_segments(Prefix, []) -> Prefix;
calc_significand_segments(Prefix, Segments) ->
    [Dpd | Tail] = Segments,
    calc_significand_segments(Prefix * 1000 + dpd_to_int(Dpd), Tail).

-spec segments_bits(integer(), integer(), list()) -> list().
segments_bits(_, 0, Segments) -> Segments;
segments_bits(DpdBits, NumSegments, Segments) ->
    NextSegment = DpdBits band 2#1111111111,
    segments_bits(DpdBits bsr 10, NumSegments - 1, [NextSegment | Segments]).

-spec calc_significand(integer(), binary(), integer()) -> integer().
calc_significand(Prefix, DpdBits, NumBits) ->
    %% https://en.wikipedia.org/wiki/Decimal128_floating-point_format#Densely_packed_decimal_significand_field
    NumSegments = NumBits div 10,
    Segments = segments_bits(DpdBits, NumSegments, []),
    calc_significand_segments(Prefix, Segments).

-spec decimal128_to_sign_digits_exponent(binary()) -> {integer(), integer(), integer()} | list().
decimal128_to_sign_digits_exponent(Bin) ->
    %% https://en.wikipedia.org/wiki/Decimal128_floating-point_format
    <<Sign:1, Combination:17, DpdBits:110>> = Bin,
    if
        Combination band 2#11111000000000000 =:= 2#11111000000000000, Sign =:= 0 -> "NaN";
        Combination band 2#11111000000000000 =:= 2#11111000000000000, Sign =:= 1 -> "-NaN";
        Combination band 2#11111000000000000 =:= 2#11110000000000000, Sign =:= 0 -> "-Infinity";
        Combination band 2#11111000000000000 =:= 2#11110000000000000, Sign =:= 1 -> "Infinity";
        true ->
            Exponent = if
                Combination band 2#11000000000000000 =:= 2#00000000000000000 ->
                    2#00000000000000 + Combination band 2#111111111111;
                Combination band 2#11000000000000000 =:= 2#01000000000000000 ->
                    2#01000000000000 + Combination band 2#111111111111;
                Combination band 2#11000000000000000 =:= 2#10000000000000000 ->
                    2#10000000000000 + Combination band 2#111111111111;
                Combination band 2#11110000000000000 =:= 2#11000000000000000 ->
                    2#00000000000000 + Combination band 2#111111111111;
                Combination band 2#11110000000000000 =:= 2#11010000000000000 ->
                    2#01000000000000 + Combination band 2#111111111111;
                Combination band 2#11110000000000000 =:= 2#11100000000000000 ->
                    2#10000000000000 + Combination band 2#111111111111
                end - 6176,
            Prefix = if
                Combination band 2#11000000000000000 =:= 2#00000000000000000 ->
                    (Combination bsr 12) band 2#111;
                Combination band 2#11000000000000000 =:= 2#01000000000000000 ->
                    (Combination bsr 12) band 2#111;
                Combination band 2#11000000000000000 =:= 2#10000000000000000 ->
                    (Combination bsr 12) band 2#111;
                Combination band 2#11110000000000000 =:= 2#11000000000000000 ->
                    8 + (Combination bsr 12) band 2#1;
                Combination band 2#11110000000000000 =:= 2#11010000000000000 ->
                    8 + (Combination bsr 12) band 2#1;
                Combination band 2#11110000000000000 =:= 2#11100000000000000 ->
                    8 + (Combination bsr 12) band 2#1
                end,
            Digits = calc_significand(Prefix, DpdBits, 110),
            {Sign, Digits, Exponent}
    end.

%% ------------------------------------------------------------------------------------

-spec decimal_fixed_to_decimal(binary(), integer()) -> list().
decimal_fixed_to_decimal(Bin, Scale) ->
    case decimal128_to_sign_digits_exponent(Bin) of
    {Sign, V, _} -> efirebirdsql_conv:parse_number(Sign, V, Scale);
    V -> V
    end.


-spec decimal64_to_decimal(binary()) -> list().
decimal64_to_decimal(Bin) ->
    %% https://en.wikipedia.org/wiki/Decimal64_floating-point_format
    <<Sign:1, Combination:5, BaseExponent:8, DpdBits:50>> = Bin,
    if
        Combination =:= 2#11111, Sign =:= 0 -> "NaN";
        Combination =:= 2#11111, Sign =:= 1 -> "-NaN";
        Combination =:= 2#11110, Sign =:= 0 -> "Infinity";
        Combination =:= 2#11110, Sign =:= 1 -> "-Infinity";
        true ->
            Exponent = if
                Combination band 2#11000 =:= 2#00000 ->
                    2#0000000000 + BaseExponent;
                Combination band 2#11000 =:= 2#01000 ->
                    2#0100000000 + BaseExponent;
                Combination band 2#11000 =:= 2#10000 ->
                    2#1000000000 + BaseExponent;
                Combination band 2#11000 =:= 2#11000 ->
                    2#0000000000 + BaseExponent;
                Combination band 2#11000 =:= 2#11010 ->
                    2#0100000000 + BaseExponent;
                Combination band 2#11000 =:= 2#11100 ->
                    2#1000000000 + BaseExponent
            end - 398,
            Prefix = if
                Combination band 2#11000 =:= 2#00000 ->
                    Combination band 2#111;
                Combination band 2#11000 =:= 2#01000 ->
                    Combination band 2#111;
                Combination band 2#11000 =:= 2#10000 ->
                    Combination band 2#111;
                Combination band 2#11000 =:= 2#11000 ->
                    8 + (Combination band 2#1);
                Combination band 2#11000 =:= 2#11100 ->
                    8 + (Combination band 2#1);
                Combination band 2#11000 =:= 2#11100 ->
                    8 + (Combination band 2#1)
            end,
            Digits = calc_significand(Prefix, DpdBits, 50),
            efirebirdsql_conv:parse_number(Sign, Digits, Exponent)
    end.

-spec decimal128_to_decimal(binary()) -> list().
decimal128_to_decimal(Bin) ->
    case decimal128_to_sign_digits_exponent(Bin) of
    {Sign, V, Scale} -> efirebirdsql_conv:parse_number(Sign, V, Scale);
    V -> V
    end.
