%%% The MIT License (MIT)
%%% Copyright (c) 2016 Hajime Nakagami<nakagami@gmail.com>

-module(efirebirdsql_conv).

-export([byte2/1, byte4/2, byte4/1,
    list_to_xdr_string/1, list_to_xdr_bytes/1 ]).

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

