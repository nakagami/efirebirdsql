%%% The MIT License (MIT)
%%% Copyright (c) 2016-2022 Hajime Nakagami<nakagami@gmail.com>

-module(efirebirdsql_op_tests).

-include_lib("eunit/include/eunit.hrl").

process_dpb_test() ->
    %% nil/nil (the default): nothing is added, so the attach DPB is unchanged.
    ?assertEqual([], efirebirdsql_op:process_dpb(nil, nil)),
    %% an empty process name is treated as unset.
    ?assertEqual([], efirebirdsql_op:process_dpb("", nil)),
    %% isc_dpb_process_name (74) + length + name bytes.
    ?assertEqual([74, 7, $l, $y, $n, $x, $w, $e, $b],
                 efirebirdsql_op:process_dpb("lynxweb", nil)),
    %% isc_dpb_process_id (71) + length 4 + pid as a 32-bit little-endian integer.
    ?assertEqual([71, 4, 12, 0, 0, 0],
                 efirebirdsql_op:process_dpb(nil, 12)),
    %% values > 255 span the little-endian bytes (300 = 16#012C).
    ?assertEqual([71, 4, 16#2C, 16#01, 0, 0],
                 efirebirdsql_op:process_dpb(nil, 300)),
    %% both set: the process_name items are followed by the process_id items.
    ?assertEqual([74, 7, $l, $y, $n, $x, $w, $e, $b, 71, 4, 12, 0, 0, 0],
                 efirebirdsql_op:process_dpb("lynxweb", 12)).
