%%% The MIT License (MIT)
%%% Copyright (c) 2016 Hajime Nakagami<nakagami@gmail.com>

-record(column, {
    name :: binary() | undefined,
    seq :: pos_integer() | undefined,
    type :: atom() | undefined,
    scale :: -1 | pos_integer() | undefined,
    length :: -1 | pos_integer() | undefined,
    null_ind :: true | false |undefined
}).

-type column() :: #column{}.

-record(conn, {
    sock,
    user :: string(),
    password :: string(),
    client_private :: integer(),
    client_public :: integer(),
    auth_data :: string() | undefined,
    auth_plugin :: string(),
    wire_crypt :: boolean(),
    read_state,     % RC4 crypto stream state
    write_state,    % RC4 crypto stream state
    db_handle,
    trans_handle,
    accept_version,
    timezone :: binary() | nil,
    timezone_name_by_id :: map() | undefined,
    timezone_id_by_name :: map() | undefined
}).

-type conn() :: #conn{}.

-record(stmt, {
    stmt_handle,
    stmt_type,
    xsqlvars = [],
    rows = [],      %% segment rows values
    more_data = false,  %% has more rows
    closed = true
}).

-type stmt() :: #stmt{}.

-record(state, {
    parameters = [],
    connection,
    statement,
    error_no,
    error_message
}).

-type state() :: #state{}.
