%%% The MIT License (MIT)
%%% Copyright (c) 2016-2022 Hajime Nakagami<nakagami@gmail.com>

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
    charset :: atom(),
    auto_commit :: boolean(),
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
    timezone :: string() | nil
}).

-type conn() :: #conn{}.

-record(stmt, {
    sql :: binary() | undefined,
    stmt_handle = nil,
    stmt_type = nil,
    xsqlvars = [],
    rows = nil,      %% segment rows values
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
