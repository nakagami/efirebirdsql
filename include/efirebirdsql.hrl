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
    user,
    password,
    client_private,
    client_public,
    auth_data,
    auth_plugin,
    wire_crypt,
    read_state,     % RC4 crypto stream state
    write_state,    % RC4 crypto stream state
    db_handle,
    trans_handle,
    accept_version,
    timezone,
    timezone_name_by_id,
    timezone_id_by_name
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
    statement
}).

-type state() :: #state{}.
