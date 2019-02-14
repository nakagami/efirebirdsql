%%% The MIT License (MIT)
%%% Copyright (c) 2016 Hajime Nakagami<nakagami@gmail.com>

-record(column, {
    name :: binary(),
    seq :: pos_integer(),
    type :: atom(),
    scale :: -1 | pos_integer(),
    length :: -1 | pos_integer(),
    null_ind :: true | false
}).

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
    timezone_data
}).

-record(stmt, {
    stmt_handle,
    stmt_type,
    xsqlvars = [],
    rows = []
}).

-record(state, {
    parameters = [],
    connection,
    statement
}).

