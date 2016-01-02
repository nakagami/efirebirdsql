%%% The MIT License (MIT)
%%% Copyright (c) 2016 Hajime Nakagami<nakagami@gmail.com>

-define(ISOLATION_LEVEL_READ_COMMITED_LEGACY, 0).
-define(ISOLATION_LEVEL_READ_COMMITED, 1).
-define(ISOLATION_LEVEL_REPEATABLE_READ, 2).
-define(ISOLATION_LEVEL_SERIALIZABLE, 3).
-define(ISOLATION_LEVEL_READ_COMMITED_RO, 4).

-record(column, {
    name :: binary(),
    type :: atom(),
    size :: -1 | pos_integer(),
    modifier :: -1 | pos_integer()
}).

-record(statement, {
    handle :: binary(),
    trans_handle :: pos_integer(),
    name :: string(),
    columns :: [#column{}],
    types :: [atom()]
}).

-record(error, {
    code :: binary()
}).
