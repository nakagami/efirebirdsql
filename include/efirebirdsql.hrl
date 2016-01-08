%%% The MIT License (MIT)
%%% Copyright (c) 2016 Hajime Nakagami<nakagami@gmail.com>

-record(column, {
    name :: binary(),
    seq :: pos_integer(),
    type :: atom(),
    sub_type :: pos_integer(),
    scale :: -1 | pos_integer(),
    length :: -1 | pos_integer(),
    null_ind :: true | false
}).

-record(statement, {
    handle :: pos_integer(),
    columns :: [#column{}],
    types :: [atom()]
}).

-record(error, {
    code :: binary()
}).
