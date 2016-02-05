=============
efirebirdsql
=============

Erlang Firebird client library.

Example
-----------

::

    {ok, C} = efirebirdsql:connect(
        "server", "username", "password", "/path/to/database", []),
    ok = efirebirdsql:execute(C, <<"select * from foo">>),
    {ok, Results} = efirebirdsql:fetchall(C).

See also test/efirebirdsql_tests.erl
