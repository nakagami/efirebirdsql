=============
efirebirdsql
=============

Erlang Firebird client library.

Examples
-----------

Simple query and fetch results::

    {ok, C} = efirebirdsql:connect(
        "server", "username", "password", "/path/to/database", []),
    ok = efirebirdsql:execute(C, <<"select * from foo">>),
    {ok, Results} = efirebirdsql:fetchall(C).

Fetch one by one::

    ok = efirebirdsql:execute(C, <<"select * from foo">>),
    {ok, R1} = efirebirdsql:fetchone(C),
    {ok, R2} = efirebirdsql:fetchone(C),
    {ok, R3} = efirebirdsql:fetchone(C).

Separate start_link() and connect()::

    C = start_link(),
    ok = efirebirdsql:connect(C,
        "server", "username", "password", "/path/to/database", []),

See also test/efirebirdsql_tests.erl

Server Configuretion with Firebird 3
--------------------------------------

This driver connect with wire protocol version 10 (Interbase 6),
So if you use with Firebird 3.x server, you need to modify firebird.conf like below.

::

   AuthServer = Srp, Legacy_Auth
   WireCrypt = Enabled

