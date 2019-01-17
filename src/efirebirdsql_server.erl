%%% The MIT License (MIT)

%%% Copyright (c) 2016-2018 Hajime Nakagami<nakagami@gmail.com>

-module(efirebirdsql_server).

-behavior(gen_server).

-export([start_link/0, get_parameter/2]).
-export([handle_call/3, handle_cast/2, handle_info/2]).
-export([init/1, code_change/3, terminate/2]).

-include("efirebirdsql.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% -- client interface --
-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_server:start_link(?MODULE, [], []).

get_parameter(C, Name) when is_list(Name) ->
    gen_server:call(C, {get_parameter, list_to_binary(Name)}, infinity);
get_parameter(C, Name) when is_list(Name) ->
    gen_server:call(C, {get_parameter, Name}, infinity).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% -- gen_server implementation --

init([]) ->
    {ok, #state{}}.

handle_call({connect, Host, Username, Password, Database, Options}, _From, State) ->
    case efirebirdsql_protocol:connect(Host, Username, Password, Database, Options, State) of
    {ok, State2} ->
        {reply, ok, State2};
    {error, Reason, State2} ->
        {reply, {error, Reason}, State2}
    end;
handle_call({transaction, Options}, _From, State) ->
    AutoCommit = proplists:get_value(auto_commit, Options, true),
    case efirebirdsql_protocol:begin_transaction(AutoCommit, State) of
    {ok, S2} -> {reply, ok, S2};
    {error, Reason, S2} -> {reply, Reason, S2}
    end;
handle_call(commit, _From, State) ->
    case efirebirdsql_protocol:commit(State) of
    {ok, S2} -> {reply, ok, S2};
    {error, Reason, S2} -> {reply, Reason, S2}
    end;
handle_call(rollback, _From, State) ->
    case efirebirdsql_protocol:rollback(State) of
    {ok, S2} -> {reply, ok, S2};
    {error, Reason, S2} -> {reply, Reason, S2}
    end;

handle_call(detach, _From, State) ->
    case efirebirdsql_protocol:detach(State) of
    {ok, S2} -> {reply, ok, S2};
    {error, Reason, S2} -> {reply, Reason, S2}
    end;
handle_call({prepare, Sql}, _From, State) ->
    case efirebirdsql_protocol:prepare_statement(Sql, State) of
    {ok, S2} -> {reply, ok, S2};
    {error, Reason, S2} -> {reply, Reason, S2}
    end;
handle_call({execute, Params}, _From, State) ->
    case efirebirdsql_protocol:execute(State, Params) of
    {ok, S2} -> {reply, ok, S2};
    {error, Reason, S2} -> {reply, Reason, S2}
    end;
handle_call(fetchone, _From, State) ->
    {ok, ConvertedRow, S2} = efirebirdsql_protocol:fetchone(State),
    {reply, {ok, ConvertedRow}, S2};
handle_call(fetchall, _From, State) ->
    {ok, Rows, S2} = efirebirdsql_protocol:fetchall(State),
    {reply, {ok, Rows}, S2};
handle_call(description, _From, State) ->
    case State#state.stmt_type of
    isc_info_sql_stmt_select
        -> {reply, efirebirdsql_protocol:description(State), State};
    _
        -> {reply, no_result, State}
    end;
handle_call({get_parameter, Name}, _From, State) ->
    Value1 = case lists:keysearch(Name, 1, State#state.parameters) of
        {value, {Name, Value}} -> Value;
        false                  -> undefined
        end,
    {reply, {ok, Value1}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({inet_reply, _, ok}, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
