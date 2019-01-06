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
    %% isc_tpb_version3,isc_tpb_write,isc_tpb_wait,isc_tpb_read_committed,isc_tpb_no_rec_version
    Tpb = [3, 9, 6, 15, 18],
    R = efirebirdsql_protocol:begin_transaction(
        lists:flatten(Tpb, if AutoCommit =:= true -> [16]; true -> [] end), State),
    case R of
        {ok, TransHandle} ->
            {reply, ok, State#state{trans_handle=TransHandle}};
        {error, _Reason} ->
            {reply, R, State}
    end;
handle_call(commit, _From, State) ->
    {reply, efirebirdsql_protocol:commit(State), State};
handle_call(rollback, _From, State) ->
    {reply, efirebirdsql_protocol:rollback(State), State};
handle_call(detach, _From, State) ->
    {reply, efirebirdsql_protocol:detach(State), State};
handle_call({prepare, Sql}, _From, State) ->
    case R = efirebirdsql_protocol:prepare_statement(Sql, State) of
        {ok, StmtType, XSqlVars} ->
            {reply, ok, State#state{stmt_type=StmtType, xsqlvars=XSqlVars}};
        {error, _Reason} ->
            {reply, R, State}
    end;
handle_call({execute, Params}, _From, State) ->
    case State#state.stmt_type of
        isc_info_sql_stmt_exec_procedure ->
            {ok, Row} = efirebirdsql_protocol:execute2(Params, State),
            {reply, ok, State#state{rows=[Row]}};
        _ ->
            ok = efirebirdsql_protocol:execute(Params, State),
            case State#state.stmt_type of
                isc_info_sql_stmt_select ->
                    {ok, Rows} = efirebirdsql_protocol:fetchrows(State),
                    efirebirdsql_protocol:free_statement(State),
                    {reply, ok, State#state{rows=Rows}};
                _ ->
                    {reply, ok, State}
            end
    end;
handle_call(fetchone, _From, State) ->
    [R | Rest] = State#state.rows,
    ConvertedRow = efirebirdsql_op:convert_row(
        State, State#state.xsqlvars, R
    ),
    {reply, {ok, ConvertedRow}, State#state{rows=Rest}};
handle_call(fetchall, _From, State) ->
    ConvertedRows = [efirebirdsql_op:convert_row(
        State, State#state.xsqlvars, R
    ) || R <- State#state.rows],
    {reply, {ok, ConvertedRows}, State};
handle_call(description, _From, State) ->
    case State#state.stmt_type of
        isc_info_sql_stmt_select
            -> {reply, efirebirdsql_protocol:description(State#state.xsqlvars), State};
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
