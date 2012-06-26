%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% File    : boss_db_adapter_dynamodb.erl
%%% Created : 14/05/2012
%%%
%%% @doc Adapter for amazon DynamoDB using the ddb library
%%%
%%% @author Ori Bar <ori.bar@nivertech.com>
%%% @author Zvi Avraham <zvi@nivertech.com>
%%% @copyright 2012 Nivertech (Nywhere Tech Ltd)
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-module(boss_db_adapter_dynamodb).
-behaviour(boss_db_adapter).
-export([init/1, terminate/1, start/1, stop/0, find/2, find/7]).
-export([count/3, counter/2, incr/2, incr/3, delete/2, save_record/2]).
-export([push/2, pop/2]).

% -define(PREFIX, "").
-define(RENEW_ERROR_INTERVAL_SEC, 300). % if error occurs when trying to get token, retry until we got one

-record(state, {
            renew_pid::pid(),
            eventually_consistent::[binary()]
        }).

start(_Options) ->
    inets:start(),
    ssl:start(),
    lager:start(),
    ibrowse:start(),
    ok.

stop() ->
    ok.

init(Options) ->
    AKI = proplists:get_value(access, Options, os:getenv("AWS_ACCESS_KEY_ID")),
    SAK = proplists:get_value(secret, Options, os:getenv("AWS_SECRET_ACCESS_KEY")),
    DurationInSec = 3600, % commented since ddb TRIES to auto renew token (search for 'expired_token' in ddb.erl) % proplists:get_value(duration_in_secs, Options, 3600),
    case {AKI,SAK} of
        {false, _} ->
            {error, missing_aki};
        {_, false} ->
            {error, missing_sak};
        _ ->
            ddb_iam:credentials(AKI, SAK), % TODO: only 1 credentials can be used simul.
            {ok, Key, Secret, Tokens} = ddb_iam:token(DurationInSec), 
            ddb:credentials(Key, Secret, Tokens), % TODO: only 1 credentials can be used simul.
            {ok, 
             #state{
               renew_pid=spawn(fun() -> renew_token(DurationInSec div 2, DurationInSec) end),
               eventually_consistent=proplists:get_value(ddb_eventually_consistent_tables, Options, [])
              }}
    end.

renew_token(IntervalSec, DurationInSec) ->
    error_logger:info_msg("renew_token: Renewing token"),
    case ddb_iam:token(DurationInSec) of
        {ok, Key, Secret, Tokens} ->
            error_logger:info_msg("renew_token: Token renewed"),
            ddb:credentials(Key, Secret, Tokens), % TODO: only 1 credentials can be used simul.
            timer:sleep(IntervalSec * 1000),
            renew_token(IntervalSec, DurationInSec);
        _ ->
            error_logger:warn("renew_token: Error while reneweing token, will try again in a ~p seconds", [?RENEW_ERROR_INTERVAL_SEC]),
            timer:sleep(?RENEW_ERROR_INTERVAL_SEC * 1000),
            renew_token(IntervalSec, DurationInSec)
    end.
            
terminate(#state{renew_pid=Pid}) ->
    exit(Pid, kill). % TODO: can we destroy the token?

find(#state{eventually_consistent=EventuallyConsistent}, Id) ->
    {Type, TableName, TableId} = infer_type_from_id(Id),
    %% TODO: can Id be a number?
    ConsistentRead = not lists:member(TableName, EventuallyConsistent),
    Ret = ddb:get(TableName, ddb:key_value(TableId, 'string'), [{<<"ConsistentRead">>, ConsistentRead}]),
    case Ret of
        {ok, Result} ->
            case proplists:get_value(<<"Item">>, Result) of
                undefined ->
                    undefined;
                PL ->
                    Record = 
                        apply(Type, 
                              new, 
                              lists:map(fun(AttrName) ->
                                                %% TODO: currently, only "single string" dynamodb is supported
                                                Val = case proplists:get_value(
                                                             list_to_binary(atom_to_list(AttrName)), PL) of
                                                          [{<<"S">>, V}] ->
                                                              remove_zero(V);
                                                          [{<<"SS">>, Vs}] ->
                                                              {string_set, sets:from_list([ remove_zero(V) || 
                                                                                              V <- Vs ])};
                                                          [{<<"N">>, V}] ->
                                                              binary_to_number(V);
                                                          [{<<"NS">>, Vs}] ->
                                                              {number_set, sets:from_list([ binary_to_number(V) || 
                                                                                              V <- Vs])};
                                                          undefined ->
                                                              undefined
                                                      end,
                                                AttrType = undefined, % TODO: currently, we ignore boss types
                                                boss_record_lib:convert_value_to_type(Val, AttrType)
                                        end, boss_record_lib:attribute_names(Type))),
                    Record:set(id, Id)
            end;
        Error ->
            Error
    end.

find(_Conn, _Type, _Conditions, _Max, _Skip, _Sort, _SortOrder) ->
    throw(notimplemented). % TODO: implement a stub that will pass tests

count(_Conn, _Type, _Conditions) ->
    throw(notimplemented). % TODO: implement a stub that will pass tests

counter(_Conn, _Id) ->
    throw(notimplemented). % TODO: not implemented

incr(Conn, Id) ->
    incr(Conn, Id, 1).
incr(_Conn, _Id, _Count) ->
    throw(notimplemented). % TODO: not implemented

delete(_, Id) ->
    {_Type, TableName, TableId} = infer_type_from_id(Id),
    case ddb:delete(TableName, ddb:key_value(TableId, 'string')) of
        {ok, _} ->
            ok;
        Error ->
            Error
    end.

save_record(_, Record) when is_tuple(Record) ->
    %% TODO: automatic id not supported
    case Record:id() of
        id -> throw(notimplemented);
        _  -> ok
    end,
    Type = strip_prefix(element(1, Record)),
    Table = list_to_binary(
              inflector:pluralize(
                atom_to_list(Type))),
    %% TODO: currently only dynamodb single string type supported
    Id = list_to_binary(lists:nthtail(string:chr(Record:id(), $-), Record:id())),
    PropListWithoutId = [property_to_ddb(K,V) || {K,V} <- Record:attributes(), K =/= id],
    PropList = [{id, Id, 'string'}|PropListWithoutId],
    ddb:put(Table, PropList),
    {ok, Record}.

% These 2 functions are not part of the behaviour but are required for
% tests to pass
push(_Conn, _Depth) -> 
    throw(notimplemented),
    ok.

pop(_Conn, _Depth) -> 
    throw(notimplemented),
    ok.

%%%%%%%%%%%%%%%%%%%% internal APIs %%%%%%%%%%%%%%%%
-ifndef(PREFIX).
strip_prefix(Type) ->
    Type.
add_prefix(Type) ->
    Type.
-else.
strip_prefix(Type) ->
    list_to_atom(lists:nthtail(atom_to_list(Type), ?PREFIX_LENGTH)).
add_prefix(Type) ->
    [?PREFIX, inflector:pluralize(Type)].
-endif.

infer_type_from_id(Id) when is_list(Id) ->
    {Type, "-" ++ TableId} = lists:split(string:chr(Id, $-) - 1, Id),
    {list_to_atom(Type), list_to_binary(add_prefix(inflector:pluralize(Type))), list_to_binary(TableId)}.

remove_zero(<<0,X/bytes>>) -> X;
remove_zero(X)             -> X.
add_zero("") -> "\0";
add_zero([0|X]) -> [0,0|X];
add_zero(<<>>) -> <<0>>;
add_zero(<<0, X/bytes>>) -> <<0, 0, X/bytes>>;
add_zero(X) -> X.

to_binary(L) when is_list(L) -> list_to_binary(L);
to_binary(B) when is_binary(B) -> B.

property_to_ddb(K,V) when is_number(V) ->
    {list_to_binary(atom_to_list(K)), number_to_binary(V), 'number'};
property_to_ddb(K,V) when is_list(V) ; is_binary(V) ->
    {list_to_binary(atom_to_list(K)), to_binary(add_zero(V)), 'string'};
property_to_ddb(K, {number_set, S}) ->
    {list_to_binary(atom_to_list(K)), sets:fold(fun(X, Acc) -> [number_to_binary(X)|Acc] end, [], S), ['number']};
property_to_ddb(K, {string_set, S}) ->
    {list_to_binary(atom_to_list(K)), sets:fold(fun(X, Acc) -> [to_binary(add_zero(X))|Acc] end, [], S), ['string']}.

binary_to_number(B) ->
    NumStr = binary_to_list(B),
    try list_to_integer(NumStr) of
        X -> X
    catch 
        error:badarg -> list_to_float(NumStr)
    end.

number_to_binary(N) when is_integer(N) -> list_to_binary(integer_to_list(N));
number_to_binary(N) when is_float(N) -> list_to_binary(float_to_list(N)).

% boss_db:start([{adapter, dynamodb}]).
% boss_news:start().
% boss_record_compiler:compile(filename:join(["../", "priv", "test_models", "boss_db_test_parent_model.erl"])).
% X = boss_db_test_parent_model:new("boss_db_test_parent_model-id1", "foo"). %% ID must start with module name
% X:save().
% Y = boss_db:find(X:id()).
% Y. % Y should be the same as X
% boss_db:delete(X:id()).
% Z = boss_db:find(X:id()).
% Z.
