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
    DurationInSec = 129600, % commented since ddb auto renews token (search for 'expired_token' in ddb.erl) % proplists:get_value(duration_in_secs, Options, 3600),
    case {AKI,SAK} of
        {false, _} ->
            {error, missing_aki};
        {_, false} ->
            {error, missing_sak};
        _ ->
            ddb_iam:credentials(AKI, SAK), % TODO: only 1 credentials can be used simul.
            {ok, Key, Secret, Tokens} = ddb_iam:token(DurationInSec), 
            ddb:credentials(Key, Secret, Tokens), % TODO: only 1 credentials can be used simul.
            {ok, dummy}
    end.
            
terminate(_) ->
    ok. % TODO: can we destroy the token?

find(_, Id) ->
    {Type, TableName, TableId} = infer_type_from_id(Id),
    %% TODO: can Id be a number?
    Ret = ddb:get(TableName, ddb:key_value(TableId, 'string'), [{<<"ConsistentRead">>, true}]),
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
                                                              V;
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
    []. % TODO: implement a stub that will pass tests

count(_Conn, _Type, _Conditions) ->
    0. % TODO: implement a stub that will pass tests

counter(_Conn, _Id) ->
    {error, notimplemented}. % TODO: not implemented

incr(Conn, Id) ->
    incr(Conn, Id, 1).
incr(_Conn, _Id, _Count) ->
    {error, notimplemented}. % TODO: not implemented

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
    Type = strip_prefix(element(1, Record)),
    Table = list_to_binary(
              inflector:pluralize(
                atom_to_list(Type))),
    %% TODO: currently only dynamodb single string type supported
    Id = list_to_binary(lists:nthtail(string:chr(Record:id(), $-), Record:id())),
    PropListWithoutId = [{list_to_binary(atom_to_list(K)), list_to_binary(V), 'string'} || 
                            {K,V} <- Record:attributes(), K =/= id],
    PropList = [{id, Id, 'string'}|PropListWithoutId],
    ddb:put(Table, PropList),
    {ok, Record}.

% These 2 functions are not part of the behaviour but are required for
% tests to pass
push(_Conn, _Depth) -> ok.

pop(_Conn, _Depth) -> ok.

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
    [Type, TableId] = string:tokens(Id, "-"),
    {list_to_atom(Type), list_to_binary(add_prefix(inflector:pluralize(Type))), list_to_binary(TableId)}.

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
