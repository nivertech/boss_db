-module(boss_cache_adapter_ets).
-behaviour(boss_cache_adapter).

-export([init/1, start/0, start/1, stop/1]).
-export([get/3, set/5, delete/3]).

-define(NAME, boss_cache_ets).

start() ->
    start([]).

start(Options) ->
    ets:new(?NAME, [set, public, named_table]),
    TTL = proplists:get_value(ttl, Options, 60),
    spawn(fun() -> del_loop(TTL) end).

stop(_Tab) ->
    ets:delete(?NAME).

init(Options) ->
    {ok, ?NAME}.

get(_Tab, Prefix, Key) ->
    case ets:lookup(?NAME, {Prefix, Key}) of
        [] ->
            undefined;
        [{{Prefix, Key}, Value}] ->
            Value
    end.

set(_Tab, Prefix, Key, Val, TTL) ->
    ets:insert(?NAME, {{Prefix, Key}, Val}).

delete(_Tab, Prefix, Key) ->
    ets:delete(?NAME, {Prefix, Key}).

%% -------------- internal functions ----------------

del_loop(TTL) ->
    timer:sleep(TTL * 1000),
    ets:delete_all_objects(?NAME), % If stop() destroyed the table, badarg will stop this process
    del_loop(TTL).
