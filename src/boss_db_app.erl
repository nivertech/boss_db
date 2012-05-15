%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% File: boss_db_app.erl
%% Date: 15/05/2012
%%
%% @doc OTP application for boss_db
%%
%% @author Ori Bar     <ori.bar@nivertech.com>
%% @author Zvi Avraham <zvi@nivertech.com>
%% @copyright 2012 Nivertech (Nywhere Tech Ltd)
%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-module(boss_db_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

-define(APPLICATION, router).

%%%===================================================================
%%% Application callbacks
%%%===================================================================
%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called whenever an application is started using
%% application:start/[1,2], and should start the processes of the
%% application. If the application is structured according to the OTP
%% design principles as a supervision tree, this means starting the
%% top supervisor of the tree.
%%
%% @end
%%--------------------------------------------------------------------
-spec start(StartType::normal|{takeover|failover, _Node}, StartArgs::term()) -> 
                   {ok, pid()} | {error, term()}.
start(_StartType, _StartArgs) ->
    {ok, Keys} = application:get_env(boss_db_params),
    boss_db:start(Keys), 
    {ok, Keys2} = application:get_env(boss_news_params),
    boss_news:start(Keys2). % TODO: since this application wraps both boss_db and boss_news, it returns the pid
                            % of only one of them. Therefore, stop won't work
    

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called whenever an application has stopped. It
%% is intended to be the opposite of Module:start/2 and should do
%% any necessary cleaning up. The return value is ignored.
%%
%% @end
%%--------------------------------------------------------------------
-spec stop(term()) -> ok.
stop(_State) ->
    ok.
                  
