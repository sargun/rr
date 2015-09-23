-module(rr_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

-define(CONSISTENT_TYPE, <<"cp">>).
setup_bucket() ->
    case riak_core_bucket_type:status(?CONSISTENT_TYPE) of
        ready ->
            riak_core_bucket_type:activate(?CONSISTENT_TYPE);
        created ->
            timer:sleep(100),
            setup_bucket();
        undefined ->
            riak_core_bucket_type:create(?CONSISTENT_TYPE, [{consistent, true}]),
            setup_bucket();
        active ->
            ok
    end.

start(_StartType, _StartArgs) ->
   setup_bucket(),
    {ok, _} = ranch:start_listener(tcp_echo, 1,
        ranch_tcp, [{port, 6379}], rr_protocol, [{redis_handler, rr_handler}]),
    %redis_protocol:start(6379, rr_handler),
    rr_sup:start_link().

stop(_State) ->
    ok.
