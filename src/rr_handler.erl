-module(rr_handler).

-export([handle/3, handle/2]).
-export([init/0]).
-define(CONSISTENT_TYPE, <<"cp">>).
-define(CONSISTENT_BUCKET, <<"bucket">>).
-record(state, {riak_client}).

-include_lib("riak_kv/include/riak_object.hrl").




handle(Connection, State = #state{riak_client = {_Module, [Node, _ClientId]}}, [<<"LLEN">>, Key]) ->
    BKey = {{?CONSISTENT_TYPE, ?CONSISTENT_BUCKET}, Key},
    Ensemble = riak_client:ensemble(BKey),
    Timeout = 61000,
    ModFun =
        fun(_Vsn, {list, List}) ->
                {reply_noreplicate, {ok, length(List)}};
            (_Vsn, _Value) ->
                {reply_noreplicate, {error, wrong_datatype}}
        end,
    Default = {list, []},
    Result = riak_ensemble_peer:kmodify(Node, Ensemble, BKey, ModFun, Default, Timeout),
    case Result of
        {reply, {ok, Count}} ->
            ok = rr_protocol:answer(Connection, Count)
    end,
    %State1 = orddict:store(Key, {kv, Value}, State),
    {ok, State};
handle(Connection, State = #state{riak_client = {_Module, [Node, _ClientId]}}, [<<"LPUSH">>, Key|Values]) ->
    BKey = {{?CONSISTENT_TYPE, ?CONSISTENT_BUCKET}, Key},
    Ensemble = riak_client:ensemble(BKey),
    Timeout = 61000,
    ModFun =
        fun(_Vsn, {list, OldList}) ->
            {reply, {ok, length(Values)}, {list, lists:reverse(Values) ++ OldList}};
            (_Vsn, _Value) ->
                {reply_noreplicate, {error, wrong_datatype}}
        end,
    Default = {list, []},
    Result = riak_ensemble_peer:kmodify(Node, Ensemble, BKey, ModFun, Default, Timeout),
    case Result of
        {reply, {ok, Count}} ->
            ok = rr_protocol:answer(Connection, Count)
    end,
    %State1 = orddict:store(Key, {kv, Value}, State),
    {ok, State};
handle(Connection, State = #state{riak_client = {_Module, [Node, _ClientId]}}, [<<"LTRIM">>, Key, StartBin, EndBin]) ->
    Start = erlang:binary_to_integer(StartBin),
    End = erlang:binary_to_integer(EndBin),
    BKey = {{?CONSISTENT_TYPE, ?CONSISTENT_BUCKET}, Key},
    Ensemble = riak_client:ensemble(BKey),
    Timeout = 61000,


    ModFun =
        fun(_Vsn, {list, OldList}) ->
            Len = length(OldList),
            Slices =
                fun({Idx, Elem}) when Start >= 0 andalso End >= 0 andalso Idx >= Start andalso End >= Idx ->
                        {true, Elem};
                    ({Idx, Elem}) when 0 > Start andalso End >= 0 andalso Idx >= (Len + Start)  andalso End >= Idx ->
                        {true, Elem};
                    ({Idx, Elem}) when Start >= 0 andalso 0 > End andalso Idx >= Start andalso  (End + Len) >= Idx ->
                        {true, Elem};
                    ({Idx, Elem}) when 0 > Start andalso 0 > End andalso Idx >= (Len + Start) andalso (End + Len) >= Idx ->
                        {true, Elem};
                    ({_Idx, _Elem}) ->
                        false
                end,
            IdxList = lists:zip(lists:seq(0, Len - 1), OldList),
            NewList = lists:filtermap(Slices, IdxList),
            {reply, {ok, ok}, {list, NewList}};
            (_Vsn, _Value) ->
                {reply_noreplicate, {error, wrong_datatype}}
        end,
    Default = {list, []},
    _ = riak_ensemble_peer:kmodify(Node, Ensemble, BKey, ModFun, Default, Timeout),
    ok = rr_protocol:answer(Connection, ok),
    %State1 = orddict:store(Key, {kv, Value}, State),
    {ok, State};
handle(Connection, State = #state{riak_client = {_Module, [Node, _ClientId]}}, [<<"LRANGE">>, Key, StartBin, EndBin]) ->
    Start = erlang:binary_to_integer(StartBin),
    End = erlang:binary_to_integer(EndBin),
    BKey = {{?CONSISTENT_TYPE, ?CONSISTENT_BUCKET}, Key},
    Ensemble = riak_client:ensemble(BKey),
    Timeout = 61000,

    ModFun =
        fun(_Vsn, Value) ->
            {reply_noreplicate, Value}
        end,
    Default = {error, not_found},

    case riak_ensemble_peer:kmodify(Node, Ensemble, BKey, ModFun, Default, Timeout) of
        {reply, {error, not_found}} ->
            ok = rr_protocol:answer(Connection, {error, not_found});
        {reply, {list, List}} ->
            Len = length(List),
            Slices =
                fun({Idx, Elem}) when Start >= 0 andalso End >= 0 andalso Idx >= Start andalso End >= Idx ->
                    {true, Elem};
                    ({Idx, Elem}) when 0 > Start andalso End >= 0 andalso Idx >= (Len + Start)  andalso End >= Idx ->
                        {true, Elem};
                    ({Idx, Elem}) when Start >= 0 andalso 0 > End andalso Idx >= Start andalso  (End + Len) >= Idx ->
                        {true, Elem};
                    ({Idx, Elem}) when 0 > Start andalso 0 > End andalso Idx >= (Len + Start) andalso (End + Len) >= Idx ->
                        {true, Elem};
                    ({_Idx, _Elem}) ->
                        false
                end,
            IdxList = lists:zip(lists:seq(0, Len - 1), List),
            NewList = lists:filtermap(Slices, IdxList),
            ok = rr_protocol:answer(Connection, NewList)
    end,
    _ = riak_ensemble_peer:kmodify(Node, Ensemble, BKey, ModFun, Default, Timeout),
    ok = rr_protocol:answer(Connection, ok),
    %State1 = orddict:store(Key, {kv, Value}, State),
    {ok, State};
handle(Connection, State = #state{riak_client = {_Module, [Node, _ClientId]}}, [<<"SET">>, Key, Value]) ->
    BKey = {{?CONSISTENT_TYPE, ?CONSISTENT_BUCKET}, Key},
    Ensemble = riak_client:ensemble(BKey),
    Timeout = 61000,
    ModFun =
        fun(_Vsn, {kv, _}) ->
                {reply, ok2, {kv, Value}};
            (_Vsn, _Value) ->
                {reply_noreplicate, {error, wrong_datatype}}
        end,
    Default = {kv, not_found},
    Result = riak_ensemble_peer:kmodify(Node, Ensemble, BKey, ModFun, Default, Timeout),
    io:format("Result: ~p~n", [Result]),
    %State1 = orddict:store(Key, {kv, Value}, State),

    ok = rr_protocol:answer(Connection, ok),
    {ok, State};

handle(Connection, State = #state{riak_client = {_Module, [Node, _ClientId]}}, [<<"DEL">>, Key]) ->
    BKey = {{?CONSISTENT_TYPE, ?CONSISTENT_BUCKET}, Key},
    Ensemble = riak_client:ensemble(BKey),
    Timeout = 61000,
    _Result = riak_ensemble_peer:kdelete(Node, Ensemble, BKey, Timeout),
    %State1 = orddict:store(Key, {kv, Value}, State),

    ok = rr_protocol:answer(Connection, 1),
    {ok, State};

handle(Connection, State = #state{riak_client = {_Module, [Node, _ClientId]}}, [<<"GET">>, Key]) ->
    BKey = {{?CONSISTENT_TYPE, ?CONSISTENT_BUCKET}, Key},
    Ensemble = riak_client:ensemble(BKey),
    Timeout = 61000,
    ModFun =
        fun(_Vsn, Value) ->
            io:format("Getting value: ~p~n", [Value]),
            {reply_noreplicate, Value}
        end,
    Default = {error, not_found},

    case riak_ensemble_peer:kmodify(Node, Ensemble, BKey, ModFun, Default, Timeout) of
        {reply, {error, not_found}} ->
            ok = rr_protocol:answer(Connection, {error, not_found});
        {reply, {kv, RealValue}} ->
            ok = rr_protocol:answer(Connection, RealValue);
        {reply, {counter, RealValue}} ->
            ok = rr_protocol:answer(Connection, erlang:integer_to_binary(RealValue))
    end,
    {ok, State};

handle(Connection, State = #state{riak_client = {_Module, [Node, _ClientId]}}, [<<"HSET">>, HashName, Key, Value]) ->
    BKey = {{?CONSISTENT_TYPE, ?CONSISTENT_BUCKET}, HashName},
    Ensemble = riak_client:ensemble(BKey),
    Timeout = 61000,
    ModFun =
        fun(_Vsn, {hash, Hash}) ->
            {NewVal, Setcount} = case orddict:find(Key, Hash) of
                         {ok, Value} ->
                             {orddict:store(Key, Value, Hash), 0};
                         error ->
                             {orddict:store(Key, Value, Hash), 1}
                                 end,
            {reply, Setcount, {hash, NewVal}};
            (_Vsn, _Value) ->
                {reply_noreplicate, {error, wrong_datatype}}
        end,
    Default = {hash, orddict:new()},
    {reply, Count} = riak_ensemble_peer:kmodify(Node, Ensemble, BKey, ModFun, Default, Timeout),

    ok = rr_protocol:answer(Connection, Count),
    {ok, State};


handle(Connection, State = #state{riak_client = {_Module, [Node, _ClientId]}}, [<<"HDEL">>, HashName | Keys]) ->
    BKey = {{?CONSISTENT_TYPE, ?CONSISTENT_BUCKET}, HashName},
    Ensemble = riak_client:ensemble(BKey),
    Timeout = 61000,
    ModFun =
        fun(_Vsn, {hash, Hash}) ->
            Newhash = lists:foldl(fun(X, Acc) -> lists:keydelete(X, 1, Acc) end, Hash, Keys),
            Delcount = length(Hash) - length(Newhash),
            case Delcount of
                0 ->
                    {reply_noreplicate, 0};
                _ ->
                    {reply, Delcount, {hash, Newhash}}
            end;
            (_Vsn, _Value) ->
                {reply_noreplicate, {error, wrong_datatype}}
        end,
    Default = {hash, orddict:new()},
    {reply, Count} = riak_ensemble_peer:kmodify(Node, Ensemble, BKey, ModFun, Default, Timeout),

    ok = rr_protocol:answer(Connection, Count),
    {ok, State};



handle(Connection, State = #state{riak_client = {_Module, [Node, _ClientId]}}, [<<"HGETALL">>, HashName]) ->
    BKey = {{?CONSISTENT_TYPE, ?CONSISTENT_BUCKET}, HashName},
    Ensemble = riak_client:ensemble(BKey),
    Timeout = 61000,
    ModFun =
        fun(_Vsn, {hash, Hash}) ->
                {reply_noreplicate, {ok, Hash}};
            (_Vsn, Value) ->
                io:format("Value: ~p~n", [Value]),
                {reply_noreplicate, {error, wrong_datatype}}
        end,
    Default = {hash, orddict:new()},
    case riak_ensemble_peer:kmodify(Node, Ensemble, BKey, ModFun, Default, Timeout) of
        {reply, {ok, HashValue}} ->
            LoL = [[Key, Value] || {Key, Value} <- orddict:to_list(HashValue)],
            ok = rr_protocol:answer(Connection, lists:merge(LoL))
    end,
    {ok, State};

handle(Connection, State = #state{riak_client = {_Module, [Node, _ClientId]}}, [<<"HGET">>, HashName, Key]) ->
    BKey = {{?CONSISTENT_TYPE, ?CONSISTENT_BUCKET}, HashName},
    Ensemble = riak_client:ensemble(BKey),
    Timeout = 61000,
    ModFun =
        fun(_Vsn, {hash, Hash}) ->
            case orddict:find(Key, Hash) of
                {ok, Value} ->
                    {reply_noreplicate, {ok, Value}};
                error ->
                    {reply_noreplicate, {error, not_found}}
            end;
            (_Vsn, Value) ->
                io:format("Value: ~p~n", [Value]),
                {reply_noreplicate, {error, wrong_datatype}}
        end,
    Default = {hash, orddict:new()},
    case riak_ensemble_peer:kmodify(Node, Ensemble, BKey, ModFun, Default, Timeout) of
        {reply, {ok, Value}} ->
            ok = rr_protocol:answer(Connection, Value);
        {reply, {error, not_found}} ->
            ok = rr_protocol:answer(Connection, {error, not_found})
    end,
    {ok, State};

handle(Connection, State = #state{riak_client = {_Module, [Node, _ClientId]}}, [<<"HSETNX">>, HashName, Key, Value]) ->
    BKey = {{?CONSISTENT_TYPE, ?CONSISTENT_BUCKET}, HashName},
    Ensemble = riak_client:ensemble(BKey),
    Timeout = 61000,
    ModFun =
        fun(_Vsn, {hash, Hash}) ->
            io:format("Hash: ~p~n", [Hash]),
            case orddict:find(Key, Hash) of
                {ok, _OldValue} ->
                    {reply_noreplicate, {error, value_existed}};
                error ->
                    {reply, 1, {hash, orddict:store(Key, Value, Hash)}}
            end;
            (_Vsn, _Value) ->
                {reply_noreplicate, {error, wrong_datatype}}
        end,
    Default = {hash, orddict:new()},
    Result = riak_ensemble_peer:kmodify(Node, Ensemble, BKey, ModFun, Default, Timeout),
    io:format("Result: ~p~n", [Result]),
    case Result of
        {reply, {error, value_existed}} ->
            ok = rr_protocol:answer(Connection, 0);
        {reply, Count} ->
            ok = rr_protocol:answer(Connection, Count)
    end,
    {ok, State};



handle(Connection, State = #state{riak_client = {_Module, [Node, _ClientId]}}, [<<"HMSET">>, HashName | KeyValues]) ->
    BKey = {{?CONSISTENT_TYPE, ?CONSISTENT_BUCKET}, HashName},
    Ensemble = riak_client:ensemble(BKey),
    Timeout = 61000,
    IDXs = lists:zip(lists:seq(1, erlang:length(KeyValues)), KeyValues),
    {KeysIDX, ValuesIDX} = lists:partition(fun({X, _}) -> X rem 2 == 1 end, IDXs),
    Keys = [Key || {_, Key} <- KeysIDX],
    Values = [Value || {_, Value} <- ValuesIDX],
    NewHash = lists:keysort(1, lists:zip(Keys, Values)),
    ModFun =
        fun(_Vsn, {hash, Hash}) ->
                NewHash2 = lists:keymerge(1, NewHash, Hash),
                {reply, ok, {hash, NewHash2}};
            (_Vsn, _Value) ->
                {reply_noreplicate, {error, wrong_datatype}}
        end,
    Default = {hash, orddict:new()},
    {reply, Reply} = riak_ensemble_peer:kmodify(Node, Ensemble, BKey, ModFun, Default, Timeout),
    ok = rr_protocol:answer(Connection, Reply),
    {ok, State};
handle(Connection, State, [<<"DECR">>, Key]) ->
    handle(Connection, State, [<<"INCRBY">>, Key, -1]);
handle(Connection, State, [<<"INCR">>, Key]) ->
    handle(Connection, State, [<<"INCRBY">>, Key, 1]);
handle(Connection, State, [<<"INCRBY">>, Key, AmountStr]) when is_binary(AmountStr) ->
    handle(Connection, State, [<<"INCRBY">>, Key, erlang:binary_to_integer(AmountStr)]);
handle(Connection, State, [<<"DECRBY">>, Key, AmountStr]) when is_binary(AmountStr) ->
    handle(Connection, State, [<<"INCRBY">>, Key, -1 * erlang:binary_to_integer(AmountStr)]);

handle(Connection, State = #state{riak_client = {_Module, [Node, _ClientId]}}, [<<"INCRBY">>, Key, Amount]) when is_integer(Amount) ->
    BKey = {{?CONSISTENT_TYPE, ?CONSISTENT_BUCKET}, Key},
    Ensemble = riak_client:ensemble(BKey),
    Timeout = 61000,
    ModFun =
        fun(_Vsn, {counter, OldAmount}) ->
            {counter, OldAmount + Amount};
            (_Vsn, _Value) ->
                failed
        end,
    Default = {counter, 0},
    Result = riak_ensemble_peer:kmodify(Node, Ensemble, BKey, ModFun, Default, Timeout),
    case Result of
        {ok, RObj} ->
            {counter, NewAmount} = riak_object:get_value(RObj),
            ok = rr_protocol:answer(Connection, NewAmount)
    end,
    {ok, State};

handle(Connection, State, [<<"PING">>]) ->
    ok = rr_protocol:answer(Connection, <<"PONG">>),
    {ok, State};
handle(Connection, State, Action) ->
    io:format("Unknown Action: ~p~n", [Action]),
    ok = rr_protocol:answer(Connection, ok),
    {ok, State}.

handle(State, [<<"PING">>]) ->
    {ok, State, <<"PONG">>};
handle(State, Action) ->
    io:format("Handling data: ~p~n", [Action]),
    {ok, State, ok}.

init() ->
    {ok, Client} = riak:local_client(),
    #state{riak_client = Client}.

