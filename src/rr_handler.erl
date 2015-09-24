-module(rr_handler).

-export([handle/3]).
-export([init/0]).
-define(CONSISTENT_TYPE, <<"cp">>).
-define(CONSISTENT_BUCKET, <<"bucket">>).
-record(state, {riak_client}).

-include_lib("riak_kv/include/riak_object.hrl").




%% Super inefficient. Optimize, please.
get_range_fun(Start, End) when is_integer(Start) andalso is_integer(End) ->
    fun(List) ->
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
        lists:filtermap(Slices, IdxList)
    end.

parse_zadd1([<<"NX">>|Cmd]) ->
    parse_zadd2(Cmd, [only_add]);
parse_zadd1([<<"XX">>|Cmd]) ->
    parse_zadd2(Cmd, [only_update]);
parse_zadd1(Cmd) ->
    parse_zadd2(Cmd, []).
parse_zadd2([<<"CH">>| Cmd], Opts) ->
    parse_zadd3(Cmd, [changed|Opts]);
parse_zadd2(Cmd, Opts) ->
    parse_zadd3(Cmd, Opts).
parse_zadd3([<<"INCR">> | Cmd], Opts) ->
    {Cmd, [increment|Opts]};
parse_zadd3(Cmd, Opts) ->
    {Cmd, Opts}.

binary_to_number(Bin) ->
    try binary_to_float(Bin) of
        Val -> Val
    catch
        _:_ ->
            binary_to_integer(Bin)
    end.




handle(_Connection, State = #state{riak_client = {_Module, [Node, _ClientId]}}, [<<"ZREM">>, Key| Keys]) ->
    BKey = {{?CONSISTENT_TYPE, ?CONSISTENT_BUCKET}, Key},
    Ensemble = riak_client:ensemble(BKey),
    Timeout = 61000,
    ModFun =
        fun(_Vsn, {z, List}) ->
            %KeyScoresToUpdate = [{NewKey, NewScore} || {NewKey, NewScore} <- MemberScores, {OldKey, _} <- List, NewKey == OldKey],
            NewUnsortedList = lists:foldl(fun(KeyToDelete, Acc) -> lists:keydelete(KeyToDelete, 1, Acc) end, List, Keys),
            SortedList = lists:keysort(2, NewUnsortedList),
            {reply, {ok, length(List) - length(NewUnsortedList)}, {z, SortedList}};
            (_Vsn, _Value) ->
                {reply_noreplicate, {error, wrong_datatype}}
        end,
    Default = {z, []},
    Result = riak_ensemble_peer:kmodify(Node, Ensemble, BKey, ModFun, Default, Timeout),
    case Result of
        {reply, {ok, Count}} ->
            {reply, Count, State}
    end;

handle(_Connection, State = #state{riak_client = {_Module, [Node, _ClientId]}}, [<<"ZSCORE">>, Key, Member]) ->

    BKey = {{?CONSISTENT_TYPE, ?CONSISTENT_BUCKET}, Key},
    Ensemble = riak_client:ensemble(BKey),
    Timeout = 61000,
    ModFun =
        fun (_Vsn, {z, Value}) ->
            Ret = case lists:keyfind(Member, 1, Value) of
                      false -> 0;
                      {_, Score} -> Score
            end,

            {reply_noreplicate, {ok, Ret}}
        end,
    Default = {z, []},

    case riak_ensemble_peer:kmodify(Node, Ensemble, BKey, ModFun, Default, Timeout) of
        {reply, {ok, Score}} ->
            io:format("Score: ~p~n", [Score]),
            {reply, float_to_binary(Score * 1.0), State}
    end;
handle(_Connection, State = #state{riak_client = {_Module, [Node, _ClientId]}}, [<<"ZCARD">>, Key]) ->

    BKey = {{?CONSISTENT_TYPE, ?CONSISTENT_BUCKET}, Key},
    Ensemble = riak_client:ensemble(BKey),
    Timeout = 61000,
    ModFun =
        fun (_Vsn, {z, Value}) ->
            {reply_noreplicate, {ok, length(Value)}}
        end,
    Default = {z, []},

    case riak_ensemble_peer:kmodify(Node, Ensemble, BKey, ModFun, Default, Timeout) of
        {reply, {ok, Length}} ->
            {reply, Length, State}
    end;
handle(_Connection, State = #state{riak_client = {_Module, [Node, _ClientId]}}, [<<"ZRANGE">>, Key, StartBin, EndBin|MaybeWithScores])  ->
    Start = erlang:binary_to_integer(StartBin),
    End = erlang:binary_to_integer(EndBin),
    BKey = {{?CONSISTENT_TYPE, ?CONSISTENT_BUCKET}, Key},
    Ensemble = riak_client:ensemble(BKey),
    Timeout = 61000,

    RangeFun = get_range_fun(Start, End),
    ModFun =
        fun (_Vsn, {z, Value}) ->
            Range = RangeFun(Value),
            Result = case MaybeWithScores of
                [] ->
                    [Member || {Member, _Score} <- Range];
                [<<"WITHSCORES">>] ->
                    lists:merge([[Member, float_to_binary(Score * 1.0)] || {Member, Score} <- Range])
            end,
            {reply_noreplicate, {ok, Result}};
            (_Vsn, {error, not_found}) ->
                {reply_noreplicate, {error, not_found}}
        end,
    Default = {error, not_found},

    case riak_ensemble_peer:kmodify(Node, Ensemble, BKey, ModFun, Default, Timeout) of
        {reply, {error, not_found}} ->
            {reply, {error, not_found}, State};
        {reply, {ok, List}} ->
            {reply, List, State}
    end;
handle(_Connection, State = #state{riak_client = {_Module, [Node, _ClientId]}}, [<<"ZREVRANGE">>, Key, StartBin, EndBin|MaybeWithScores])  ->
    Start = erlang:binary_to_integer(StartBin),
    End = erlang:binary_to_integer(EndBin),
    BKey = {{?CONSISTENT_TYPE, ?CONSISTENT_BUCKET}, Key},
    Ensemble = riak_client:ensemble(BKey),
    Timeout = 61000,

    RangeFun = get_range_fun(Start, End),
    ModFun =
        fun (_Vsn, {z, Value}) ->
            Range = RangeFun(lists:reverse(Value)),
            Result = case MaybeWithScores of
                         [] ->
                             [Member || {Member, _Score} <- Range];
                         [<<"WITHSCORES">>] ->
                             lists:merge([[Member, float_to_binary(Score * 1.0)] || {Member, Score} <- Range])
                     end,
            {reply_noreplicate, {ok, Result}};
            (_Vsn, {error, not_found}) ->
                {reply_noreplicate, {error, not_found}}
        end,
    Default = {error, not_found},

    case riak_ensemble_peer:kmodify(Node, Ensemble, BKey, ModFun, Default, Timeout) of
        {reply, {error, not_found}} ->
            {reply, {error, not_found}, State};
        {reply, {ok, List}} ->
            {reply, List, State}
    end;
handle(_Connection, State = #state{riak_client = {_Module, [Node, _ClientId]}}, [<<"ZADD">>, Key|Opts]) ->
    BKey = {{?CONSISTENT_TYPE, ?CONSISTENT_BUCKET}, Key},
    Ensemble = riak_client:ensemble(BKey),
    Timeout = 61000,
    {MemberScoresBin, []} = parse_zadd1(Opts),
    IDXs = lists:zip(lists:seq(1, erlang:length(MemberScoresBin)), MemberScoresBin),
    {ScoresIDX, MemberIDX} = lists:partition(fun({X, _}) -> X rem 2 == 1 end, IDXs),
    Members = [Member || {_, Member} <- MemberIDX],
    Scores = [binary_to_number(Value)|| {_, Value} <- ScoresIDX],
    %  lists:keysort(1,
    MemberScores = lists:zip(Members, Scores) ,
    ModFun =
        fun(_Vsn, {z, List}) ->
            %KeyScoresToUpdate = [{NewKey, NewScore} || {NewKey, NewScore} <- MemberScores, {OldKey, _} <- List, NewKey == OldKey],
            NewUnsortedList = lists:foldl(fun({Member, Score}, Acc) -> lists:keystore(Member, 1, Acc, {Member, Score}) end, List, MemberScores),
            SortedList = lists:keysort(2, NewUnsortedList),
            {reply, {ok, length(NewUnsortedList) - length(List)}, {z, SortedList}};
            (_Vsn, _Value) ->
                {reply_noreplicate, {error, wrong_datatype}}
        end,
    Default = {z, []},
    Result = riak_ensemble_peer:kmodify(Node, Ensemble, BKey, ModFun, Default, Timeout),
    case Result of
        {reply, {ok, Count}} ->
            {reply, Count, State}
    end;
handle(_Connection, State = #state{riak_client = {_Module, [Node, _ClientId]}}, [<<"LLEN">>, Key]) ->
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
            {reply, Count, State}
    end;
handle(_Connection, State = #state{riak_client = {_Module, [Node, _ClientId]}}, [<<"LPUSH">>, Key|Values]) ->
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
            {reply, Count, State}
    end;
handle(_Connection, State = #state{riak_client = {_Module, [Node, _ClientId]}}, [<<"LTRIM">>, Key, StartBin, EndBin]) ->
    Start = erlang:binary_to_integer(StartBin),
    End = erlang:binary_to_integer(EndBin),
    BKey = {{?CONSISTENT_TYPE, ?CONSISTENT_BUCKET}, Key},
    Ensemble = riak_client:ensemble(BKey),
    Timeout = 61000,
    RangeFun = get_range_fun(Start, End),

    ModFun =
        fun(_Vsn, {list, OldList}) ->
            NewList = RangeFun(OldList),
            {reply, {ok, ok}, {list, NewList}};
            (_Vsn, _Value) ->
                {reply_noreplicate, {error, wrong_datatype}}
        end,
    Default = {list, []},
    _ = riak_ensemble_peer:kmodify(Node, Ensemble, BKey, ModFun, Default, Timeout),
    {reply, ok, State};
handle(_Connection, State = #state{riak_client = {_Module, [Node, _ClientId]}}, [<<"LRANGE">>, Key, StartBin, EndBin]) ->
    Start = erlang:binary_to_integer(StartBin),
    End = erlang:binary_to_integer(EndBin),
    BKey = {{?CONSISTENT_TYPE, ?CONSISTENT_BUCKET}, Key},
    Ensemble = riak_client:ensemble(BKey),
    Timeout = 61000,

    RangeFun = get_range_fun(Start, End),
    ModFun =
        fun(_Vsn, {list, Value}) ->
            {reply_noreplicate, {ok, RangeFun(Value)}};
            (_Vsn, {error, not_found}) ->
                {reply_noreplicate, {error, not_found}}
        end,
    Default = {error, not_found},

    case riak_ensemble_peer:kmodify(Node, Ensemble, BKey, ModFun, Default, Timeout) of
        {reply, {error, not_found}} ->
            {reply, {error, not_found}, State};
        {reply, {ok, List}} ->
            {reply, List, State}
    end;
handle(_Connection, State = #state{riak_client = {_Module, [Node, _ClientId]}}, [<<"SET">>, Key, Value]) ->
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
    %State1 = orddict:store(Key, {kv, Value}, State),

    {reply, ok, State};

handle(_Connection, State = #state{riak_client = {_Module, [Node, _ClientId]}}, [<<"DEL">>, Key]) ->
    BKey = {{?CONSISTENT_TYPE, ?CONSISTENT_BUCKET}, Key},
    Ensemble = riak_client:ensemble(BKey),
    Timeout = 61000,
    _Result = riak_ensemble_peer:kdelete(Node, Ensemble, BKey, Timeout),
    %State1 = orddict:store(Key, {kv, Value}, State),

    {reply, 1, State};

handle(_Connection, State = #state{riak_client = {_Module, [Node, _ClientId]}}, [<<"GET">>, Key]) ->
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
            {reply, {error, not_found}, State};
        {reply, {kv, RealValue}} ->
            {reply, RealValue, State};
        {reply, {counter, RealValue}} ->
            {reply, erlang:integer_to_binary(RealValue), State}
    end;

handle(_Connection, State = #state{riak_client = {_Module, [Node, _ClientId]}}, [<<"HSET">>, HashName, Key, Value]) ->
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
    {reply, Count, State};

handle(_Connection, State = #state{riak_client = {_Module, [Node, _ClientId]}}, [<<"HDEL">>, HashName | Keys]) ->
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

    {reply, Count, State};

handle(_Connection, State = #state{riak_client = {_Module, [Node, _ClientId]}}, [<<"HGETALL">>, HashName]) ->
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
            {reply, lists:merge(LoL), State}
    end;

handle(_Connection, State = #state{riak_client = {_Module, [Node, _ClientId]}}, [<<"HGET">>, HashName, Key]) ->
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
            {reply, Value, State};
        {reply, {error, not_found}} ->
            {reply, {error, not_found}, State}
    end;

handle(_Connection, State = #state{riak_client = {_Module, [Node, _ClientId]}}, [<<"HSETNX">>, HashName, Key, Value]) ->
    BKey = {{?CONSISTENT_TYPE, ?CONSISTENT_BUCKET}, HashName},
    Ensemble = riak_client:ensemble(BKey),
    Timeout = 61000,
    ModFun =
        fun(_Vsn, {hash, Hash}) ->
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
    case Result of
        {reply, {error, value_existed}} ->
            {reply, 0, State};
        {reply, Count} ->
            {reply, Count, State}
    end;

handle(_Connection, State = #state{riak_client = {_Module, [Node, _ClientId]}}, [<<"HMSET">>, HashName | KeyValues]) ->
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
            NewHash2 = orddict:merge(fun(_Key, NewValue, _OldValue) -> NewValue end, NewHash, Hash),
             {reply, ok, {hash, NewHash2}};
            (_Vsn, _Value) ->
                {reply_noreplicate, {error, wrong_datatype}}
        end,
    Default = {hash, orddict:new()},
    {reply, Reply} = riak_ensemble_peer:kmodify(Node, Ensemble, BKey, ModFun, Default, Timeout),
    {reply, Reply, State};
handle(_Connection, State, [<<"DECR">>, Key]) ->
    handle(_Connection, State, [<<"INCRBY">>, Key, -1]);
handle(_Connection, State, [<<"INCR">>, Key]) ->
    handle(_Connection, State, [<<"INCRBY">>, Key, 1]);
handle(_Connection, State, [<<"INCRBY">>, Key, AmountStr]) when is_binary(AmountStr) ->
    handle(_Connection, State, [<<"INCRBY">>, Key, erlang:binary_to_integer(AmountStr)]);
handle(_Connection, State, [<<"DECRBY">>, Key, AmountStr]) when is_binary(AmountStr) ->
    handle(_Connection, State, [<<"INCRBY">>, Key, -1 * erlang:binary_to_integer(AmountStr)]);

handle(_Connection, State = #state{riak_client = {_Module, [Node, _ClientId]}}, [<<"INCRBY">>, Key, Amount]) when is_integer(Amount) ->
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
            {reply, NewAmount, State}
    end;

handle(_Connection, State, [<<"PING">>]) ->
    {reply, <<"PONG">>, State};
handle(_Connection, State, Action) ->
    io:format("Unknown Action: ~p~n", [Action]),
    {reply, ok, State}.

init() ->
    {ok, Client} = riak:local_client(),
    #state{riak_client = Client}.

