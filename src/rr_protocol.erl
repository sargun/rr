%%%-------------------------------------------------------------------
%%% @author sdhillon
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 22. Sep 2015 4:55 PM
%%%-------------------------------------------------------------------
-module(rr_protocol).
-author("sdhillon").

-behaviour(ranch_protocol).

-record(state, {handler}).
-export([start_link/4]).
-export([init/4, answer/2]).
-include_lib("eredis/include/eredis.hrl").


-record(connection, {
    socket,
    transport,
    state,
    options = [],
    module
}).

start_link(Ref, Socket, Transport, Opts) ->
    {redis_handler, Handler} = proplists:lookup(redis_handler, Opts),
    State = #state{handler = Handler},
    Pid = spawn_link(?MODULE, init, [Ref, Socket, Transport, State]),
    {ok, Pid}.

init(Ref, Socket, Transport, _InitState = #state{handler = Mod}) ->
    ok = ranch:accept_ack(Ref),
    Parser = eredis_parser:init(),
    State = Mod:init(),
    read_line(#connection{
        socket = Socket,
        transport = Transport,
        options = [],
        state = State,
        module = Mod}, Parser, <<>>).


read_line(#connection{socket=Socket, transport=Transport, options=Options} = Connection, Parser, Rest) ->
    ok = Transport:setopts(Socket, [binary, {active, once}]),
    Timeout = proplists:get_value(timeout, Options, 30000),
    Line = receive {tcp, Socket, ILine} ->
        ILine
           after Timeout ->
               exit(timeout)
           end,
    case parse(Connection, Parser, <<Rest/binary, Line/binary>>) of
        {ok, ConnectionState, NewState} ->
            read_line(Connection#connection{state=ConnectionState}, NewState, <<>>);
        {continue, NewState} -> read_line(Connection, NewState, Rest);
        Oups -> io:format("Oups le readline. : ~p~p~n", [Oups, Line])
    end.

parse(#connection{socket = Socket, transport=Transport, state=HandleState, module=Mod} = Connection, State, Data) ->
    case eredis_parser:parse(State, Data) of
        {ok, Return, NewParserState} ->
            {ok, ConnectionState} = Mod:handle({Socket, Transport}, HandleState, Return),
            {ok, ConnectionState, NewParserState};
        {ok, Return, Rest, NewParserState} ->
            {ok, ConnectionState} = Mod:handle({Socket, Transport}, HandleState, Return),
            parse(Connection#connection{state=ConnectionState}, NewParserState, Rest);
        {continue, NewParserState} ->
            {continue, NewParserState};
        {error,unknown_response} -> %% Handling painful old syntax, without prefix
            case get_newline_pos(Data) of
                undefined ->
                    {continue, State};
                Pos ->
                    <<Value:Pos/binary, ?NL, Rest/binary>> = Data,
                    {ok, ConnectionState} = Mod:handle({Socket, Transport}, HandleState, binary:split(Value, <<$ >>, [global])),
                    case Rest of
                        <<>> ->
                            {ok, ConnectionState, State};
                        _ ->
                            parse(Connection#connection{state=ConnectionState}, HandleState, Rest)
                    end
            end;
        Error ->
            io:format("Error ~p~n", [Error]),
            {error, Error}
    end.

answer({Socket, Transport}, Answer) ->
    Transport:send(Socket, redis_protocol_encoder:encode(Answer)).


get_newline_pos(B) ->
    case re:run(B, ?NL) of
        {match, [{Pos, _}]} -> Pos;
        nomatch -> undefined
    end.
