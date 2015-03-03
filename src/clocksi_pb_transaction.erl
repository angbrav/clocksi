%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 SyncFree Consortium.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
-module(clocksi_pb_transaction).

-ifdef(TEST).
-compile([export_all]).
-include_lib("eunit/include/eunit.hrl").
-endif.

-behaviour(riak_api_pb_service).

-include_lib("riak_pb/include/clocksi_pb.hrl").

-export([init/0,
         decode/2,
         encode/1,
         process/2,
         process_stream/3
        ]).

-record(state, {client}).

%% @doc init/0 callback. Returns the service internal start
%% state.
init() ->
    %lager:info("initing"),
    #state{}.

%% @doc decode/2 callback. Decodes an incoming message.
decode(Code, Bin) ->
    %lager:info("Decoding message: ~w ~w ~n",[Code, Bin]),
    Msg = riak_pb_codec:decode(Code, Bin),
    %lager:info("Before~n"),
    %lager:info("Decoded ~w~n", [Msg]),
    case Msg of
        #fpbstarttxreq{} ->
            {ok, Msg, {"clocksi.starttx", <<>>}};
        #fpbupdatereq{} ->
            {ok, Msg, {"clocksi.update", <<>>}};
        #fpbreadreq{} ->
            {ok, Msg, {"clocksi.read", <<>>}};
        #fpbcommittxreq{} ->
            {ok, Msg, {"clocksi.committx", <<>>}}
    end.

%% @doc encode/1 callback. Encodes an outgoing response message.
encode(Message) ->
    %lager:info("Encoding ~w ~n", [Message]),
    {ok, riak_pb_codec:encode(Message)}.

%% @doc process/2 callback. Handles an incoming request message.
%% Not really using the clock.
process(#fpbstarttxreq{time=_Time}, State) ->
    %lager:info("Start tx~n"),
    {ok, TxId} = clocksi:start_tx(), 
    {Counter, PID} = TxId,
    {reply, #fpbstarttxresp{txid=#txid{uuid=Counter, pid=term_to_binary(PID)}}, State};

%% @doc process/2 callback. Handles an incoming request message.
process(#fpbupdatereq{txid=ByteTxId, key=Key, value=Value}, State) ->
    %lager:info("Update tx~n"),
    BytePID = ByteTxId#txid.pid,
    TxId = {ByteTxId#txid.uuid, binary_to_term(BytePID)},
    Result = clocksi:update(TxId, Key, Value), 
    {reply, #fpbupdateresp{result=term_to_binary(Result)}, State};

%% @doc process/2 callback. Handles an incoming request message.
%% @todo accept different types of counters.
process(#fpbreadreq{txid=ByteTxId, key=Key}, State) ->
    %lager:info("Read tx~n"),
    BytePID = ByteTxId#txid.pid,
    TxId = {ByteTxId#txid.uuid, binary_to_term(BytePID)},
    Result = clocksi:read(TxId, Key),
    {reply, #fpbreadresp{result = term_to_binary(Result)}, State};

process(#fpbcommittxreq{txid=ByteTxId}, State) ->
    %lager:info("Commit tx~n"),
    BytePID = ByteTxId#txid.pid,
    TxId = {ByteTxId#txid.uuid, binary_to_term(BytePID)},
    Result = clocksi:commit(TxId),
    {reply, #fpbcommittxresp{result = term_to_binary(Result)}, State}.

%% @doc process_stream/3 callback. This service does not create any
%% streaming responses and so ignores all incoming messages.
process_stream(_,_,State) ->
    {ignore, State}.
