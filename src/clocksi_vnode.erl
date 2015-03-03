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
-module(clocksi_vnode).

-behaviour(riak_core_vnode).

-include("clocksi.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([start_vnode/1,
         read_data_item/3,
         prepare/4,
         local_commit/5,
         commit/4,
         abort/2,
         init/1,
         terminate/2,
         handle_command/3,
         is_empty/1,
         delete/1,
         handle_handoff_command/3,
         handoff_starting/2,
         handoff_cancelled/1,
         handoff_finished/2,
         handle_handoff_data/2,
         encode_handoff_item/2,
         handle_coverage/4,
         handle_exit/3]).

-ignore_xref([start_vnode/1]).

%%%===================================================================
%%% API
%%%===================================================================

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

%% @doc Sends a read request to the Node that is responsible for the Key
read_data_item(Node, Key, SnapshotTime) ->
    riak_core_vnode_master:command(Node,
                                   {read_data_item, Key, SnapshotTime},
                                   {fsm, undefined, self()},
                                   ?CLOCKSI_MASTER).

%% @doc Sends a prepare request to a Node involved in a tx identified by TxId
prepare(Node, Keys, TxId, SnapshotTime) ->
    riak_core_vnode_master:command(Node,
                                   {prepare, Keys, TxId, SnapshotTime},
                                   {fsm, undefined, self()},
                                   ?CLOCKSI_MASTER).

%% @doc Sends a local_commit request to a Node involved in a tx identified by TxId
local_commit(Node, Updates, TxId, SnapshotTime, Client) ->
    riak_core_vnode_master:command(Node,
                                   {local_commit, Updates, TxId, SnapshotTime, Client},
                                   {fsm, undefined, self()},
                                   ?CLOCKSI_MASTER).

%% @doc Sends a commit request to a Node involved in a tx identified by TxId
commit(Node, Updates, TxId, CommitTime) ->
    riak_core_vnode_master:command(Node,
                                   {commit, Updates, TxId, CommitTime},
                                   {fsm, undefined, self()},
                                   ?CLOCKSI_MASTER).

%% @doc Sends a commit request to a Node involved in a tx identified by TxId
abort(ListofNodes, TxId) ->
    riak_core_vnode_master:command(ListofNodes,
                                   {abort, TxId},
                                   {fsm, undefined, self()},
                                   ?CLOCKSI_MASTER).

%% @doc Initializes all data structures that vnode needs to track information
%%      the transactions it participates on.
init([Partition]) ->
    Clocks = ?CLOCKS_IMPLEMENTATION,
    Backend = ?BACKEND,
    PreparedKey = ets:new(list_to_atom(atom_to_list(prepared_key) ++
                                          integer_to_list(Partition)),
                          [set]),
    TxPendingForCommit = ets:new(list_to_atom(atom_to_list(tx_pending_for_commit) ++
                                           integer_to_list(Partition)),
                                 [bag]),
    NToNotifyTx = ets:new(list_to_atom(atom_to_list(n_to_notify_tx)
                                           ++ integer_to_list(Partition)),
                          [set]),
    PendingReads = ets:new(list_to_atom(atom_to_list(pending_reads) ++
                                           integer_to_list(Partition)),
                                  [bag]),
    NToRead = ets:new(list_to_atom(atom_to_list(n_to_read)
                                           ++ integer_to_list(Partition)),
                           [set]),
    PreparedTx = ets:new(list_to_atom(atom_to_list(prepared_tx)
                                           ++ integer_to_list(Partition)),
                          [set]),
    PendingCertification = ets:new(list_to_atom(atom_to_list(pending_certification)
                                           ++ integer_to_list(Partition)),
                                   [set]),
    CommittedKey = ets:new(list_to_atom(atom_to_list(committed_key) ++
                                          integer_to_list(Partition)),
                          [set]),

    SD0 = #vnode_state{partition=Partition,
                       clocks= Clocks,
                       backend=Backend,
                       prepared_key=PreparedKey,
                       tx_pending_for_commit=TxPendingForCommit,
                       n_to_notify_tx=NToNotifyTx,
                       pending_reads=PendingReads,
                       n_to_read=NToRead,
                       prepared_tx=PreparedTx,
                       pending_certification=PendingCertification,
                       committed_key=CommittedKey,
                       committed_write_set=[]},

    SD = Backend:start(SD0),
    {ok, SD}.
               
handle_command(ping, _Sender, SD0) ->
    {reply, pong, SD0};
 
%% @doc starts a read_fsm to handle a read operation.
handle_command({read_data_item, Key, SnapshotTime}, Sender, SD0=#vnode_state{clocks=Clocks,
                                                                             backend=Backend,
                                                                             prepared_key=PreparedKey,
                                                                             pending_reads=PendingReads,
                                                                             n_to_read=NToRead}) ->
    ReqId = clocksi:mk_reqid(),
    SD = Clocks:wait_until_safe(SnapshotTime, SD0),
    case ets:lookup(PreparedKey, Key) of
        [{Key, Orddict0}] ->
            case get_txs_to_wait_for(SnapshotTime, Orddict0, Clocks) of
                [] ->
                    {Reply, SD1} = Backend:get(Key, SnapshotTime, SD),
                    {reply, Reply, SD1};
                ListTxs ->
                    lists:foreach(fun(TxId) ->
                                    true = ets:insert(PendingReads, {TxId, {ReqId, Key, SnapshotTime, Sender}})
                                  end, ListTxs),
                    true = ets:insert(NToRead, {ReqId, length(ListTxs)}),
                    {noreply, SD}
            end;
        [] ->
            {Reply, SD1} = Backend:get(Key, SnapshotTime, SD),
            {reply, Reply, SD1}
    end;

handle_command({prepare, Keys, TxId, SnapshotTime}, Sender, SD0=#vnode_state{clocks=Clocks}) ->
    SD = Clocks:wait_until_safe(SnapshotTime, SD0),
    do_prepare(Keys, TxId, SnapshotTime, Sender, SD);
    
handle_command({local_commit, Updates, TxId, SnapshotTime, Client}, _Sender, SD0=#vnode_state{clocks=Clocks}) ->
    SD = Clocks:wait_until_safe(SnapshotTime, SD0),
    do_localcommit(Updates, TxId, SnapshotTime, Client, SD);

handle_command({commit, Updates, TxId, CommitTime}, _Sender, SD0) ->
    do_commit(Updates, TxId, CommitTime, SD0);

handle_command({abort, TxId}, _Sender, SD0=#vnode_state{prepared_key=PreparedKey,
                                                          prepared_tx=PreparedTx}) ->
    [{TxId, {_PreparedTime, Keys}}] = ets:lookup(PreparedTx, TxId),
    clean_prepared(PreparedKey, PreparedTx, Keys, TxId),
    handle_txs_pendings(TxId, SD0),
    SD = handle_pending_reads(TxId, SD0),
    {reply, ack_abort, SD};

handle_command(_Message, _Sender, State) ->
    {noreply, State}.

handle_handoff_command(_Message, _Sender, State) ->
    {noreply, State}.

handoff_starting(_TargetNode, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(_Data, State) ->
    {reply, ok, State}.

encode_handoff_item(_ObjectName, _ObjectValue) ->
    <<>>.

is_empty(State) ->
    {true, State}.

delete(State) ->
    {ok, State}.

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%%===================================================================
%%% Internal Functions
%%%===================================================================

%% @doc Performs a certification check when a transaction wants to move
%%      to the prepared state.
certification_check(_, [], _, _) ->
    true;
certification_check(SnapshotTime, [Key|Rest], CommittedKey, Clocks) ->
    
    case ets:lookup(CommittedKey, Key) of
    [] ->
        certification_check(SnapshotTime, Rest, CommittedKey, Clocks);
    [{Key, Orddict}] ->
        case concurrent(SnapshotTime, Orddict, Clocks) of
            true ->
                false;
            false ->
                certification_check(SnapshotTime, Rest, CommittedKey, Clocks)
        end
    end.

concurrent(SnapshotTime, Orddict, Clocks) ->
    {CommitTime, _TxId} = lists:last(Orddict),
    Clocks:compare_g(CommitTime, SnapshotTime).

check_conflicting_prepared(Keys, PreparedKeys)->
    lists:foldl(fun(Key, List)->
                    case ets:lookup(PreparedKeys, Key) of
                        [] ->
                            List;
                        [{Key, Orddict}] ->
                            FilteredTxs = [TxId || {_PT, TxId} <- Orddict],
                            lists:foldl(fun(TxId, Acc) ->
                                            case lists:member(TxId, Acc) of
                                                true ->
                                                    Acc;
                                                false ->
                                                    Acc ++ [TxId]
                                            end
                                        end, List, FilteredTxs)
                        end
                end, [], Keys).

clean_prepared(PreparedKey, PreparedTx, Keys, TxId) ->
    [{TxId, {PrepareTime, _Keys}}] = ets:lookup(PreparedTx, TxId),
    true = ets:delete(PreparedTx, TxId),
    
    lists:foreach(fun(Key) ->
                    [{Key, Orddict0}] = ets:lookup(PreparedKey, Key),
                    Orddict = orddict:erase(PrepareTime, Orddict0),
                    true = ets:insert(PreparedKey, {Key, Orddict})
                  end, Keys).

handle_txs_pendings(TxId, State = #vnode_state{tx_pending_for_commit=TxPendingForCommit,
                                               n_to_notify_tx=NToNotifyTx,
                                               pending_certification=PendingCertification}) ->
    case ets:lookup(TxPendingForCommit, TxId) of
        [] ->
            ok;
        PendingList ->
            lists:foreach(fun({_TxId, Elem}) ->
                            [{Elem, N0}] = ets:lookup(NToNotifyTx, Elem),
                            case N0 of
                                1 ->
                                    true = ets:delete(NToNotifyTx, Elem),
                                    [{Elem, {Keys, SnapshotTime, From}}] = ets:lookup(PendingCertification, Elem),
                                    true = ets:delete(PendingCertification, Elem),
                                    case From of
                                    {client, Client} ->
                                        do_localcommit(Keys, Elem, SnapshotTime, Client, State);
                                    _ ->
                                        case do_prepare(Keys, Elem, SnapshotTime, From, State) of
                                            {reply, Reply, _State} ->
                                                gen_fsm:reply(From, Reply);
                                            {noreply, _State} ->
                                                ok
                                        end
                                    end;
                                _ ->
                                    true = ets:insert(NToNotifyTx, {Elem, N0 - 1})
                            end
                          end, PendingList)
    end,
    true = ets:delete(TxPendingForCommit, TxId).

handle_pending_reads(TxId, State = #vnode_state{pending_reads=PendingReads,
                                                n_to_read=NToRead,
                                                backend=Backend}) ->
    case ets:lookup(PendingReads, TxId) of
        [] ->
            true = ets:delete(PendingReads, TxId),
            State;
        PendingList ->
            true = ets:delete(PendingReads, TxId),
            lists:foldl(fun(Element, Acc0) ->
                            {_TxId, {ReqId, Key, SnapshotTime, Sender}} = Element,
                            [{ReqId, N0}] = ets:lookup(NToRead, ReqId),
                            case N0 of
                                1 ->
                                    true = ets:delete(NToRead, ReqId),
                                    {Reply, Acc} = Backend:get(Key, SnapshotTime, Acc0),
                                    riak_core_vnode:reply(Sender, Reply),
                                    Acc;
                                _ ->
                                    true = ets:insert(NToRead, {ReqId, N0 - 1}),
                                    Acc0
                            end
                        end, State, PendingList)
    end.

get_txs_to_wait_for(SnapshotTime, Tuples, Clocks) ->
    List = lists:takewhile(fun({PreparedTime, _TxId}) ->
                            Clocks:compare_g(SnapshotTime, PreparedTime)
                           end, Tuples),
    %lager:info("It gets here, result list: ~p", [List]),
    [TxId || {_PT, TxId} <- List]. 

do_prepare(Keys0, TxId, SnapshotTime, From, State0 = #vnode_state{prepared_key=PreparedKey,
                                                                 committed_key=CommittedKey,
                                                                 clocks=Clocks,
                                                                 tx_pending_for_commit=TxPendingForCommit,
                                                                 n_to_notify_tx=NToNotifyTx,
                                                                 prepared_tx=PreparedTx,
                                                                 pending_certification=PendingCertification}) ->
    case From of
        {localcommit, _Client} ->
            Keys = [Key || {Key, _Value} <- Keys0];
        _ ->
            Keys = Keys0
    end,
    case certification_check(SnapshotTime, Keys, CommittedKey, Clocks) of
        true ->
            ConflictingPrepared = check_conflicting_prepared(Keys, PreparedKey),
            case ConflictingPrepared of
                [] ->
                    {PrepareTime, State} = Clocks:get_prepare_time(State0),
                    true = ets:insert(PreparedTx, {TxId, {PrepareTime, Keys}}),
                    lists:foreach(fun(Key) ->
                                    case ets:lookup(PreparedKey, Key) of
                                        [] ->
                                            Dict0 = orddict:new(),
                                            Dict = orddict:store(PrepareTime, TxId, Dict0);
                                        [{Key, Dict0}] ->
                                            Dict = orddict:store(PrepareTime, TxId, Dict0)
                                    end,
                                    true = ets:insert(PreparedKey, {Key, Dict})
                                  end, Keys),
                    {reply, {prepared, PrepareTime}, State};
                List ->
                    lists:foreach(fun(Elem) ->
                                    true = ets:insert(TxPendingForCommit, {Elem, TxId})
                                  end, List),
                    true = ets:insert(NToNotifyTx, {TxId, length(List)}),
                    true = ets:insert(PendingCertification, {TxId, {Keys0, SnapshotTime, From}}),
                    {noreply, State0}
            end;
        false ->
            {reply, abort, State0}
    end.

do_localcommit(Updates, TxId, SnapshotTime, Client, SD0) ->
    case do_prepare(Updates, TxId, SnapshotTime, {localcommit, Client}, SD0) of
        {reply, {prepared, PrepareTime}, SD1} ->
            case do_commit(Updates, TxId, PrepareTime, SD1) of
                {reply, committed, SD2} ->
                    Client ! {ok, {TxId, PrepareTime}};
                {reply, {error, _Reason}, SD2} ->
                    riak_core_vnode:reply(Client, {aborted, TxId})
            end;
        {noreply, SD2} ->
            ok
    end,
    {noreply, SD2}.

do_commit(Updates, TxId, CommitTime, SD0=#vnode_state{committed_write_set=CommittedWriteSet0,
                                                                              backend=Backend,
                                                                              prepared_key=PreparedKey,
                                                                              prepared_tx=PreparedTx,
                                                                              committed_key=CommittedKey}) ->
    case Backend:put(Updates, CommitTime, SD0) of 
        {ok, SD} ->
            Keys = lists:foldl(fun({Key, _Value}, Acc) ->
                                case ets:lookup(CommittedKey, Key) of
                                    [] ->
                                        Dict0 = orddict:new(),
                                        Dict = orddict:store(CommitTime, TxId, Dict0);
                                    [{Key, Dict0}] ->
                                        Dict = orddict:store(CommitTime, TxId, Dict0)
                                end,
                                true = ets:insert(CommittedKey, {Key, Dict}),
                                Acc ++ [Key]
                               end, [], Updates),
            CommittedWriteSet = orddict:store(CommitTime, Keys, CommittedWriteSet0),
            clean_prepared(PreparedKey, PreparedTx, Keys, TxId),
            handle_txs_pendings(TxId, SD),
            handle_pending_reads(TxId, SD),
            {reply, committed, SD#vnode_state{committed_write_set=CommittedWriteSet}};
        {{error, Reason}, SD} ->
            {reply, {error, Reason}, SD}
    end.
