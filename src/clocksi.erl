-module(clocksi).
-include("clocksi.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([
         ping/0,
         mk_reqid/0,
         get_responsible/1,
         start_tx/0,
         start_tx/1,
         read/2,
         update/3,
         commit/1]).

%% Public API

%% @doc Pings a random vnode to make sure communication is functional
ping() ->
    DocIdx = riak_core_util:chash_key({<<"ping">>, term_to_binary(now())}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, clocksi),
    [{IndexNode, _Type}] = PrefList,
    riak_core_vnode_master:sync_spawn_command(IndexNode, ping, clocksi_vnode_master).

%% @doc Generate a request id.
%%
%%      Helper function; used to generate a unique request identifier.
%%
mk_reqid() ->
    erlang:phash2(erlang:now()).

-spec start_tx(Clock:: vectorclock:vectorclock()) -> term().
start_tx(Clock) ->
    {ok, _} = clocksi_interactive_tx_coord_sup:start_fsm([self(), Clock]),
    receive
        TxId ->
            TxId
    end.

start_tx() ->
    {ok, _} = clocksi_interactive_tx_coord_sup:start_fsm([self()]),
    receive
        TxId ->
            %lager:info("TxId ~w ~n", [TxId]),
            TxId
    end.

read({_UId, CoordFsmPid}, Key) ->
    gen_fsm:send_event(CoordFsmPid, {read, Key, self()}),
    wait_for_reply().

update({_UId, CoordFsmPid}, Key, Value) ->
    gen_fsm:send_event(CoordFsmPid, {update, {Key, Value}, self()}),
    wait_for_reply().

commit({_Uid, CoordFsmPid})->
    gen_fsm:send_event(CoordFsmPid, {commit, [], self()}),
    wait_for_reply().

get_responsible(Key)->
    HashedKey = riak_core_util:chash_key({?BUCKET, term_to_binary(Key)}),
    PreflistANN = riak_core_apl:get_primary_apl(HashedKey, 1, clocksi),
    [IndexNode || {IndexNode, _Type} <- PreflistANN].

wait_for_reply() ->
    receive
        Whatever ->
            Whatever
    end.
