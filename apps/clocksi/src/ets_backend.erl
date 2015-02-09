-module(ets_backend).
-behaviour(clocksi_backend).

-include("clocksi.hrl").

-export([start/1,
         put/3,
         get/3]).

start(S0) ->
    Store = ets:new(list_to_atom(atom_to_list(ets_backend) ++ integer_to_list(Partition)), [set]),
    S0#vnode_state{store=Store}. 

put({Key, Value}, Version, SD0=#vnode_state{store=Store}) ->
    case ets:lookup(Store, Key) of
        [] ->
            Orddict0 = orddict:new(),
            Orddict1 = orddict:store(Version, Value, Orddict0);
        [{Key, Orddict0] -> 
            Orddict1 = orddict:store(Version, Value, Orddict0)
    end,
    case ets:insert(Store, {Key, Orddict1}) of
        true ->
            {ok, SD0};
        Error ->
            {{error, Error}, SD0}
    end;

put([Next|Rest], Version, SD0=#vnode_state{store=Store}) ->
    case put(Next, Version, SD0) of
        {ok, SD} ->
            put(Rest, Version, SD);
        {{error, Error}, SD} ->
            {{error, Error}, SD}
    end;

put([], _Version, _SD0) ->
    ok.

get(Key, Version SD0=#vnode_state{store=Store}) ->
    case ets:lookup(Store, Key) pf
        [] ->
            {{error, key_not_found}, SD0};
        [{Key, Orddict}] ->
            case orddict:find(Version, Orddict) of
                {ok, Value} ->
                    {{ok, Value}, SD0};
                error ->
                    {{error, version_not_found}, SD0}
            end
    end.