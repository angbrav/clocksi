-module(ets_backend).
-behaviour(clocksi_backend).

-include("clocksi.hrl").

-export([start/1,
         put/3,
         get/3]).

start(S0=#vnode_state{partition=Partition}) ->
    Store = ets:new(list_to_atom(atom_to_list(ets_backend) ++ integer_to_list(Partition)), [set]),
    S0#vnode_state{store=Store}. 

put({Key, Value}, Version, SD0=#vnode_state{store=Store}) ->
    case ets:lookup(Store, Key) of
        [] ->
            Orddict0 = orddict:new(),
            Orddict1 = orddict:store(Version, Value, Orddict0);
        [{Key, Orddict0}] -> 
            Orddict1 = orddict:store(Version, Value, Orddict0)
    end,
    case ets:insert(Store, {Key, Orddict1}) of
        true ->
            {ok, SD0};
        Error ->
            {{error, Error}, SD0}
    end;

put([Next|Rest], Version, SD0) ->
    case put(Next, Version, SD0) of
        {ok, SD} ->
            put(Rest, Version, SD);
        {{error, Error}, SD} ->
            {{error, Error}, SD}
    end;

put([], _Version, SD0) ->
    {ok, SD0}.

get(Key, Version, SD0=#vnode_state{store=Store, clocks=Clocks}) ->
    case ets:lookup(Store, Key) of
        [] ->
            {{error, key_not_found}, SD0};
        [{Key, Orddict}] ->
            case find_version(Version, Orddict, Clocks) of
                {ok, Value} ->
                    {{ok, Value}, SD0};
                error ->
                    {{error, version_not_found}, SD0}
            end
    end.
     
find_version(Version, Orddict, Clocks) ->
    Sublist = lists:takewhile(fun(Elem) ->
                                {Time, _Value} = Elem,
                                Clocks:compare_ge(Version, Time)
                              end, Orddict),
    case Sublist of
        [] ->
            error;
        _ ->
            {_CT, Value} = lists:nth(length(Sublist), Sublist),
            {ok, Value}
    end.
