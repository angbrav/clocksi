-module(physical_clock).
-behaviour(clocksi_clocks).

-include("clocksi.hrl").

-export([get_snapshot_time/1,
         get_commit_client_clock/1,
         get_prepare_time/1,
         wait_until_safe/1,
         compare_g/2,
         compare_ge/2]).

%% @doc converts a tuple {MegaSecs,Secs,MicroSecs} into microseconds
now_milisec({MegaSecs, Secs, MicroSecs}) ->
    (MegaSecs * 1000000 + Secs) * 1000000 + MicroSecs.

get_snapshot_time(SD0)->
    SnapshotTime = now_milisec(erlang:now()),
    {SnapshotTime, SD0}.
    
get_commit_client_clock(SD0=#coord_state{commit_time=CommitTime}) ->
    {CommitTime, SD0}.

get_prepare_time(SD0) ->
    PrepareTime = now_milisec(erlang:now()),
    {PrepareTime, SD0}.

wait_until_safe(Threshold, SD0) ->
    Now = now_milisec(erlang:now()),
    case Threshold => Now of
        true ->
            Diff = Threshold - Now,
            timer:sleep(Diff + 1),
            SD0;
        false ->
            SD0
    end.

compare_g(Time1, Time2) ->
    Time1 > Time2, S0.

compare_ge(Time1, Time2) ->
    Time1 => Time2.
