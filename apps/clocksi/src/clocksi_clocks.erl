-module(clocksi_clocks).

-callback get_snapshot_time(State :: term()) -> {Snapshot :: term(), UptState :: term()}.

-callback get_commit_client_clock(State :: term()) -> {ClientClock :: term(), UptState :: term()}.

-callback get_prepare_time(State :: term()) -> {PrepareTime :: term(), UptState :: term()}.

-callback wait_until_safe(Threshold :: term(), State :: term()) -> UptState :: term().

-callback compare_g(Time1 :: term(), Time2 :: term()) -> Result :: boolean().

-callback compare_ge(Time1 :: term(), Time2 :: term()) -> Result :: boolean().
