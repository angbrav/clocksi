-define(PRINT(Var), io:format("DEBUG: ~p:~p - ~p~n~n ~p~n~n", [?MODULE, ?LINE, ??Var, Var])).
-define(CLOCKS_IMPLEMENTATION, physical_clocks).
-define(BACKEND, ets_backend).
-define(BUCKET, clock_si).
-define(CLOCKSI_MASTER, clocksi_master_vnode).

-record(vnode_state, {partition,
                      clocks,
                      backend,
                      store,
                      prepared_key,
                      tx_pending_for_commit,
                      n_to_notify_tx,
                      pending_reads,
                      n_to_read,
                      prepared_tx,
                      pending_certification,
                      committed_key,
                      committed_write_set}).

-record(coord_state, {from,
                      key :: term(),
                      tx_id :: {term(), pid()},
                      snapshot_time = 0 :: term(),
                      updated_partitions = [] :: list(),
                      partitions_buffer_keys = dict:new() :: dict(),
                      partitions_buffer_values = dict:new() :: dict(),
                      updates_buffer = dict:new() :: dict(),
                      reads_buffer = dict:new() :: dict(),
                      num_ack = 0 :: integer(),
                      prepare_time :: term(),
                      commit_time :: term(),
                      clocks :: atom(),
                      client_clock :: term(),
                      state :: atom()}).
