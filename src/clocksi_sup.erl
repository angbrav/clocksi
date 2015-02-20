-module(clocksi_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init(_Args) ->
    VMaster = { clocksi_vnode_master,
                  {riak_core_vnode_master, start_link, [clocksi_vnode]},
                  permanent, 5000, worker, [riak_core_vnode_master]},
    ClockSISup = {clocksi_interactive_tx_coord_sup,
                    {clocksi_interactive_tx_coord_sup, start_link, []},
                    permanent, 5000, supervisor, [clocksi_interactive_tx_coord_sup]},

    { ok,
        { {one_for_one, 5, 10},
          [VMaster,
           ClockSISup]}}.
