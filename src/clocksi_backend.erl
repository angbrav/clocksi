-module(clocksi_backend).

-callback start(State :: term()) -> UptState :: term().

-callback put(Updates :: list() | {term(), term()}, Version :: term(), State :: term()) -> {ok, UptState :: term()} | {{error, Reason :: atom()}, UpState :: term()}.

-callback get(Key :: term(), Version :: term(), State :: term()) -> {{ok, Value :: term()}, UpState :: term()} | {{error, Reason :: atom()}, UpState :: term()}.
