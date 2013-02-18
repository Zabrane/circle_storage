-module(circle_storage_util).
-export([pmap/3, pmap/4, spawn_worker/4]).

pmap(Module, Fun, List) ->
    pmap(Module, Fun, List, 5000).

pmap(Module, Fun, List, Timeout) ->
   Pid_Ref_list = [erlang:spawn_monitor(?MODULE, spawn_worker, [self(), Module, Fun, Args]) || Args <- List],
   [wait_result(Pid, Ref, Timeout) || {Pid, Ref} <- Pid_Ref_list ].

spawn_worker(Parent, Module, Fun, Args) ->
    %% erlang:display([Parent, Module, Fun, Args]),
    Result = erlang:apply(Module, Fun, Args),
    Parent ! {self(), Result}.

wait_result(Pid, Ref, Timeout) ->
    receive
        {'DOWN', Ref, _, _, normal} ->
            receive {Pid, Result} -> Result end;
        {'DOWN', Ref, _, _, Reason} -> exit(Reason)
    after Timeout ->
            exit(time_out)
    end.
