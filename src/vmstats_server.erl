%%% Main worker for vmstats. This module sits in a loop fired off with
%%% timers with the main objective of routinely sending data to Sink.
-module(vmstats_server).
-behaviour(gen_server).
%% Interface
-export([start_link/2]).
%% Internal Exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3, terminate/2]).

-define(TIMER_MSG, '#interval').

-record(state, {sink :: atom(),
                key :: string(),
                tags = [] :: [{string(), string()}],
                memory_metrics :: [{erlang:memory_type(), atom()}],
                sched_time :: enabled | disabled,
                prev_sched :: [{integer(), integer(), integer()}],
                timer_ref :: reference(),
                interval :: integer(), % milliseconds
                prev_io :: {In::integer(), Out::integer()},
                prev_gc :: {GCs::integer(), Words::integer()}}
       ).
%%% INTERFACE
start_link(Sink, BaseKey) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, {Sink, BaseKey}, []).

%%% INTERNAL EXPORTS
init({Sink, BaseKey}) ->
    {ok, Interval} = application:get_env(vmstats, interval),
    {ok, KeySeparator} = application:get_env(vmstats, key_separator),
    {ok, SchedTimeEnabled} = application:get_env(vmstats, sched_time),
    {ok, MemoryMetrics} = application:get_env(vmstats, memory_metrics),

    Ref = erlang:start_timer(Interval, self(), ?TIMER_MSG),
    {{input, In}, {output, Out}} = erlang:statistics(io),
    {GCs, Words, _} = erlang:statistics(garbage_collection),

    State = #state{key = case BaseKey of
                           [] -> [];
                           _ -> [BaseKey, KeySeparator]
                         end,
                   memory_metrics = MemoryMetrics,
                   sink = Sink,
                   timer_ref = Ref,
                   interval = Interval,
                   prev_io = {In, Out},
                   prev_gc = {GCs, Words}},

    erlang:system_flag(scheduler_wall_time, true),
    case SchedTimeEnabled of
        true ->
            {ok, State#state{sched_time = enabled,
                             prev_sched = scheduler_wall_time()}};
        false ->
            {ok, State#state{sched_time = disabled}}
    end.

handle_call(_Msg, _From, State) ->
    {noreply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({timeout, R, ?TIMER_MSG}, State = #state{sink=Sink, key=K, memory_metrics=MM, interval=I, timer_ref=R, tags = Tags}) ->

  {Measurements, State2} =
    lists:foldl(fun(Fun, Acc) ->
                    Fun(Acc)
                end,
                [],
                [
                 fun(Acc) -> collect_processes(K, Tags, Acc) end,
                 fun(Acc) -> collect_ports(K, Tags, Acc) end,
                 fun(Acc) -> collect_atoms(K, Tags, Acc) end,
                 fun(Acc) -> collect_message_queue_len(K, Tags, Acc) end,
                 fun(Acc) -> collect_modules_loaded(K, Tags, Acc) end,
                 fun(Acc) -> collect_run_queue(K, Tags, Acc) end,
                 fun(Acc) -> collect_error_logger_backlog(K, Tags, Acc) end,
                 fun(Acc) -> collect_memory_stats(K, Tags, MM, Acc) end,
                 fun(Acc) -> collect_fragmentation(K, Tags, Acc) end,
                 fun(Acc) -> collect_reductions(K, Tags, Acc) end,
                 fun(Acc) -> collect_io_stats(Acc, State) end,
                 fun({Acc, S}) -> collect_gc_stats(Acc, S) end,
                 fun({Acc, S}) -> collect_sched_time_stats(Acc, S) end
                ]),

  Sink:collect(Measurements),

  Ref = erlang:start_timer(I, self(), ?TIMER_MSG),

  {noreply, State2#state{timer_ref = Ref}};

handle_info(_Msg, {state, _Key, _TimerRef, _Delay}) ->
    exit(forced_upgrade_restart);

handle_info(_Msg, {state, _Key, SchedTime, _PrevSched, _TimerRef, _Delay}) ->
    %% The older version may have had the scheduler time enabled by default.
    %% We could check for settings and preserve it in memory, but then it would
    %% be more confusing if the behaviour changes on the next restart.
    %% Instead, we show a warning and restart as usual.
    case {application:get_env(vmstats, sched_time), SchedTime} of
        {undefined, active} -> % default was on
            error_logger:warning_msg("vmstats no longer runs scheduler time by default. Restarting...");
        _ ->
            ok
    end,
    exit(forced_upgrade_restart);
handle_info(_Msg, State) ->
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

%% Returns the two timeslices as a ratio of each other,
%% as a percentage so that StatsD gets to print something > 1
wall_time_diff(T1, T2) ->
    [{I, Active2-Active1, Total2-Total1}
     || {{I, Active1, Total1}, {I, Active2, Total2}} <- lists:zip(T1,T2)].

collect_processes(MeasurementPrefix, Tags, Acc) ->
  Fields = [{"count", erlang:system_info(process_count)},
            {"limit", erlang:system_info(process_limit)}],

  Measurement = {[MeasurementPrefix, "processes"], Fields, Tags},

  [Measurement | Acc].

collect_ports(MeasurementPrefix, Tags, Acc) ->
  Fields = [{"count", erlang:system_info(port_count)},
            {"limit", erlang:system_info(port_limit)}],

  Measurement = {[MeasurementPrefix, "ports"], Fields, Tags},

  [Measurement | Acc].

collect_atoms(MeasurementPrefix, Tags, Acc) ->
  %% Atom count, working only on Erlang 20+
  try erlang:system_info(atom_count) of
      AtomCount -> [{[MeasurementPrefix, "atoms"], [{"count", AtomCount}], Tags} | Acc]
  catch
    _:badarg -> Acc
  end.

collect_message_queue_len(MeasurementPrefix, Tags, Acc) ->
  %% Messages in queues
  TotalMessages = lists:foldl(
                    fun(Pid, Total) ->
                        case process_info(Pid, message_queue_len) of
                          undefined -> Total;
                          {message_queue_len, Count} -> Count + Total
                        end
                    end,
                    0,
                    processes()
                   ),
  [{[MeasurementPrefix, "queue_len"], [{"value", TotalMessages}], Tags} | Acc].

collect_modules_loaded(MeasurementPrefix, Tags, Acc) ->
  %% Modules loaded
  [{[MeasurementPrefix, "modules"], [{"count", length(code:all_loaded())}], Tags} | Acc].

collect_run_queue(MeasurementPrefix, Tags, Acc) ->
  %% Queued up processes (lower is better)
  [{[MeasurementPrefix,"run_queue"], [{"value", erlang:statistics(run_queue)}], Tags} | Acc].

collect_error_logger_backlog(MeasurementPrefix, Tags, Acc) ->
  %% Error logger backlog (lower is better)
  case whereis(error_logger) of
    undefined ->
      Acc;
    Pid ->
      {_, MQL} = process_info(Pid, message_queue_len),
      [{[MeasurementPrefix, "error_logger_queue_len"], [{"value", MQL}], Tags} | Acc]
  end.

%% There are more options available, but not all were kept.
%% Memory usage is in bytes.
collect_memory_stats(MeasurementPrefix, Tags, MemoryMetrics, Acc) ->
  Fields = [
            {
             atom_to_list(Name),
             case Metric of
               {recon_alloc, {memory, Type}} when Type == allocated;
                                                  Type == used;
                                                  Type == unused ->
                 recon_alloc:memory(Type);

               {recon_alloc, fragmentation} ->
                 recon_alloc:fragmentation(current);

               _ ->
                 erlang:memory(Metric)
             end
             }
            || {Name, Metric} <- MemoryMetrics],

  [{[MeasurementPrefix, "memory"], Fields, Tags} | Acc].

collect_reductions(MeasurementPrefix, Tags, Acc) ->
  %% Reductions across the VM, excluding current time slice, already incremental
  {_, Reds} = erlang:statistics(reductions),
  [{[MeasurementPrefix, "reductions"], [{"value", Reds}], Tags} | Acc].

collect_io_stats(Acc, State = #state{prev_io = {PrevIn, PrevOut},
                                     key = MeasurementPrefix,
                                     tags = Tags}) ->

  {{input, In}, {output, Out}} = erlang:statistics(io),

  State2 = State#state{prev_io = {In, Out}},

  Fields = [{"bytes_in", In - PrevIn},
            {"bytes_out", Out - PrevOut}],

  {[{[MeasurementPrefix, "io"], Fields, Tags} | Acc], State2}.

collect_gc_stats(Acc, State = #state{prev_gc = {PrevGCs, PrevWords},
                                     key = MeasurementPrefix,
                                     tags = Tags}) ->

  {GCs, Words, _} = erlang:statistics(garbage_collection),

  State2 = State#state{prev_gc = {GCs, Words}},

  Fields = [{"count", GCs - PrevGCs},
            {"words_reclaimed", Words - PrevWords}],

  {[{[MeasurementPrefix, "gc"], Fields, Tags} | Acc], State2}.

collect_sched_time_stats(Acc, State = #state{sched_time = enabled,
                                             prev_sched = PrevSched,
                                             key = MeasurementPrefix,
                                             tags = Tags}) ->
  Sched = scheduler_wall_time(),

  State2 = State#state{prev_sched = Sched},

  Acc2 = lists:foldl(fun({Sid, Active, Total}, InnerAcc) ->
                         LSid = integer_to_list(Sid),

                         Fields = [{"active", Active},
                                   {"total", Total}],

                         [{[MeasurementPrefix, "scheduler"], Fields, [{"sid", LSid} | Tags]} | InnerAcc]
                     end,
                     Acc,
                     wall_time_diff(PrevSched, Sched)),

  {Acc2, State2};

collect_sched_time_stats(Acc, State) ->
  {Acc, State}.

collect_fragmentation(MeasurementPrefix, Tags, Acc) ->

  Frags = recon_alloc:fragmentation(current),

  Acc2 = lists:foldl(fun({{Allocator, Sid}, Fields}, InnerAcc) ->

                         Fields2 = [{atom_to_list(Name), Value} || {Name, Value} <- Fields],
                         Tags2 = [{"allocator", atom_to_list(Allocator)},
                                  {"sid", integer_to_list(Sid)} | Tags],

                         [{[MeasurementPrefix, "fragmentation"], Fields2, Tags2} | InnerAcc]
                     end,
                     Acc,
                     Frags),

  Acc2.

%% [{{eheap_alloc,1},
%%   [{sbcs_usage,0.859430539099526},
%%    {mbcs_usage,0.3064538204308712},
%%    {sbcs_block_size,742768},
%%    {sbcs_carriers_size,864256},
%%    {mbcs_block_size,1325528},
%%    {mbcs_carriers_size,4325376}]},

scheduler_wall_time() ->
    lists:sort(erlang:statistics(scheduler_wall_time)).
