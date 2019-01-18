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

-record(state,
        {
         sink :: atom()
        ,key :: string()
        ,tags = [] :: [{string(), string()}]
        ,memory_metrics :: [{erlang:memory_type(), atom()}]
        ,sched_time :: enabled | disabled
        ,timer_ref :: reference()
        ,interval :: integer() % milliseconds
        ,word_size :: non_neg_integer()
        }
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

  WordSize = erlang:system_info(wordsize),

   Ref = erlang:start_timer(Interval, self(), ?TIMER_MSG),

   State = #state{key = case BaseKey of
                          [] -> [];
                          _ -> [BaseKey, KeySeparator]
                        end,
                  memory_metrics = MemoryMetrics,
                  sink = Sink,
                  timer_ref = Ref,
                  interval = Interval,
                  word_size = WordSize
                 },

   erlang:system_flag(scheduler_wall_time, true),
   case SchedTimeEnabled of
     true ->
       {ok, State#state{sched_time = enabled}};
     false ->
       {ok, State#state{sched_time = disabled}}
   end.

handle_call(_Msg, _From, State) ->
  {noreply, State}.

handle_cast(_Msg, State) ->
  {noreply, State}.

handle_info({timeout, R, ?TIMER_MSG}, State = #state{sink=Sink, key=K, memory_metrics=MM, interval=I, timer_ref=R, tags = Tags}) ->

  Measurements =
    lists:foldl(fun(Fun, Acc) ->
                    Fun(Acc)
                end,
                [],
                [
                 fun(Acc) -> collect_system_info(K, Tags, Acc) end,
                 fun(Acc) -> collect_processes(K, Tags, Acc) end,
                 fun(Acc) -> collect_ports(K, Tags, Acc) end,
                 fun(Acc) -> collect_atoms(K, Tags, Acc) end,
                 fun(Acc) -> collect_message_queue_len(K, Tags, Acc) end,
                 fun(Acc) -> collect_modules_loaded(K, Tags, Acc) end,
                 fun(Acc) -> collect_run_queue(Acc, State) end,
                 fun(Acc) -> collect_error_logger_backlog(K, Tags, Acc) end,
                 fun(Acc) -> collect_memory_stats(K, Tags, MM, Acc) end,
                 fun(Acc) -> collect_allocator_metrics(K, Tags, Acc) end,
                 fun(Acc) -> collect_reductions(K, Tags, Acc) end,
                 fun(Acc) -> collect_io_stats(Acc, State) end,
                 fun(Acc) -> collect_gc_stats(Acc, State) end,
                 fun(Acc) -> collect_sched_time_stats(Acc, State) end,
                 fun(Acc) -> collect_misc_stats(K, Tags, Acc) end
                ]),

  Sink:collect(Measurements),

  Ref = erlang:start_timer(I, self(), ?TIMER_MSG),

  {noreply, State#state{timer_ref = Ref}};

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

collect_system_info(MeasurementPrefix, Tags, Acc) ->

  Fields = lists:foldl(fun(Name, FieldsAcc) ->
                           try
                             case erlang:system_info(Name) of
                               unknown -> FieldsAcc;
                               true -> [{Name, 1} | FieldsAcc];
                               false -> [{Name, 0} | FieldsAcc];
                               Value -> [{Name, Value} | FieldsAcc]
                             end
                           catch
                             error:badarg -> FieldsAcc
                           end
                       end,
                       [],
                       [dirty_cpu_schedulers,
                        dirty_cpu_schedulers_online,
                        dirty_io_schedulers,
                        ets_limit,
                        logical_processors,
                        logical_processors_available,
                        logical_processors_online,
                        port_count,
                        port_limit,
                        process_count,
                        process_limit,
                        schedulers,
                        schedulers_online,
                        smp_support,
                        thread_support,
                        thread_pool_size,
                        time_correction,
                        atom_count,
                        atom_limit]),

  Measurement = {[MeasurementPrefix, "system_info"], Fields, Tags},

  [Measurement | Acc].

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

collect_run_queue(Acc, #state{key = MeasurementPrefix,
                              tags = Tags}) ->

  %% Queued up processes (lower is better)
  SchedulerCount = erlang:system_info(schedulers_online),
  RunQueues = erlang:statistics(run_queue_lengths_all),
  TotalRunQueues = erlang:statistics(total_run_queue_lengths),

  {_, Acc2} = lists:foldl(fun(RunQueue, {N, InnerAcc}) ->

                              Fields = [{sched, RunQueue}],
                              Tag = if
                                      N < SchedulerCount ->
                                        {sched, N};
                                      N == SchedulerCount ->
                                        {sched, dirty_cpu};
                                      N == SchedulerCount + 1 ->
                                        {sched, dirty_io}
                                    end,
                              Tags2 = [Tag | Tags],

                              Measurement = {[MeasurementPrefix, "run_queue"], Fields, Tags2},

                              {N + 1, [Measurement | InnerAcc]}
                          end,
                          {0, Acc},
                          RunQueues),

  %% TODO - scheduler utilization - node name (latter should be done by influx_vm_stats)

  [{[MeasurementPrefix,"run_queue"], [{total, TotalRunQueues}], Tags} | Acc2].

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

  Fields2 = [{ets_tables, length(ets:all())},
             {dets_tables, length(dets:all())} | Fields],

  [{[MeasurementPrefix, "memory"], Fields2, Tags} | Acc].

collect_reductions(MeasurementPrefix, Tags, Acc) ->
  %% Reductions across the VM, excluding current time slice, already incremental
  {_, Reds} = erlang:statistics(reductions),
  [{[MeasurementPrefix, "reductions"], [{"value", Reds}], Tags} | Acc].

collect_io_stats(Acc, #state{key = MeasurementPrefix,
                             tags = Tags}) ->

  {{input, In}, {output, Out}} = erlang:statistics(io),

  Fields = [{"bytes_in", In},
            {"bytes_out", Out}],

  [{[MeasurementPrefix, "io"], Fields, Tags} | Acc].

collect_gc_stats(Acc, #state{key = MeasurementPrefix,
                             tags = Tags,
                             word_size = WordSize}) ->

  {GCs, WordsReclaimed, _} = erlang:statistics(garbage_collection),


  Fields = [{"count", GCs},
            {"words_reclaimed", WordsReclaimed},
            {"bytes_reclaimed", WordsReclaimed * WordSize}
           ],

  [{[MeasurementPrefix, "gc"], Fields, Tags} | Acc].

collect_sched_time_stats(Acc, #state{sched_time = enabled,
                                     key = MeasurementPrefix,
                                     tags = Tags}) ->
  Sched = scheduler_wall_time(),

  Acc2 = lists:foldl(fun({Sid, Active, Total}, InnerAcc) ->
                         LSid = integer_to_list(Sid),

                         Fields = [{"active", Active},
                                   {"total", Total}],

                         [{[MeasurementPrefix, "scheduler"], Fields, [{"sid", LSid} | Tags]} | InnerAcc]
                     end,
                     Acc,
                     Sched),

  Acc2;

collect_sched_time_stats(Acc, _State) ->
  Acc.

collect_misc_stats(MeasurementPrefix, Tags, Acc) ->
  {ContextSwitches, _} = erlang:statistics(context_switches),
  {Runtime, _} = erlang:statistics(runtime),
  {WallclockTime, _} = erlang:statistics(wall_clock),

  Fields = [{"context_switches", ContextSwitches},
            {"runtime", Runtime},
            {"walltime", WallclockTime}
           ],

  Measurement = {[MeasurementPrefix, "misc"], Fields, Tags},

  [Measurement | Acc].

%% collect_allocator_metrics(MeasurementPrefix, Tags, Acc) ->

%%   Frags = recon_alloc:fragmentation(current),

%%   Acc2 = lists:foldl(fun({{Allocator, Sid}, Fields}, InnerAcc) ->

%%                          Fields2 = [{atom_to_list(Name), Value} || {Name, Value} <- Fields],
%%                          Tags2 = [{"allocator", atom_to_list(Allocator)},
%%                                   {"sid", integer_to_list(Sid)} | Tags],

%%                          [{[MeasurementPrefix, "fragmentation"], Fields2, Tags2} | InnerAcc]
%%                      end,
%%                      Acc,
%%                      Frags),

%%   Acc2.

%% Adapted from https://github.com/deadtrickster/prometheus.erl
collect_allocator_metrics(MeasurementPrefix, Tags, Acc) ->

  lists:foldl(fun({{Allocator, Instance}, AllocatorInfo}, InnerAcc) ->
                  lists:foldl(fun({CarrierType, CarrierInfo}, InnerInnerAcc) when (CarrierType =:= mbcs) orelse (CarrierType =:= mbcs_pool) orelse (CarrierType =:= sbcs) ->

                                  Fields = [{atom_to_list(Key), element(2, lists:keyfind(Key, 1, CarrierInfo))}
                                            || Key <- [blocks, blocks_size, carriers, carriers_size]],

                                  Tags2 = [{"allocator", Allocator},
                                           {"instance", Instance},
                                           {"carrier_type", CarrierType} | Tags],

                                  [{[MeasurementPrefix, "carrier"], Fields, Tags2} | InnerInnerAcc];
                                 (_, InnerInnerAcc) ->
                                  InnerInnerAcc
                              end,
                              InnerAcc,
                              AllocatorInfo)
              end,
              Acc,
              allocators()).

allocators() ->
    Allocators = erlang:system_info(alloc_util_allocators),
    %% versions is deleted in order to allow the use of the orddict api,
    %% and never really having come across a case where it was useful to know.
    [{{A, N}, lists:sort(proplists:delete(versions, Props))} ||
        A <- Allocators,
        Allocs <- [erlang:system_info({allocator, A})],
        Allocs =/= false,
        {_, N, Props} <- Allocs].

scheduler_wall_time() ->
  lists:sort(erlang:statistics(scheduler_wall_time)).
