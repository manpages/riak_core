%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.

%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
-module(riak_core_handoff_manager).
-behaviour(gen_server).

%% gen_server api
-export([start_link/0,
         init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3
        ]).

%% exclusion api
-export([add_exclusion/2,
         get_exclusions/1,
         remove_exclusion/2
        ]).

%% handoff api
-export([add_outbound/4,
         add_inbound/1,
         add_repair/1,
         status/0,
         set_concurrency/1,
         kill_handoffs/0
        ]).

-include("riak_core_handoff.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type mod()   :: atom().
-type index() :: integer().

-record(handoff_status,
        { modindex      :: {mod(),index()},
          node          :: atom(),
          direction     :: inbound | outbound,
          transport_pid :: pid(),
          timestamp     :: tuple(),
          status        :: any(),
          vnode_pid     :: pid() | undefined,
          origin        :: term()
        }).
-type handoff_status() :: #handoff_status{}.
-type handoffs() :: [handoff_status()].

-record(repair,
        { partition :: index(),
          minus_one_hs :: handoff_status(),
          plus_one_hs :: handoff_status()
        }).
-type repair() :: #repair{}.
-type repairs() :: [repair()].

-record(state,
        { excl,
          handoffs :: handoffs(),
          repairs :: [repair()]
        }).

%% this can be overridden with riak_core handoff_concurrency
-define(HANDOFF_CONCURRENCY,1).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    {ok, #state{excl=ordsets:new(), handoffs=[], repairs=[]}}.

%% handoff_manager API

add_outbound(Module,Idx,Node,VnodePid) ->
    gen_server:call(?MODULE,{add_outbound,Module,Idx,Node,VnodePid}).

add_inbound(SSLOpts) ->
    gen_server:call(?MODULE,{add_inbound,SSLOpts}).

%% @doc Add a repair request for the given `Partition'.
-spec add_repair(index()) -> ok | repair_in_progress.
add_repair(Partition) ->
    %% Fwd the repair req to the partition's owner to guarentee that
    %% there is only one req per partition.
    {ok, Ring} = riak_core_ring_manager:get_raw_ring(),
    Owner = riak_core_ring:index_owner(Ring, Partition),
    gen_server:call({?MODULE, Owner}, {add_repair, Partition}).

status() ->
    gen_server:call(?MODULE,status).

set_concurrency(Limit) ->
    gen_server:call(?MODULE,{set_concurrency,Limit}).

kill_handoffs() ->
    set_concurrency(0).

add_exclusion(Module, Index) ->
    gen_server:cast(?MODULE, {add_exclusion, {Module, Index}}).

remove_exclusion(Module, Index) ->
    gen_server:cast(?MODULE, {del_exclusion, {Module, Index}}).

get_exclusions(Module) ->
    gen_server:call(?MODULE, {get_exclusions, Module}, infinity).

%% gen_server API

handle_call({get_exclusions, Module}, _From, State=#state{excl=Excl}) ->
    Reply =  [I || {M, I} <- ordsets:to_list(Excl), M =:= Module],
    {reply, {ok, Reply}, State};
handle_call({add_outbound,Mod,Idx,Node,Pid},_From,State=#state{handoffs=HS}) ->
    case send_handoff(Mod, {Idx, Idx}, Node, Pid, HS) of
        {ok,Handoff=#handoff_status{transport_pid=Sender}} ->
            {reply,{ok,Sender},State#state{handoffs=HS ++ [Handoff]}};
        {false,_ExistingHandoff=#handoff_status{transport_pid=Sender}} ->
            {reply,{ok,Sender},State};
        Error ->
            {reply,Error,State}
    end;
handle_call({add_inbound,SSLOpts},_From,State=#state{handoffs=HS}) ->
    case receive_handoff(SSLOpts) of
        {ok,Handoff=#handoff_status{transport_pid=Receiver}} ->
            {reply,{ok,Receiver},State#state{handoffs=HS ++ [Handoff]}};
        Error ->
            {reply,Error,State}
    end;

handle_call({add_repair, Partition}, _From, State=#state{handoffs=HS,
                                                         repairs=RS}) ->
    case get_repair(Partition, RS) of
        none ->
            Repair = repair(Partition, HS),
            RS2 = RS ++ [Repair],
            State2 = State#state{repairs=RS2},
            lager:info("add_repair ~p/~p", [node(), Partition]),
            {reply, ok, State2};
        #repair{} ->
            {reply, repair_in_progress, State}
    end;

handle_call({send_handoff, Mod, {Src, Target}, Node, {CH, NValMap}},
            _From,
            State=#state{handoffs=HS}) ->
    Filter = riak_search_utils:repair_filter(Target, CH, NValMap),
    {ok, SrcPid} = riak_core_vnode_manager:get_vnode_pid(Src, riak_search_vnode),
    case send_handoff(Mod, {Src, Target}, Node, Filter, SrcPid, HS, Node) of
        {ok, Handoff} ->
            {reply, {ok, Handoff}, State#state{handoffs=HS ++ [Handoff]}};
        {false, Handoff} ->
            {reply, {ok, Handoff}, State}
    end;

handle_call(status,_From,State=#state{handoffs=HS}) ->
    Handoffs=[{M,N,D,active,S} ||
                 #handoff_status{modindex=M,node=N,direction=D,status=S} <- HS],
    {reply, Handoffs, State};
handle_call({set_concurrency,Limit},_From,State=#state{handoffs=HS}) ->
    application:set_env(riak_core,handoff_concurrency,Limit),
    case Limit < erlang:length(HS) of
        true ->
            %% Note: we don't update the state with the handoffs that we're
            %% keeping because we'll still get the 'DOWN' messages with
            %% a reason of 'max_concurrency' and we want to be able to do
            %% something with that if necessary.
            {_Keep,Discard}=lists:split(Limit,HS),
            [erlang:exit(Pid,max_concurrency) ||
                #handoff_status{transport_pid=Pid} <- Discard],
            {reply, ok, State};
        false ->
            {reply, ok, State}
    end.


handle_cast({del_exclusion, {Mod, Idx}}, State=#state{excl=Excl}) ->
    {noreply, State#state{excl=ordsets:del_element({Mod, Idx}, Excl)}};
handle_cast({add_exclusion, {Mod, Idx}}, State=#state{excl=Excl}) ->
    {ok, Ring} = riak_core_ring_manager:get_raw_ring(),
    case riak_core_ring:my_indices(Ring) of
        [] ->
            %% Trigger a ring update to ensure the node shuts down
            riak_core_ring_events:ring_update(Ring);
        _ ->
            ok
    end,
    {noreply, State#state{excl=ordsets:add_element({Mod, Idx}, Excl)}}.

handle_info({handoff_finished, {_Mod, Target}, Handoff, _Reason},
            State=#state{repairs=RS}) ->
    %% NOTE: this msg will only come in for repair handoff
    R2 = case get_repair(Target, RS) of
             #repair{partition=Target, minus_one_hs=MO, plus_one_hs=PO}=R ->
                 if MO == Handoff -> R#repair{minus_one_hs=completed};
                    PO == Handoff -> R#repair{plus_one_hs=completed};
                    true -> throw({finished_handoff_matches_none, R, Handoff})
                 end
         end,
    case R2 of
        #repair{minus_one_hs=completed, plus_one_hs=completed} ->
            {noreply, State#state{repairs=remove_repair(R2, RS)}};
        _ ->
            {noreply, State#state{repairs=replace_repair(R2, RS)}}
    end;

handle_info({'DOWN', _Ref, process, Pid, Reason}, State=#state{handoffs=HS}) ->
    case lists:keytake(Pid,#handoff_status.transport_pid,HS) of
        {value,
         #handoff_status{modindex={M,I},direction=Dir,vnode_pid=Vnode,
                         origin=Origin}=Handoff,
         NewHS
        } ->
            WarnVnode =
                case Reason of
                    %% if the reason the handoff process died was anything other
                    %% than 'normal' we should log the reason why as an error
                    normal ->
                        false;
                    max_concurrency ->
                        lager:info("An ~w handoff of partition ~w ~w was terminated for reason: ~w~n", [Dir,M,I,Reason]),
                        true;
                    _ ->
                        lager:error("An ~w handoff of partition ~w ~w was terminated for reason: ~w~n", [Dir,M,I,Reason]),
                        true
                end,

            %% if we have the vnode process pid, tell the vnode why the
            %% handoff stopped so it can clean up its state
            case WarnVnode andalso is_pid(Vnode) of
                true ->
                    riak_core_vnode:handoff_error(Vnode,'DOWN',Reason);
                _ ->
                    case Origin of
                        none -> ok;
                        local -> self() ! {handoff_finished, {M, I}, Handoff, Reason};
                        _ -> {?MODULE, Origin} ! {handoff_finished, {M, I},
                                                  Handoff, Reason}
                    end,
                    ok
            end,

            %% removed the handoff from the list of active handoffs
            {noreply, State#state{handoffs=NewHS}};
        false ->
            {noreply, State}
    end;
handle_info(Info, State) ->
    io:format(">>>>> ~w~n", [Info]),
    {noreply, State}.


terminate(_Reason, _State) ->
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%
%% @private functions
%%

get_concurrency_limit () ->
    app_helper:get_env(riak_core,handoff_concurrency,?HANDOFF_CONCURRENCY).

%% @private
%%
%% @doc Get the corresponding repair entry in `Repairs', if one
%% exists, for the given `Partition'.
-spec get_repair(index(), repairs()) -> repair() | none.
get_repair(Partition, Repairs) ->
    case lists:keyfind(Partition, #repair.partition, Repairs) of
        false -> none;
        Val -> Val
    end.

%% @private
%%
%% @doc Remove the repair entry.
-spec remove_repair(repair(), repairs()) -> repairs().
remove_repair(Repair, Repairs) ->
    lists:keydelete(Repair#repair.partition, #repair.partition, Repairs).

%% @private
%%
%% @doc Replace the matching repair entry with `Repair'.
-spec replace_repair(repair(), repairs()) -> repairs().
replace_repair(Repair, Repairs) ->
    lists:keyreplace(Repair#repair.partition, #repair.partition,
                     Repairs, Repair).

%% true if handoff_concurrency (inbound + outbound) hasn't yet been reached
handoff_concurrency_limit_reached () ->
    Receivers=supervisor:count_children(riak_core_handoff_receiver_sup),
    Senders=supervisor:count_children(riak_core_handoff_sender_sup),
    ActiveReceivers=proplists:get_value(active,Receivers),
    ActiveSenders=proplists:get_value(active,Senders),
    get_concurrency_limit() =< (ActiveReceivers + ActiveSenders).

%% @doc Initiate the repair handoffs for the given `Partition'.
-spec repair(index(), handoffs()) -> repair().
repair(Partition, HS) ->
    {ok, Ring} = riak_core_ring_manager:get_raw_ring(),
    CH = element(4, Ring),
    NValMap = [{S:name(), S:n_val()} ||
                  S <- riak_search_config:get_all_schemas()],
    [_, {_PMinusOne, _PMinusOneNode}=MOne] =
        chash:predecessors(<<Partition:160/integer>>, CH, 2),
    [{_PPlusOne, _PPlusOneNode}=POne] =
        chash:successors(<<Partition:160/integer>>, CH, 1),
    MinusOneHS = repair_handoff(MOne, Partition, {CH, NValMap}, HS),
    PlusOneHS = repair_handoff(POne, Partition, {CH, NValMap}, HS),
    #repair{partition=Partition, minus_one_hs=MinusOneHS, plus_one_hs=PlusOneHS}.

%% @private
%%
%% @doc Initiate a repair handoff from `Src' to `Target'.  Return the
%% `handoff_status' entry.
%%
%% TODO: return {local|remote, HS}?
%%
%% NOTE: This function assumes it runs on the owner of `Target'.
-spec repair_handoff({index(), node()}, index(),
                     {chash:chash(), list()}, handoffs()) ->
                            {ok, handoff_status()}.
repair_handoff({Src, SrcOwner}, Target, {CH, NValMap}, HS) ->
    case SrcOwner == node() of
        true ->
            {ok, SrcPid} = riak_core_vnode_manager:get_vnode_pid(Src,
                                                                 riak_search_vnode),
            Filter = riak_search_utils:repair_filter(Target, CH, NValMap),
            {ok, Handoff} = send_handoff(riak_search_vnode, {Src, Target},
                                         node(), Filter, SrcPid, HS, node()),
            Handoff;
        false ->
            {ok, Handoff} = gen_server:call({?MODULE, SrcOwner},
                                            {send_handoff, riak_search_vnode,
                                             {Src, Target}, node(),
                                             {CH, NValMap}}),
            Handoff
    end.

send_handoff(Mod, {Src, Target}, Node, Vnode, HS) ->
    send_handoff(Mod, {Src, Target}, Node, Vnode, none, HS, none).

%% @private
%%
%% @doc Start a handoff sender to transfer data from `Src' to `Target'.
%%
%% NOTE: `Origin' should be `none' for non-repair handoff.
send_handoff(Mod, {Src, Target}, Node, Filter, Vnode, HS, Origin) ->
    case handoff_concurrency_limit_reached() of
        true ->
            {error, max_concurrency};
        false ->
            ShouldHandoff=
                case lists:keyfind({Mod, Target},#handoff_status.modindex,HS) of
                    false ->
                        true;
                    Handoff=#handoff_status{node=Node,vnode_pid=Vnode} ->
                        {false,Handoff};
                    #handoff_status{transport_pid=Sender} ->
                        %% found a running handoff with a different vnode
                        %% source or a different arget ndoe, kill the current
                        %% one and the new one will start up
                        erlang:exit(Sender,resubmit_handoff_change),
                        true
                end,

            case ShouldHandoff of
                true ->
                    %% start the sender process
                    {ok,Pid}=riak_core_handoff_sender_sup:start_sender(Node,
                                                                       Mod,
                                                                       {Src, Target},
                                                                       Filter,
                                                                       Vnode),
                    erlang:monitor(process,Pid),

                    %% successfully started up a new sender handoff
                    {ok, #handoff_status{ transport_pid=Pid,
                                          direction=outbound,
                                          timestamp=now(),
                                          node=Node,
                                          modindex={Mod, Target},
                                          vnode_pid=Vnode,
                                          status=[],
                                          origin=Origin
                                        }
                    };

                %% handoff already going, just return it
                AlreadyExists={false,_CurrentHandoff} ->
                    AlreadyExists
            end
    end.

%% spawn a receiver process
receive_handoff (SSLOpts) ->
    case handoff_concurrency_limit_reached() of
        true ->
            {error, max_concurrency};
        false ->
            {ok,Pid}=riak_core_handoff_receiver_sup:start_receiver(SSLOpts),
            erlang:monitor(process,Pid),

            %% successfully started up a new receiver
            {ok, #handoff_status{ transport_pid=Pid,
                                  direction=inbound,
                                  timestamp=now(),
                                  modindex={undefined,undefined},
                                  node=undefined,
                                  status=[],
                                  origin=none
                                }
            }
    end.

%%
%% EUNIT tests...
%%

-ifdef (TEST_BROKEN_AZ_TICKET_1042).

handoff_test_ () ->
    {spawn,
     {setup,

      %% called when the tests start and complete...
      fun () -> {ok,Pid}=start_link(), Pid end,
      fun (Pid) -> exit(Pid,kill) end,

      %% actual list of test
      [?_test(simple_handoff())
      ]}}.

simple_handoff () ->
    ?assertEqual([],status()),

    %% clear handoff_concurrency and make sure a handoff fails
    ?assertEqual(ok,set_concurrency(0)),
    ?assertEqual({error,max_concurrency},add_inbound([])),
    ?assertEqual({error,max_concurrency},add_outbound(riak_kv,0,node(),self())),

    %% allow for a single handoff
    ?assertEqual(ok,set_concurrency(1)),

    %% done
    ok.

-endif.
