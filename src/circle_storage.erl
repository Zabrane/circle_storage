%%%-------------------------------------------------------------------
%%% @copyright 2013 ShopEx Network Technology Co,.Ltd
%%% File : circle_storage.erl
%%% Author : filebat <markfilebat@126.com>
%%% Description :
%%% --
%%% Created : <2012-12-20>
%%% Updated: Time-stamp: <2013-02-18 11:45:22>
%%%-------------------------------------------------------------------
-module(circle_storage).
-behaviour(gen_server).
-define(SERVER, ?MODULE).

-define(RECORD_FIXED_SIZE, 512).
-define(PARALLEL_COUNT, 4).
-define(POS_NOT_FOUND, -1).
-record(state, {fd, dets, pos_start, pos_end, max_cell_counts, leveldb_ref, index=[]}).
-record(record, {time, error_group, error_type, server, client, node, url}).

-include("records.hrl").

%% -define(FORMAT_VER, 2.0). %% TODO

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/2, write/1, list/2]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-compile(export_all). %% TODO
%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link(File, Options) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [File, Options], []).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init([File, Options]) ->
    ets:new(circle_storage, [public, set, named_table]),
    S = open(File, Options),
    {ok, S}.

handle_call({state}, _From, State) ->
    {reply, State, State};

handle_call({write, Record}, _From, State) ->
    try
        State2 = write_data(Record, State),
        {reply, ok, State2}
    catch _:_Error ->
            io:format("error: ~p, ~p\n", [_Error, erlang:get_stacktrace()]),
            {reply, ok, State}
    end;

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

write(Data)->
    gen_server:call(?MODULE, {write, Data}, timer:seconds(30)).

state()->
    gen_server:call(?MODULE, {state}).

open(File, Options)->
    {ok, Fd} = file:open(File, [read, write, binary]),
    {ok, Dets} = dets:open_file(File++".idx", [{type, set}, {ram_file, true}]),
    {ok, Leveldb_ref} = eleveldb:open(File++".leveldb",
                                      [{create_if_missing, true}, {cache_size, 83886080}]),
    Max_cell_counts = proplists:get_value(max_cell_counts, Options, 10*1024*1024),
    ets:insert(circle_storage,{max_cell_counts, Max_cell_counts}),
    Index = proplists:get_value(index, Options, []),
    case get_value_from_dets(pos_start_end, Dets) of
        {ok, {Pos_start, Pos_end}} ->
            #state{fd=Fd, dets=Dets, pos_start=Pos_start, leveldb_ref=Leveldb_ref,
                   pos_end=Pos_end, max_cell_counts=Max_cell_counts, index=Index};
        _ ->
            error_logger:info_msg("[~p:~p] Rebuild dets index file:~p~n",[?MODULE, ?LINE, Dets]),
            dets:delete_all_objects(Dets),
            ok = dets:insert(Dets, {pos_start_end, {0, 1}}),
            ok = dets:sync(Dets),
            #state{fd=Fd, dets=Dets, pos_start=0, pos_end=1, leveldb_ref=Leveldb_ref,
                   max_cell_counts=Max_cell_counts, index=Index}
    end.

update_info(update_pos, {Pos_start, Pos_end, Max_cell_counts}) ->
    case (Pos_end + 1) rem Max_cell_counts of
        Pos_start ->
            error_logger:info_msg("[~p:~p] circle is full~n",[?MODULE, ?LINE]),
            {ok, {((Pos_start+1) rem Max_cell_counts),
                  ((Pos_end+1) rem Max_cell_counts)}};
        _ -> {ok, {Pos_start, ((Pos_end+1) rem Max_cell_counts)}}
    end;
update_info(append_record, {Fd, Pos, Record}) ->
    Bin = term_to_binary(Record),
    %% erlang:display({?LINE, Bin}),

    Len = erlang:byte_size(Bin),
    Data = <<Len:16/integer, Bin/binary>>,
    Pad_bits = (?RECORD_FIXED_SIZE - erlang:byte_size(Data))*8,
    true = (Pad_bits>=0), %% TODO: defensive check
    %% erlang:display({Pos, Pad_bits, erlang:byte_size(Data), erlang:byte_size(<<Data/binary, 0:Pad_bits/integer>>)}),
    ok = file:pwrite(Fd, Pos, <<Data/binary, 0:Pad_bits/integer>>);
update_info(save_leveldb_data, {Ref, Pos, Data}) ->
    ok = eleveldb:put(Ref, term_to_binary(Pos), Data, []),
    eleveldb:fold(Ref, fun({_K, _V}, A) -> A end, [], [{fill_cache, true}]),
    ok;

%% sample format
%% dets:lookup("data/circle_storage.db.idx", {link_head, 7, <<"errors-4xx">>}).
%% dets:lookup("data/circle_storage.db.idx", {link_next, 7, Pos}).
update_info(update_index, {Log_item, Dets, Pos_current, Index_keys}) ->
    lists:foreach(fun(Index) ->
                          Value = element(Index, Log_item),
                          case get_value_from_dets({link_head, Index, Value}, Dets) of
                              {notfound, _} ->
                                  dets:insert(Dets, {{link_next, Index, Pos_current}, ?POS_NOT_FOUND});
                              {ok, Pos_head} ->
                                  dets:insert(Dets, {{link_next, Index, Pos_current}, Pos_head})
                          end,
                          dets:insert(Dets, {{link_head, Index, Value}, Pos_current})
                  end, Index_keys).

write_data(Log_item, S) when is_record(Log_item, log_item)->
    %% error_logger:info_msg("[~p:~p] write_data~n",[?MODULE, ?LINE]),
    Record = logitem_to_record(Log_item),

    ok = update_info(append_record, {S#state.fd, S#state.pos_end*?RECORD_FIXED_SIZE, Record}),

    ok = update_info(save_leveldb_data, {S#state.leveldb_ref, S#state.pos_end, Log_item#log_item.data}),
    %% update position of the circle
    {ok,{New_pos_start, New_pos_end}} =
        update_info(update_pos, {S#state.pos_start, S#state.pos_end, S#state.max_cell_counts}),

    %% update index
    update_info(update_index, {Log_item, S#state.dets, S#state.pos_end, S#state.index}),

    S#state{pos_start=New_pos_start, pos_end=New_pos_end}.

%%% ###############################################
find({Pos, Limit, Index, Filters}, L, S) ->
    %% disk prefetch is normally 256K, thus we set the prefetch count as 500
    find({Pos, Limit, Index, Filters}, L, S, 500).

find({Pos, 0, Index, Filters}, L, S, Read_count) ->
    L;
find({Pos, Limit, Index, Filters}, L, S, Read_count) ->
    Count = circle_record_count(Pos, S#state.pos_start, S#state.pos_end, S#state.max_cell_counts),
    error_logger:info_msg("[~p:~p] find. Pos:~p, Limit:~p, Read_count:~p, Count:~p~n", [?MODULE, ?LINE, Pos, Limit, Read_count, Count]),
    Read_count_new =
        case Count > Read_count of
            true -> Read_count;
            _ -> Count
        end,

    case Limit > Count of
        true -> find({Pos, Count, Index, Filters}, L, S, Read_count_new);
        _ ->
            Pos_new = add_step(Pos, -Read_count_new, S),

            Zip_list = read_records(Pos_new, Read_count_new, Index),
            %% error_logger:info_msg("[~p:~p] length(Zip_list):~p~n",[?MODULE, ?LINE, length(Zip_list)]),
            List = filter_result(Zip_list, Filters),

            %% error_logger:info_msg("[~p:~p] List:~p~n",[?MODULE, ?LINE, List]),
            Result_count = length(List),
            case Limit > Result_count of
                %% TODO
                true -> find({Pos_new, Limit - Result_count, Index, Filters}, List++L, S, Read_count_new);
                _ -> List
            end
    end.

filter_result(Zip_list, Filters) ->
    Arg_list = lists:map(fun(V) -> [Filters, V] end, Zip_list),

    %% use map-reduce to fasten the process of filtering
    Result_list = circle_storage_util:pmap(?MODULE, match_filter, Arg_list),

    %% error_logger:info_msg("[~p:~p] Result_list:~p~n",[?MODULE, ?LINE, Result_list]),
    %% remove the mismatched items
    List = lists:reverse(
             lists:foldl(fun({K, Item}, List_t) ->
                                 case K of
                                     true -> [Item | List_t];
                                     false -> List_t
                                 end
                         end, [], Result_list)),
    List.

list({0, Limit}, Options) when is_integer(Limit)->
    S = state(),
    Start = (S#state.pos_end -1 + S#state.max_cell_counts) rem S#state.max_cell_counts,
    list({Start, Limit}, Options);
list({Start, Limit}, Options) when is_integer(Limit)->
    S = state(),
    error_logger:info_msg("[~p:~p] Start:~p, Limit:~p, Options:~p, pos_end:~p~n",
                          [?MODULE, ?LINE, Start, Limit, Options, S#state.pos_end]),

    Index = proplists:get_value(index, Options, {}),
    Filters = proplists:get_value(filters, Options, []),

    Zip_list = find({Start, Limit, Index, Filters}, [], S),

    %% error_logger:info_msg("[~p:~p] zip_list:~p~n",[?MODULE, ?LINE, Zip_list]),

    %% fill data from leveldb
    L = lists:foldl(fun({Pos, Logitem}, List_t) ->
                            case eleveldb:get(S#state.leveldb_ref, term_to_binary(Pos), []) of
                                {ok, Value} -> Value, [Logitem#log_item{data = Value} | List_t];
                                not_found -> error_logger:error_msg("[~p:~p] Pos:~p, not_found~n",[?MODULE, ?LINE, Pos]),
                                             List_t;
                                {error, Reason} ->
                                    error_logger:error_msg("[~p:~p] error:~p~n",[?MODULE, ?LINE, Reason]),
                                    List_t
                            end
                    end, [], Zip_list),
    %% error_logger:info_msg("[~p:~p] L:~p~n",[?MODULE, ?LINE, L]),
    lists:map(fun(Item) ->
                      Id = term_to_binary(random:seed(erlang:now())), %% TODO
                      {Id, Item}
              end, lists:reverse(L)).

%% dets:lookup("data/circle_storage.db.idx", {link_head, 7, <<"errors-4xx">>}).
%% dets:lookup("data/circle_storage.db.idx", {link_next, 7, Pos}).
read_records(Pos, Count, {}) ->
    S = state(),
    {ok, Read_count, Bin} = circle_file_read(S, Pos, Count),
    error_logger:info_msg("[~p:~p] after read, Pos:~p, Count:~p~n",[?MODULE, ?LINE, Pos, Count]),
    Max_cell_counts = S#state.max_cell_counts,
    Logitem_list = binary_to_logitems(Bin, []),
    Item_count = length(Logitem_list),
    Pos_list =
        [add_step(Pos, Offset, S) || Offset <- lists:seq(0, Item_count-1, 1)],
    lists:zip(Pos_list, lists:reverse(Logitem_list));
read_records(Pos, Count, {Index, Index_value}) ->
    S = state(),
    Fd = S#state.fd,
    Dets=S#state.dets,

    %% get link head for current index search
    case get_value_from_dets({link_head, Index, Index_value}, Dets) of
        {notfound, _} ->
            error_logger:info_msg("[~p:~p] link_head not found. Index:~p, Index_value:~p~n",
                                  [?MODULE, ?LINE, Index, Index_value]),
            [];

        {ok, Pos_head} ->
            %% find position list
            Pos_list = get_link_items(Pos_head, Dets, Index, Count, []),
            error_logger:info_msg("[~p:~p] Post_list:~p~n",[?MODULE, ?LINE, Pos_list]),
            %% find records using disk prefetch
            Record_list =
                lists:map(fun(Position) ->
                                  {ok, Bin} = file:pread(Fd, Position*?RECORD_FIXED_SIZE, ?RECORD_FIXED_SIZE),
                                  [Logitem] = binary_to_logitems(Bin, []),
                                  Logitem
                          end, Pos_list),
            lists:zip(Pos_list, Record_list)
    end.

add_step(Pos, Offset, S) ->
    %% error_logger:info_msg("[~p:~p] add_step, Pos:~p, Offset:~p~n",[?MODULE, ?LINE, Pos, Offset]),
    Max_cell_counts = S#state.max_cell_counts,
    Pos_start = S#state.pos_start,
    Pos_end = S#state.pos_end,
    Pos2 = (Pos+Offset+Max_cell_counts) rem Max_cell_counts,
    case is_in_circle(Pos2, Pos_start, Pos_end) of
        true -> Pos2;
        _ ->
            error_logger:warning_msg("[~p:~p] add_step return -1, Pos:~p, Offset:~p~n",[?MODULE, ?LINE, Pos, Offset]),
            throw(add_step_fail),
            -1
    end.

circle_file_read(S, Pos, 0) ->
    {ok, 0, <<>>};
circle_file_read(S, Pos, Read_count) ->
    %% error_logger:info_msg("[~p:~p] circle_file_read, Pos:~p, Read_count:~p~n",[?MODULE, ?LINE, Pos, Read_count]),
    Fd = S#state.fd,
    case S#state.pos_end > Pos of
        true ->
            {ok, Bin} = file:pread(Fd, Pos*?RECORD_FIXED_SIZE, Read_count*?RECORD_FIXED_SIZE),
            {ok, Read_count, Bin};
        false ->
            {ok, Bin1} = file:pread(Fd, Pos*?RECORD_FIXED_SIZE,
                                    (S#state.max_cell_counts - Pos + 1)*?RECORD_FIXED_SIZE),
            {ok, Bin2} = file:pread(Fd, 0,
                                    (Read_count - S#state.max_cell_counts + Pos - 1)*?RECORD_FIXED_SIZE),
            {ok, Read_count, <<Bin1/binary, Bin2/binary>>}
    end.

%%% ###############################################
circle_record_count(Pos, Pos_start, Pos_end, Max_cell_counts) ->
    case is_in_circle(Pos, Pos_start, Pos_end, Max_cell_counts) of
        false -> 0;
        true ->
            (Pos - Pos_start - 1 + Max_cell_counts) rem Max_cell_counts
    end.

get_max_cell_counts() ->
    case ets:lookup(circle_storage, max_cell_counts) of
        [{max_cell_counts, Max_cell_counts}] -> Max_cell_counts;
        _ -> error(fail_to_get_max_cell_counts)
    end.

%% decode binary to logitem, notes data is retrieved from leveldb
binary_to_logitems(<<>>, L) ->
    lists:reverse(L);
binary_to_logitems(Bin, L) ->
    <<Record_data:?RECORD_FIXED_SIZE/binary, Res/binary>> = Bin,
    Record_bin = decode_record(Record_data),
    case Record_bin of
        <<>> ->
            error_logger:error_msg("[~p:~p] data is empty~n",[?MODULE, ?LINE]),
            binary_to_logitems(Res, L);
        _ ->
            Record = binary_to_term(Record_bin),
            Log_item = record_to_logitem(Record),
            binary_to_logitems(Res, [Log_item | L])
    end.

match_filter(F, {_, Term}=V) when is_function(F)->
    {F(Term), V};
match_filter([], V)-> {true, V};
match_filter([F|T], {_, Term}=V) when is_function(F)->
    case F(Term) of
        true ->
            match_filter(T, V);
        _ -> {false, none}
    end;

match_filter(_,_)-> {false, none}.

get_value_from_dets(Key, Dets) ->
    case dets:lookup(Dets, Key) of
        [{Key, Value} | _ ] -> {ok, Value};
        Any -> {notfound, Any}
    end.

record_to_logitem(Record) when is_record(Record, record) ->
    #log_item{time = Record#record.time,
              error_group = list_to_binary(Record#record.error_group),
              error_type = list_to_binary(Record#record.error_type),
              server = list_to_binary(Record#record.server),
              client = list_to_binary(Record#record.client),
              node = list_to_binary(Record#record.node),
              url = list_to_binary(Record#record.url)}.

logitem_to_record(Log_item) when is_record(Log_item, log_item) ->
    #record{time = Log_item#log_item.time,
            error_group = binary_to_list(Log_item#log_item.error_group),
            error_type = binary_to_list(Log_item#log_item.error_type),
            server = binary_to_list(Log_item#log_item.server),
            client = binary_to_list(Log_item#log_item.client),
            node = binary_to_list(Log_item#log_item.node),
            url = binary_to_list(Log_item#log_item.url)}.

decode_record(<<Len:16/integer, Data:Len/binary, _/binary>>) ->
    Data.

get_link_items(Link_start, Dets, Index, 0, L) ->
    L;
get_link_items(Link_start, Dets, Index, Count, L) when Count>0 ->
    case get_value_from_dets({link_next, Index, Link_start}, Dets) of
        {notfound, _} -> get_link_items(Link_start, Dets, Index, 0, L);
        {ok, ?POS_NOT_FOUND} -> get_link_items(Link_start, Dets, Index, 0, L);
        {ok, Value} ->get_link_items(Value, Dets, Index, Count-1, [Value| L])
    end.

is_in_circle(Pos, Pos_start, Pos_end) ->
    Max_cell_counts = get_max_cell_counts(),
    is_in_circle(Pos, Pos_start, Pos_end, Max_cell_counts).

is_in_circle(Pos, _Pos_start, _Pos_end, Max_cell_counts)
  when ((Pos>=Max_cell_counts) or (Pos=<0)) ->
    false;
is_in_circle(Pos, Pos_start, Pos_end, _Max_cell_counts)
  when Pos_end > Pos_start ->
    ((Pos > Pos_start) and (Pos < Pos_end));
is_in_circle(_Pos, Pos_start, Pos_end, _Max_cell_counts)
  when Pos_start =:= Pos_end ->
    false;
is_in_circle(Pos, Pos_start, Pos_end, _Max_cell_counts) ->
    ((Pos > Pos_start) or (Pos < Pos_end)).

%%% File : circle_storage.erl ends
