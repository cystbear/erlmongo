-module(store_mongodb).
-copyright('Synrc Research Center s.r.o.').
-include_lib("kvs/include/config.hrl").
-include_lib("kvs/include/metainfo.hrl").
-compile(export_all).

%% API

% service
start() ->
  application:start(bson),
  application:start(mongodb),
  ok. % {error, Reason}

stop() -> stopped.

% schema change
destroy() -> ok.
join() -> connect().
%% ok.
join(_Node) -> ok.
init(_Backend, _Module) -> ok.

% meta info
modules() -> ok.
containers() -> ok.
tables() -> [{table, T} || T <- kvs:modules()]. % [#table{}].
table(_Tab) -> #table{}.
version() -> {version, "KVS MONGODB"}.

% read ops
get(Tab, {Key}) -> get(Tab, Key);
get(Tab, Key) when is_binary(Key) -> mongo_get(Tab, Key).

get(Tab, Key, Value) ->
  Document = get(Tab, Key),
  case Document of
    {error, not_found} -> Value;
    _ -> Document
  end.

index(Tab, Key, Value) ->
  Connection = get_connection(),
  Collection = atom_to_binary(Tab, utf8),
  Cursor = mongo:find(Connection, Collection, {Key, to_bin(Value)}),
  Result = mc_cursor:rest(Cursor),
  mc_cursor:close(Cursor),
  Result.

%% INTERNAL
mongo_get(Tab, Key) ->
  Connection = get_connection(),
  Collection = atom_to_binary(Tab, utf8),
  Document = mongo:find_one(Connection, Collection, {'_id', {Key}}),
  case Document of
    {} -> {error, not_found};
    _ -> {ok, Document}
  end.

connect() ->
  {DbName} = kvs:config(kvs, mongodb_settings),
  {ok, Connection} = mongo:connect(DbName),
  save_connection(Connection),
  Connection.

save_connection(Connection) ->
  ets:insert(mongo_id_server, {conn_pid, Connection}).

get_connection() ->
  [{conn_pid, Connection}|_] = ets:lookup(mongo_id_server, conn_pid),
  Connection.

to_bin(Value) when is_list(Value) -> unicode:characters_to_binary(Value);
to_bin(Value) -> Value.
