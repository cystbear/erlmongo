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
get(Tab, Key) ->
  Connection = get_connection(),
  Collection = get_collection_name(Tab),
  Id = get_id(Key),
  Doc = mongo:find_one(Connection, Collection, Id),
  case Doc of
    {} -> {error, not_found};
    _ -> {ok, Doc}
  end.

%%   {ok,
%%     {
%%       {
%%         '_id', {<<85,37,71,234,203,12,25,240,82,161,182,78>>},
%%         name, <<"Alex">>
%%       }
%%     }
%%   },
%%   {ok, Doc},
  %%   Data = mongo:find_one(Connection, <<"users">>, {name, <<"Alex">>}),
  %%   {'_id',{<<85,37,71,234,203,12,25,240,82,161,182,78>>}


get(_Tab, _Key, _Value) -> {ok, document}.
index(_Tab, _Key, _Value) -> [document, document].

%% INTERNAL
connect() ->
%%   {DbName} = kvs:config(kvs, mongodb_settings),
%%     ConnRes = mongo:connect(DbName),
%%     case ConnRes of
%%       {ok, Connection} -> save_connection(Connection), ok;
%%       {error, Reason}  -> {error, Reason}
%%     end.
  %% {Db, User, Pass, Wmode, Rmode, Options} = {<<"test">>, <<>>, <<>>, slave_ok, unsafe, []},
  {DbName} = kvs:config(kvs, mongodb_settings),
  {ok, Connection} = mongo:connect(DbName),
  save_connection(Connection),
  Connection.

save_connection(Connection) ->
  ets:insert(mongo_id_server, {conn_pid, Connection}).

get_connection() ->
  [{conn_pid, Connection}|_] = ets:lookup(mongo_id_server, conn_pid),
  Connection.

get_collection_name(RawCollName) when is_atom(RawCollName)   -> atom_to_binary(RawCollName, utf8);
get_collection_name(RawCollName) when is_binary(RawCollName) -> RawCollName;
get_collection_name(RawCollName) when is_list(RawCollName)   -> unicode:characters_to_binary(RawCollName);
get_collection_name(RawCollName)                             -> RawCollName.

get_id(RawId) when is_binary(RawId) -> {'_id', {RawId}};
get_id({'_id', {_}} = RawId)        -> RawId;
get_id(RawId)                       -> RawId.
