-module(web_sup).
-author('Oleg Zinchenko').
-behaviour(supervisor).
-export([start_link/0, init/1]).

start_link() -> supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
  {ok, _} = cowboy:start_http(
    http,
    100,
    [{port, wf:config(n2o, port, 5555)}],
    [{env, [{dispatch, dispatch_rules()}]}]
  ),

%%   kvs:join(),
%%
  {ok, {{one_for_one, 5, 10}, []}}.

mime() -> [{mimetypes, cow_mimetypes, all}].

dispatch_rules() ->
  cowboy_router:compile(
    [{'_', [
      {"/", cowboy_static, {priv_file, web, "static/index.html", mime()}},
      {"/favicon.ico", cowboy_static, {priv_file, web, "static/favicon.ico"}},
      {"/ws/[...]", bullet_handler, [{handler, n2o_bullet}]},
      {"/n2o/[...]", cowboy_static, {priv_dir, n2o, "", mime()}},
      {"/static/[...]", cowboy_static, {priv_dir, web, "static", mime()}},
      {'_', n2o_cowboy, []}
  ]}]).
