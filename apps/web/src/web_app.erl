-module(web_app).
-author('Oleg Zinchenko').
-behaviour(application).
-export([start/2, stop/1]).

start(_StartType, _StartArgs) -> web_sup:start_link().
stop(_State) -> ok.
