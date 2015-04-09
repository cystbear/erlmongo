-module(config).
-compile(export_all).

log_level() -> info.
log_modules() -> any.
%%   [
%%     index
%%   ].

info() ->  spawn(fun()-> wf:info(index,"~p",[mnesia:info()]) end).
