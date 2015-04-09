-module(index).
-compile(export_all).
-include_lib("n2o/include/wf.hrl").

main() -> ok.

event({client, Message}) ->
%%   mongo:connect()
  wf:send(erlmongo, {server, {self(), Message}});

event({server, {Pid, Message}}) ->
  case self() of
    Pid -> nop;
    _   -> self() ! {bin, Message}
  end;

event({bin, Message}) -> Message;
event(init) -> wf:reg(erlmongo);

event(Event) -> wf:info(?MODULE, "EMPTY Event: ~p", [Event]).
