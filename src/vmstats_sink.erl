-module(vmstats_sink).

-type collect_type() :: counter | gauge | timing.

-callback collect(Measurement :: collect_type(), Fields :: list({string(), term()}), Tags :: list({string(), string()})) -> ok.
