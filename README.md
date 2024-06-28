# RabbitMQ AMQP 1.0 DotNet Client


This library is in early stages of development.
It is meant to be used with RabbitMQ 4.0.

## How to Run
- start the broker with `ci/start-broker.sh`
- run the tests with ` dotnet test ./Build.csproj  --logger "console;verbosity=detailed" /p:AltCover=true`

## Getting Started
You can find an example in: `docs/Examples/GettingStarted`


## TODO

- [x] Declare queues
- [ ] Declare exchanges
- [ ] Declare bindings
- [x] Simple Publish messages
- [x] Implement backpressure ( atm it is implemented with MaxInflightMessages `MaxInFlight(2000).`)
- [ ] Simple Consume messages
- [ ] Implement metrics ( See `System.Diagnostics.DiagnosticSource` [Link](https://learn.microsoft.com/en-us/dotnet/core/diagnostics/metrics-instrumentation) )
- [x] Recovery connection on connection lost
- [x] Recovery management on connection lost
- [x] Recovery queues on connection lost
- [ ] Recovery publisher on connection lost
- [ ] Recovery consumer on connection lost
- [ ] Docker image to test in LRE 
- [ ] Check the TODO in the code

