all: format test

format:
	dotnet format $(CURDIR)/rabbitmq-amqp-dotnet-client.sln

build:
	dotnet build $(CURDIR)/Build.csproj

test: build
	dotnet test -c Debug $(CURDIR)/Tests/Tests.csproj --no-build --logger:"console;verbosity=detailed"

rabbitmq-server-start:
	 ./.ci/ubuntu/one-node/gha-setup.sh start pull

rabbitmq-server-stop:
	 ./.ci/ubuntu/one-node/gha-setup.sh stop
