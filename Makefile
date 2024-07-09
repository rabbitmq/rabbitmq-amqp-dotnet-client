all: format test

format:
	dotnet format $(CURDIR)/rabbitmq-amqp-dotnet-client.sln

build:
	dotnet build $(CURDIR)/Build.csproj

test: build
	dotnet test -c Debug $(CURDIR)/Tests/Tests.csproj --no-build --logger:"console;verbosity=detailed" /p:AltCover=true

rabbitmq-server-start:
	 ./.ci/ubuntu/gha-setup.sh start

rabbitmq-server-stop:
	 ./.ci/ubuntu/gha-setup.sh stop


# TODO:
## publish the documentation on github pages
## you should execute this command only on the `main` branch
# publish-github-pages:
# 	## Create the PDF
# 	docker run  -it -v $(shell pwd)/docs/:/client_doc/  asciidoctor/docker-asciidoctor /bin/bash -c "cd /client_doc/asciidoc &&  asciidoctor-pdf index.adoc"
# 	## Create the HTML
# 	docker run  -it -v $(shell pwd)/docs/:/client_doc/  asciidoctor/docker-asciidoctor /bin/bash -c "cd /client_doc/asciidoc &&  asciidoctor index.adoc"
# 	## copy the PDF and HTML to temp folder
# 	rm -rf docs/temp
# 	mkdir -p docs/temp
# 	cp docs/asciidoc/index.pdf docs/temp/dotnet-stream-client.pdf
# 	cp docs/asciidoc/index.html docs/temp/index.html
# 	## check out the gh-pages branch
# 	git checkout gh-pages
# 	## copy the PDF and HTML to the root folder
# 	mv docs/temp/dotnet-stream-client.pdf stable/dotnet-stream-client.pdf
# 	mv docs/temp/index.html stable/htmlsingle/index.html
# 	## commit and push
# 	git add stable/dotnet-stream-client.pdf
# 	git add stable/htmlsingle/index.html
# 	git commit -m "Update the documentation"
# 	git push origin gh-pages
# 	## go back to the main branch
# 	git checkout main
