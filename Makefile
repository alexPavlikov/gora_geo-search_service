.PHONY: run-app
run-app:
	go run ./cmd/api/main.go --config_path=./config/ --config_file=config

.PHONY: run-zookeeper
run-zookeeper:
	cd ..\..\kafka_2.13-3.7.0
	.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties

.PHONY: run-kafka
run-kafka:
	cd ..\..\kafka_2.13-3.7.0
	.\bin\windows\kafka-server-start.bat .\config\server.properties
