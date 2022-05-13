# Zenon Tools Server

The API server for https://zenon.tools

## Building from source
The Dart SDK is required to build the server from source (https://dart.dev/get-dart).
Use the Dart SDK to install the dependencies and compile the program by running the following commands:
```
dart pub get
dart compile exe bin/server.dart
```

## Configuration
Make a copy of the example.config.yaml file and name the copy "config.yaml". Set your desired configuration in the file.
* The database properties are referring to the database generated by the NoM Indexer.
* The "refiner_data_store_directory" property is referring to the output folder of the NoM Data Refiner.
* The "pillars_off_chain_data_directory" property is referring to the JSON file containing the information for pillar avatar URLs and social links.

## Running the server
```
cd bin
dart run server
```
