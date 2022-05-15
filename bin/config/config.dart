import 'dart:io';

import 'package:settings_yaml/settings_yaml.dart';

class Config {
  static int _serverPort = 8080;

  static String _databaseAddress = '127.0.0.1';
  static int _databasePort = 5432;
  static String _databaseName = '';
  static String _databaseUsername = '';
  static String _databasePassword = '';

  static String _refinerDataStoreDirectory = '';
  static String _pillarsOffChainInfoDirectory = '';

  static int get serverPort {
    return _serverPort;
  }

  static String get databaseAddress {
    return _databaseAddress;
  }

  static int get databasePort {
    return _databasePort;
  }

  static String get databaseName {
    return _databaseName;
  }

  static String get databaseUsername {
    return _databaseUsername;
  }

  static String get databasePassword {
    return _databasePassword;
  }

  static String get refinerDataStoreDirectory {
    return _refinerDataStoreDirectory;
  }

  static String get pillarsOffChainInfoDirectory {
    return _pillarsOffChainInfoDirectory;
  }

  static void load() {
    final settings = SettingsYaml.load(
        pathToSettings: '${Directory.current.path}/config.yaml');

    _serverPort = settings['server_port'] as int;

    _databaseAddress = settings['database_address'] as String;
    _databasePort = settings['database_port'] as int;
    _databaseName = settings['database_name'] as String;
    _databaseUsername = settings['database_username'] as String;
    _databasePassword = settings['database_password'] as String;

    _refinerDataStoreDirectory =
        settings['refiner_data_store_directory'] as String;
    _pillarsOffChainInfoDirectory =
        settings['pillars_off_chain_info_directory'] as String;
  }
}
