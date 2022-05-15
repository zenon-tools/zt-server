import 'dart:io';

import 'package:shelf/shelf.dart';
import 'package:shelf/shelf_io.dart' as shelf_io;

import 'api/api.dart';
import 'config/config.dart';
import 'services/database_service.dart';

Future main() async {
  Config.load();

  await DatabaseService().init();

  final server = await shelf_io.serve(
    logRequests().addHandler(Api().router),
    InternetAddress.anyIPv4,
    Config.serverPort,
  );

  print('Serving at http://${server.address.host}:${server.port}');
}
