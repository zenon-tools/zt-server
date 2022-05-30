import 'dart:io';

import 'package:shelf/shelf.dart';
import 'package:shelf_router/shelf_router.dart';

import '../config/config.dart';
import '../services/database_service.dart';
import '../utils/utils.dart';

class Api {
  final headers = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, POST',
    'Access-Control-Allow-Headers': 'Origin, Content-Type',
    'Content-Type': 'application/json'
  };

  Router get router {
    final router = Router()
      ..get('/momentum-height', _momentumHeightHandler)
      ..get('/nom-data', _nomDataHandler)
      ..get('/pcs-pool', _pcsPoolHandler)
      ..get('/pillars', _pillarsHandler)
      ..get('/pillars-off-chain', _pillarsOffChainHandler)
      ..get('/portfolio', _portfolioHandler)
      ..get('/votes', _votesHandler);

    router.all('/<ignored|.*>', (Request request) => Response.notFound('null'));

    return router;
  }

  Future<Response> _momentumHeightHandler(Request request) async {
    final data = await DatabaseService().getLatestHeight();
    return Response.ok(
      Utils.toJson({'momentum_height': data}),
      headers: headers,
    );
  }

  Future<Response> _nomDataHandler(Request request) async {
    final data = File('${Config.refinerDataStoreDirectory}/nom_data.json')
        .readAsStringSync();
    return Response.ok(
      data,
      headers: headers,
    );
  }

  Future<Response> _pcsPoolHandler(Request request) async {
    final data = File('${Config.refinerDataStoreDirectory}/pcs_pool_data.json')
        .readAsStringSync();
    return Response.ok(
      data,
      headers: headers,
    );
  }

  Future<Response> _pillarsHandler(Request request) async {
    final data = File('${Config.refinerDataStoreDirectory}/pillar_data.json')
        .readAsStringSync();
    return Response.ok(
      data,
      headers: headers,
    );
  }

  Future<Response> _pillarsOffChainHandler(Request request) async {
    final data = File(
            '${Config.pillarsOffChainInfoDirectory}/pillars_off_chain_info.json')
        .readAsStringSync();
    return Response.ok(
      data,
      headers: headers,
    );
  }

  Future<Response> _portfolioHandler(Request request) async {
    final address = request.url.queryParameters['address'] ?? '';

    if (address.length != 40) {
      return Response.internalServerError();
    }

    final futures = await Future.wait([
      DatabaseService().getStakes(address),
      DatabaseService().getDelegation(address),
      DatabaseService().getSentinel(address),
      DatabaseService().getPillar(address),
      DatabaseService().getBalances(address),
    ]);

    return Response.ok(
      Utils.toJson({
        'address': address,
        'stakes': futures[0],
        'delegation': futures[1],
        'sentinel': futures[2],
        'pillar': futures[3],
        'balances': futures[4]
      }),
      headers: headers,
    );
  }

  Future<Response> _votesHandler(Request request) async {
    final pillar = request.url.queryParameters['pillar'] ?? '';

    if (pillar.length == 0 || pillar.length > 30) {
      return Response.internalServerError();
    }

    final votes = await DatabaseService().getVotesByPillar(pillar);

    return Response.ok(
      Utils.toJson(votes),
      headers: headers,
    );
  }
}
