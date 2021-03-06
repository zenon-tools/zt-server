import 'dart:convert';
import 'dart:io';

import 'package:hex/hex.dart';
import 'package:shelf/shelf.dart';
import 'package:shelf_router/shelf_router.dart';
import 'package:znn_sdk_dart/znn_sdk_dart.dart';

import '../config/config.dart';
import '../services/database_service.dart';
import '../utils/utils.dart';

extension ContainsKeys on Map {
  bool containsKeys(List<String> keys) {
    for (final key in keys) {
      if (!this.containsKey(key)) {
        return false;
      }
    }
    return true;
  }
}

class Api {
  final headers = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, PUT, OPTIONS',
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
      ..get('/votes', _votesHandler)
      ..get('/projects', _projectsHandler)
      ..get('/project', _projectHandler)
      ..get('/project-votes', _projectVotesHandler)
      ..get('/phase-votes', _phaseVotesHandler)
      ..get('/reward-share-history', _rewardShareHistoryHandler)
      ..get('/pillar-delegators', _pillarDelegatorsHandler)
      ..get('/pillar-profile', _pillarProfileHandler)
      ..get('/donations', _donationsHandler)
      ..put('/pillar-off-chain', _pillarOffChainHandler);

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
    final page = int.parse(request.url.queryParameters['page'] ?? '1');
    final searchText = request.url.queryParameters['search'] ?? '';

    if (pillar.length == 0 ||
        pillar.length > 30 ||
        page <= 0 ||
        page > 100 ||
        searchText.length > 50) {
      return Response.internalServerError();
    }

    final votes =
        await DatabaseService().getVotesByPillar(pillar, page, searchText);

    return Response.ok(
      Utils.toJson(votes),
      headers: headers,
    );
  }

  Future<Response> _projectsHandler(Request request) async {
    final page = int.parse(request.url.queryParameters['page'] ?? '1');
    final searchText = request.url.queryParameters['search'] ?? '';

    if (page <= 0 || page > 100 || searchText.length > 50) {
      return Response.internalServerError();
    }

    final proposals = await DatabaseService().getAzProjects(page, searchText);

    return Response.ok(
      Utils.toJson(proposals),
      headers: headers,
    );
  }

  Future<Response> _projectHandler(Request request) async {
    final id = request.url.queryParameters['projectId'] ?? '';

    if (id.isEmpty || id.length > 100) {
      return Response.internalServerError();
    }

    final project = await DatabaseService().getAzProjectById(id);
    final phases = await DatabaseService().getAzPhasesByProjectId(id);
    project['phases'] = phases;

    return Response.ok(
      Utils.toJson(project),
      headers: headers,
    );
  }

  Future<Response> _projectVotesHandler(Request request) async {
    final id = request.url.queryParameters['projectId'] ?? '';

    if (id.isEmpty || id.length > 100) {
      return Response.internalServerError();
    }

    final votes = await DatabaseService().getAzVotesForProjectById(id);
    return Response.ok(
      Utils.toJson(votes),
      headers: headers,
    );
  }

  Future<Response> _phaseVotesHandler(Request request) async {
    final id = request.url.queryParameters['phaseId'] ?? '';

    if (id.isEmpty || id.length > 100) {
      return Response.internalServerError();
    }

    final votes = await DatabaseService().getAzVotesForPhaseById(id);
    return Response.ok(
      Utils.toJson(votes),
      headers: headers,
    );
  }

  Future<Response> _rewardShareHistoryHandler(Request request) async {
    final pillar = request.url.queryParameters['pillar'] ?? '';

    if (pillar.isEmpty || pillar.length > 100) {
      return Response.internalServerError();
    }

    final List events = await DatabaseService().getPillarUpdateEvents(pillar);

    final rewardShareEvents = [];
    var previousEvent;
    for (final event in events) {
      if (previousEvent == null ||
          previousEvent['giveBlockRewardPercentage'] !=
              event['giveBlockRewardPercentage'] ||
          previousEvent['giveDelegateRewardPercentage'] !=
              event['giveDelegateRewardPercentage']) {
        rewardShareEvents.add(event);
      }
      previousEvent = event;
    }
    return Response.ok(
      Utils.toJson(rewardShareEvents.reversed.toList()),
      headers: headers,
    );
  }

  Future<Response> _pillarDelegatorsHandler(Request request) async {
    final pillar = request.url.queryParameters['pillar'] ?? '';

    if (pillar.length == 0 || pillar.length > 50) {
      return Response.internalServerError();
    }

    final delegators = await DatabaseService().getPillarDelegators(pillar);
    return Response.ok(
      Utils.toJson(delegators),
      headers: headers,
    );
  }

  Future<Response> _pillarProfileHandler(Request request) async {
    final pillar = request.url.queryParameters['pillar'] ?? '';

    if (pillar.length == 0 || pillar.length > 50) {
      return Response.internalServerError();
    }

    final profile = await DatabaseService().getPillarProfile(pillar);
    return Response.ok(
      Utils.toJson(profile),
      headers: headers,
    );
  }

  Future<Response> _donationsHandler(Request request) async {
    final List donations = await DatabaseService().getDonations();

    return Response.ok(
      Utils.toJson(donations),
      headers: headers,
    );
  }

  Future<Response> _pillarOffChainHandler(Request request) async {
    print('Start update pillar off-chain information.');

    final reqData = jsonDecode(await request.readAsString());
    print(reqData);

    if (!_verifyPillarOffChainInfo(reqData)) {
      return Response.internalServerError();
    }

    final pillarAddress = reqData['pillarAddress'];
    final pubKey = await DatabaseService().getPublicKeyByAddress(pillarAddress);
    final pillar = await DatabaseService().getPillar(pillarAddress);

    if (pillar.length == 0 ||
        pillar['name'].length == 0 ||
        pillar['name'] != reqData['info']['name']) {
      print('Failed: Pillar not found.');
      return Response.internalServerError();
    }

    print('PubKey: ' + pubKey);
    print('Pillar: ' + pillar['name']);

    const challenge = 'kF5Ja7nPZ4';
    if (!(await _verifySignature(
        challenge, pillarAddress, pubKey, reqData['signature'].trim()))) {
      print('Failed: Unable to verify signature.');
      return Response.internalServerError();
    }

    final dbFile = File(
        '${Config.pillarsOffChainInfoDirectory}/pillars_off_chain_info.json');
    final db = jsonDecode(dbFile.readAsStringSync());

    for (final key in reqData['info'].keys) {
      if (key == 'links') {
        for (final linkKey in reqData['info'][key].keys) {
          reqData['info'][key][linkKey] =
              (reqData['info'][key][linkKey] as String).trim();
        }
      } else {
        reqData['info'][key] = (reqData['info'][key] as String).trim();
      }
    }

    db[pillarAddress] = reqData['info'];

    dbFile.writeAsStringSync(Utils.toJson(db));

    print('Updated successfully.');
    return Response.ok(
      '',
      headers: headers,
    );
  }
}

Future<bool> _verifySignature(String message, String address,
    String base64PubKey, String signature) async {
  final decodedMsg = HEX.decode(HEX.encode(Utf8Encoder().convert(message)));
  final decodedPubKey = base64Decode(base64PubKey);
  final decodedSignature = HEX.decode(signature);

  return Address.fromPublicKey(decodedPubKey).toString() == address &&
      (await Crypto.verify(decodedSignature, decodedMsg, decodedPubKey));
}

bool _verifyPillarOffChainInfo(Map<String, dynamic> offChainInfo) {
  const rootKeys = ['pillarAddress', 'signature', 'info'];
  const infoKeys = ['name', 'links', 'avatar', 'description'];
  const linkKeys = [
    'telegram',
    'twitter',
    'website',
    'github',
    'medium',
    'email'
  ];

  if (offChainInfo.length == rootKeys.length &&
      offChainInfo.containsKeys(rootKeys)) {
    for (final key in rootKeys) {
      if (!(offChainInfo[key].length <= 250)) {
        print('Failed: Root value too long.');
        return false;
      }
    }

    if (offChainInfo['info'].length == infoKeys.length &&
        (offChainInfo['info'] as Map<String, dynamic>).containsKeys(infoKeys)) {
      if (!(offChainInfo['info']['name'].length <= 100) ||
          !(offChainInfo['info']['avatar'].length <= 250) ||
          !(offChainInfo['info']['description'].length <= 500)) {
        print('Failed: Info value too long.');
        return false;
      }

      if (offChainInfo['info']['links'].length == linkKeys.length &&
          (offChainInfo['info']['links'] as Map<String, dynamic>)
              .containsKeys(linkKeys)) {
        for (final key in linkKeys) {
          if (!(offChainInfo['info']['links'][key].length <= 250)) {
            print('Failed: Link value too long.');
            return false;
          }
        }
        return true;
      }
    }
  }

  print('Failed: Bad request data.');
  return false;
}
