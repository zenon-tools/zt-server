import 'dart:convert';
import 'dart:io';

import 'package:hex/hex.dart';
import 'package:shelf/shelf.dart';
import 'package:shelf_router/shelf_router.dart';
import 'package:znn_sdk_dart/znn_sdk_dart.dart';
import 'package:timezone/data/latest.dart' as tz;
import 'package:timezone/timezone.dart' as tz;

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

  final int _genesisTimestamp = 1637755200;

  Router get router {
    final router = Router()
      ..get('/momentum-height', _momentumHeightHandler)
      ..get('/nom-data', _nomDataHandler)
      ..get('/znn-eth-pool', _znnEthPoolHandler)
      ..get('/pillars', _pillarsHandler)
      ..get('/pillars/<pillar>/votes', _votesHandler)
      ..get('/pillars-off-chain', _pillarsOffChainHandler)
      ..get('/portfolio', _portfolioHandler)
      ..get('/votes', _votesHandlerDeprecated)
      ..get('/projects', _projectsHandler)
      ..get('/project', _projectHandler)
      ..get('/project-votes', _projectVotesHandler)
      ..get('/phase-votes', _phaseVotesHandler)
      ..get('/reward-share-history', _rewardShareHistoryHandler)
      ..get('/pillar-delegators', _pillarDelegatorsHandler)
      ..get('/pillar-profile', _pillarProfileHandler)
      ..get('/donations', _donationsHandler)
      ..get('/accounts', _accountsHandler)
      ..get('/accounts/<address>', _accountDetailsHandler)
      ..get('/accounts/<address>/transactions/received',
          _accountTransactionsHandler)
      ..get('/accounts/<address>/transactions/unreceived',
          _accountUnreceivedTransactionsHandler)
      ..get('/accounts/<address>/tokens', _accountTokensHandler)
      ..get('/accounts/<address>/proposals', _accountAzProposalsHandler)
      ..get('/accounts/<address>/fusions', _accountPlasmaFusionsHandler)
      ..get('/accounts/<address>/participation', _accountParticipationHandler)
      ..get('/accounts/<address>/rewards/csv', _accountRewardsHandlerCsv)
      ..get('/accounts/<address>/rewards/count', _accountRewardsCountHandler)
      ..get('/tokens', _tokensHandler)
      ..get('/tokens/<tokenId>', _tokenHandler)
      ..get('/tokens/<tokenId>/holders', _tokenHoldersHandler)
      ..get('/tokens/<tokenId>/transactions', _tokenTransactionsHandler)
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

  Future<Response> _znnEthPoolHandler(Request request) async {
    final data =
        File('${Config.refinerDataStoreDirectory}/znn_eth_pool_data.json')
            .readAsStringSync();
    return Response.ok(
      data,
      headers: headers,
    );
  }

  Future<Response> _pillarsHandler(Request request) async {
    final data = jsonDecode(
        File('${Config.refinerDataStoreDirectory}/pillar_data.json')
            .readAsStringSync()) as Map<String, dynamic>;
    final statistics30d = jsonDecode(
        File('${Config.statisticsDirectory}/pillars/statistics/30d.json')
            .readAsStringSync()) as Map<String, dynamic>;

    final additionalData = await DatabaseService().getPillarInfo();
    final now = (DateTime.now().millisecondsSinceEpoch / 1000).round();

    data.forEach((k, v) {
      if (statistics30d.containsKey(k)) {
        data[k]['uptime30d'] = statistics30d[k]['uptime'] * 100;
        data[k]['delegateApr30d'] = statistics30d[k]['avgDelegateApr'];
      } else {
        data[k]['uptime30d'] = 0;
        data[k]['delegateApr30d'] = 0;
      }

      if (additionalData.containsKey(k)) {
        final daysInBetween = _daysInBetween(
            additionalData[k]['spawnTimestamp'] == 0
                ? _genesisTimestamp
                : additionalData[k]['spawnTimestamp'],
            now);
        data[k]['votingActivity'] = additionalData[k]['votingActivity'] * 100;
        data[k]['producedMomentumCount'] =
            additionalData[k]['producedMomentumCount'];
        data[k]['dailyMomentumAvg'] = daysInBetween == 0
            ? 0
            : (data[k]['producedMomentumCount'] / daysInBetween).round();
      } else {
        data[k]['votingActivity'] = 0;
        data[k]['producedMomentumCount'] = 0;
        data[k]['dailyMomentumAvg'] = 0;
      }
    });

    return Response.ok(
      new JsonEncoder.withIndent('  ').convert(data),
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
      DatabaseService().getAccountTokens(address),
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

  Future<Response> _votesHandlerDeprecated(Request request) async {
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

  Future<Response> _votesHandler(Request request) async {
    final pillar = request.params['pillar'] ?? '';
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
    final totalCount =
        await DatabaseService().getPillarVoteCount(pillar, searchText);
    return Response.ok(
      Utils.toJson({'count': totalCount, 'page': page, 'list': votes}),
      headers: headers,
    );
  }

  Future<Response> _projectsHandler(Request request) async {
    final page = int.parse(request.url.queryParameters['page'] ?? '1');
    final searchText = request.url.queryParameters['search'] ?? '';

    if (page <= 0 || page > 100 || searchText.length > 100) {
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
    profile['delegatorCount'] =
        await DatabaseService().getPillarDelegatorCount(pillar);

    final now = (DateTime.now().millisecondsSinceEpoch / 1000).round();
    final daysInBetween = _daysInBetween(
        profile['spawnTimestamp'] == 0
            ? _genesisTimestamp
            : profile['spawnTimestamp'],
        now);
    profile['dailyMomentumAvg'] = daysInBetween == 0
        ? 0
        : (profile['producedMomentumCount'] / daysInBetween).round();

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

  Future<Response> _accountsHandler(Request request) async {
    final page = int.parse(request.url.queryParameters['page'] ?? '1');
    final searchText = request.url.queryParameters['search'] ?? '';

    if (page <= 0 || page > 100 || searchText.length > 50) {
      return Response.internalServerError();
    }

    final List accounts = await DatabaseService().getAccounts(page, searchText);

    return Response.ok(
      Utils.toJson(accounts),
      headers: headers,
    );
  }

  Future<Response> _accountDetailsHandler(Request request) async {
    final address = request.params['address'] ?? '';

    final accountDetails = await DatabaseService().getAccountDetails(address);
    final fusedQsr = await DatabaseService().getAccountFusedQsr(address);
    accountDetails['fusedQsr'] = fusedQsr;
    return Response.ok(
      Utils.toJson(accountDetails),
      headers: headers,
    );
  }

  Future<Response> _accountTransactionsHandler(Request request) async {
    final address = request.params['address'] ?? '';
    final page = int.parse(request.url.queryParameters['page'] ?? '1');

    if (page <= 0 || page > 10000 || address.length != 40) {
      return Response.internalServerError();
    }

    final txs = await _getTransactions(address, page: page);

    return Response.ok(
      Utils.toJson(txs),
      headers: headers,
    );
  }

  /* Future<Response> _accountTransactionsHandlerCsv(Request request) async {
    final address = request.params['address'] ?? '';

    if (address.length != 40) {
      return Response.internalServerError();
    }

    final List txs = await _getTransactions(address, limit: 10000);

    String csv =
        'Account Height, Momentum Timestamp, Amount, Symbol, Momentum Height, Sender, Receiver\n';
    txs.forEach((item) {});

    final timestamp = (DateTime.now().millisecondsSinceEpoch / 1000).round();
    return Response.ok(
      csv,
      headers: {
        ...headers,
        'Content-Type': 'text/csv',
        'Content-Disposition':
            'attachment; filename="txs_${address}_${timestamp}.csv"'
      },
    );
  } */

  Future<List> _getTransactions(String address,
      {int page = 1, int limit = 10}) async {
    final List txs = await DatabaseService()
        .getAddressTransactions(address, page, limit: limit);

    // TODO: This could probably be fixed with better indexing.
    for (int i = 0; i < txs.length; i++) {
      final hash = txs[i]['hash'];
      if (txs[i]['toAddress'] == 'z1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqsggv2f' &&
          hash.length > 0 &&
          txs[i]['pairedAccountBlock'] !=
              '0000000000000000000000000000000000000000000000000000000000000000' &&
          txs[i]['pairedAccountBlock'] != '') {
        final data =
            await DatabaseService().getAddressReceivedTransactionData(hash);
        txs[i]['amount'] = data['amount'];
        txs[i]['symbol'] = data['symbol'];
        txs[i]['decimals'] = data['decimals'];
        txs[i]['address'] = data['address'];
        txs[i]['toAddress'] = data['toAddress'];
        if (txs[i]['method'] == 'Unknown') {
          if (data['method'].length > 0) {
            txs[i]['method'] = data['method'];
          } else if (data['symbol'].length > 0) {
            txs[i]['method'] = 'Transfer';
          }
        }
      }
    }

    return txs;
  }

  Future<Response> _accountUnreceivedTransactionsHandler(
      Request request) async {
    final address = request.params['address'] ?? '';
    final page = int.parse(request.url.queryParameters['page'] ?? '1');

    if (page <= 0 || page > 100 || address.length != 40) {
      return Response.internalServerError();
    }

    final List txs =
        await DatabaseService().getAddressUnreceivedTransactions(address, page);

    return Response.ok(
      Utils.toJson(txs),
      headers: headers,
    );
  }

  Future<Response> _accountTokensHandler(Request request) async {
    final address = request.params['address'] ?? '';

    if (address.length != 40) {
      return Response.internalServerError();
    }

    final tokens = await DatabaseService().getAccountTokens(address);

    return Response.ok(
      Utils.toJson(tokens),
      headers: headers,
    );
  }

  Future<Response> _accountAzProposalsHandler(Request request) async {
    final address = request.params['address'] ?? '';
    final page = int.parse(request.url.queryParameters['page'] ?? '1');

    if (page <= 0 || page > 100 || address.length != 40) {
      return Response.internalServerError();
    }

    final proposals =
        await DatabaseService().getAddressAzProposals(address, page);

    return Response.ok(
      Utils.toJson(proposals),
      headers: headers,
    );
  }

  Future<Response> _accountPlasmaFusionsHandler(Request request) async {
    final address = request.params['address'] ?? '';
    final page = int.parse(request.url.queryParameters['page'] ?? '1');

    if (page <= 0 || page > 100 || address.length != 40) {
      return Response.internalServerError();
    }

    final fusions = await DatabaseService().getAddressFusions(address, page);

    return Response.ok(
      Utils.toJson(fusions),
      headers: headers,
    );
  }

  Future<Response> _accountParticipationHandler(Request request) async {
    final address = request.params['address'] ?? '';

    if (address.length != 40) {
      return Response.internalServerError();
    }

    final futures = await Future.wait([
      DatabaseService().getStakes(address),
      DatabaseService().getDelegation(address),
      DatabaseService().getSentinel(address),
      DatabaseService().getPillar(address)
    ]);

    return Response.ok(
      Utils.toJson({
        'address': address,
        'stakes': futures[0],
        'delegation': futures[1],
        'sentinel': futures[2],
        'pillar': futures[3]
      }),
      headers: headers,
    );
  }

  Future<Response> _accountRewardsHandlerCsv(Request request) async {
    final address = request.params['address'] ?? '';
    final includeQsr = (request.url.queryParameters['qsr'] ?? 'true') == 'true';
    final currency = (request.url.queryParameters['currency'] ?? 'usd');
    final year = request.url.queryParameters['year'] ?? '2021';
    final timezone = (request.url.queryParameters['timezone'] ?? '');

    if (address.length != 40 ||
        !['2021', '2022', 'all'].contains(year) ||
        !['usd', 'eur', 'gbp', 'cad', 'aud'].contains(currency) ||
        timezone.length == 0 ||
        timezone.length > 100) {
      return Response.internalServerError();
    }

    tz.initializeTimeZones();
    int startTimestamp = 0;
    int endTimestamp = 1893456000; // 2030

    if (year != 'all') {
      startTimestamp =
          (tz.TZDateTime(tz.getLocation(timezone), int.parse(year), 1, 1)
                      .millisecondsSinceEpoch /
                  1000)
              .round();
      endTimestamp =
          (tz.TZDateTime(tz.getLocation(timezone), int.parse(year) + 1, 1, 1)
                      .millisecondsSinceEpoch /
                  1000)
              .round();
    }

    final ignoredToken = includeQsr ? '' : 'zts1qsrxxxxxxxxxxxxxmrhjll';

    final List rewards = await DatabaseService().getAddressRewardTransactions(
        address, startTimestamp, endTimestamp, timezone,
        ignoredToken: ignoredToken);

    final marketHistory = jsonDecode(
        File('${Config.refinerDataStoreDirectory}/market_history_cache.json')
            .readAsStringSync());

    final timestamp = (DateTime.now().millisecondsSinceEpoch / 1000).round();
    return Response.ok(
      _createRewardTransactionsCsv(
          rewards, marketHistory, address, currency, timezone),
      headers: {
        ...headers,
        'Content-Type': 'text/csv',
        'Content-Disposition':
            'attachment; filename="rewards_${year}_${address}_${timestamp}.csv"'
      },
    );
  }

  Future<Response> _accountRewardsCountHandler(Request request) async {
    final address = request.params['address'] ?? '';

    if (address.length != 40) {
      return Response.internalServerError();
    }

    final count = await DatabaseService().getAddressRewardTransactionsCount(
      address,
    );

    return Response.ok(
      Utils.toJson(count),
      headers: headers,
    );
  }

  Future<Response> _tokensHandler(Request request) async {
    final page = int.parse(request.url.queryParameters['page'] ?? '1');
    final searchText = request.url.queryParameters['search'] ?? '';

    if (page <= 0 || page > 1000 || searchText.length > 50) {
      return Response.internalServerError();
    }

    final List tokens = await DatabaseService().getTokens(page, searchText);

    return Response.ok(
      Utils.toJson(tokens),
      headers: headers,
    );
  }

  Future<Response> _tokenHandler(Request request) async {
    final tokenId = request.params['tokenId'] ?? '';

    if (tokenId.length != 26) {
      return Response.internalServerError();
    }

    final token = await DatabaseService().getToken(tokenId);
    final creationTimestamp =
        await DatabaseService().getTokenCreationTimestamp(tokenId);
    token['creationTimestamp'] = creationTimestamp;

    return Response.ok(
      Utils.toJson(token),
      headers: headers,
    );
  }

  Future<Response> _tokenHoldersHandler(Request request) async {
    final tokenId = request.params['tokenId'] ?? '';
    final page = int.parse(request.url.queryParameters['page'] ?? '1');
    final searchText = request.url.queryParameters['search'] ?? '';

    if (page <= 0 ||
        page > 10000 ||
        tokenId.length != 26 ||
        searchText.length > 50) {
      return Response.internalServerError();
    }

    final holders =
        await DatabaseService().getTokenHolders(tokenId, page, searchText);

    return Response.ok(
      Utils.toJson(holders),
      headers: headers,
    );
  }

  Future<Response> _tokenTransactionsHandler(Request request) async {
    final tokenId = request.params['tokenId'] ?? '';
    final page = int.parse(request.url.queryParameters['page'] ?? '1');
    final searchText = request.url.queryParameters['search'] ?? '';

    if (page <= 0 ||
        page > 10000 ||
        tokenId.length != 26 ||
        searchText.length > 50) {
      return Response.internalServerError();
    }

    final txs =
        await DatabaseService().getTokenTransactions(tokenId, page, searchText);

    return Response.ok(
      Utils.toJson(txs),
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

String _createRewardTransactionsCsv(List rewards, dynamic marketHistory,
    String address, String currency, String timezone) {
  String csv =
      'Momentum Timestamp, Amount, Symbol, Value (${currency.toUpperCase()}), Price (${currency.toUpperCase()}), Reward Type, Account Height, Momentum Height, Hash, Sender, Receiver\n';

  num totalZnnRewards = 0;
  num totalQsrRewards = 0;
  num totalFiatValue = 0;

  rewards.forEach(((item) {
    String type = 'Stake';
    switch (item['rewardType']) {
      case 1:
        type = 'Delegation';
        break;
      case 2:
        type = 'Liquidity';
        break;
      case 3:
        type = 'Sentinel';
        break;
      case 4:
        type = 'Pillar';
        break;
    }

    final amount = item['amount'] / 100000000;

    final dt = tz.TZDateTime.fromMillisecondsSinceEpoch(
        (tz.getLocation(timezone)), item['momentumTimestamp'] * 1000);
    final midnight =
        DateTime.utc(dt.year, dt.month, dt.day).millisecondsSinceEpoch;

    final price = item['symbol'].toLowerCase() == 'znn'
        ? (marketHistory['znn'][currency][midnight.toString()] ?? 0)
        : 0;

    final fiatValue = amount * price;
    csv +=
        '''${item['momentumDateTime']}, ${amount}, ${item['symbol']}, ${fiatValue.toStringAsFixed(2)}, ${price.toStringAsFixed(2)}, ${type}, ${item['accountHeight']}, ${item['momentumHeight']}, ${item['hash']}, ${item['sourceAddress']}, ${address}\n''';

    if (item['symbol'].toLowerCase() == 'znn') {
      totalZnnRewards += amount;
    } else {
      totalQsrRewards += amount;
    }

    totalFiatValue += fiatValue;
  }));

  csv +=
      '\nTotal ZNN rewards:, ${totalZnnRewards}\nTotal QSR rewards:, ${totalQsrRewards}\nTotal ZNN value (${currency.toUpperCase()}):, ${totalFiatValue.toStringAsFixed(2)}';

  csv += '\n\nPrice information provided by CoinGecko.';

  return csv;
}

int _daysInBetween(int start, int end) {
  return ((end - start) / (24 * 60 * 60)).round();
}
