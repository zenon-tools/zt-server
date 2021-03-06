import 'package:postgresql2/postgresql.dart';

import '../config/config.dart';

class Table {
  static String get momentums => 'Momentums';
  static String get balances => 'Balances';
  static String get accounts => 'Accounts';
  static String get accountBlocks => 'AccountBlocks';
  static String get pillars => 'Pillars';
  static String get sentinels => 'Sentinels';
  static String get tokens => 'Tokens';
  static String get projects => 'Projects';
  static String get projectPhases => 'ProjectPhases';
  static String get votes => 'Votes';
}

class DatabaseService {
  static final DatabaseService _instance = DatabaseService._internal();

  factory DatabaseService() {
    return _instance;
  }

  DatabaseService._internal();

  late final Connection _conn;

  final _uri =
      'postgres://${Config.databaseUsername}:${Config.databasePassword}@${Config.databaseAddress}:${Config.databasePort}/${Config.databaseName}';

  init() async {
    _conn = await connect(_uri);
    print('Connected to database');
  }

  dispose() {
    _conn.close();
  }

  Future<int> getLatestHeight() async {
    List r = await _conn
        .query('SELECT MAX(height) FROM ${Table.momentums}')
        .toList();
    return r.isNotEmpty && r[0][0] != null ? r[0][0] : 0;
  }

  Future<dynamic> getStakes(String address) async {
    List r = await _conn.query(
        '''SELECT T1.momentumtimestamp, (T1.input::json->>'durationInSec')::int, T1.amount
                FROM ${Table.accountBlocks} T1
                WHERE method = 'Stake' and address = @address
                ORDER BY T1.momentumtimestamp DESC LIMIT 100''',
        {'address': address}).toList();

    List stakes = [];
    if (r.isNotEmpty) {
      for (final Row row in r) {
        if (row.toList().length == 3) {
          stakes.add({
            'startTimestamp': row[0],
            'lockUpDurationInSec': row[1],
            'stakedAmount': row[2]
          });
        }
      }
    }
    return stakes;
  }

  Future<dynamic> getDelegation(String address) async {
    List r = await _conn.query(
        '''SELECT T2.name, T1.delegationStartTimestamp, T3.balance
            FROM ${Table.accounts} T1, ${Table.pillars} T2, ${Table.balances} T3
            WHERE T1.address = @address
	          and T2.ownerAddress = T1.delegate
	          and T3.tokenStandard = @tokenStandard
	          and T3.address = @address
            LIMIT 1''',
        {
          'address': address,
          'tokenStandard': 'zts1znnxxxxxxxxxxxxx9z4ulx'
        }).toList();
    if (r.isNotEmpty && (r[0] as Row).toList().length == 3) {
      final row = r[0];
      return {
        'delegate': row[0],
        'delegationStartTimestamp': row[1],
        'delegatedBalance': row[2]
      };
    } else {
      return {};
    }
  }

  Future<dynamic> getSentinel(String address) async {
    List r = await _conn
        .query('''SELECT T1.registrationTimestamp, T1.isRevocable, T1.active
            FROM ${Table.sentinels} T1
            WHERE T1.owner = @address
            LIMIT 1''', {'address': address}).toList();
    if (r.isNotEmpty && (r[0] as Row).toList().length == 3) {
      final row = r[0];
      return {
        'registrationTimestamp': row[0],
        'isRevocable': row[1],
        'active': row[2]
      };
    } else {
      return {};
    }
  }

  Future<dynamic> getPillar(String address) async {
    List r = await _conn.query(
        '''SELECT T1.name, T1.spawnTimestamp, T1.slotCostQsr, T1.isRevocable, T1.revokeCooldown, T1.revokeTimestamp
            FROM ${Table.pillars} T1
            WHERE T1.ownerAddress = @address and T1.isRevoked = false
            LIMIT 1''', {'address': address}).toList();
    if (r.isNotEmpty && (r[0] as Row).toList().length == 6) {
      final row = r[0];
      return {
        'name': row[0],
        'spawnTimestamp': row[1],
        'slotCostQsr': row[2],
        'isRevocable': row[3],
        'revokeCooldown': row[4],
        'revokeTimestamp': row[5]
      };
    } else {
      return {};
    }
  }

  Future<dynamic> getBalances(String address) async {
    List r =
        await _conn.query('''SELECT T1.balance, T2.name, T2.symbol, T2.decimals
            FROM ${Table.balances} T1, tokens T2
            WHERE T1.address = @address and T2.tokenStandard = T1.tokenStandard
            LIMIT 50''', {'address': address}).toList();
    List balances = [];
    if (r.isNotEmpty) {
      for (final Row row in r) {
        if (row.toList().length == 4) {
          balances.add({
            'balance': row[0],
            'name': row[1],
            'symbol': row[2],
            'decimals': row[3]
          });
        }
      }
    }
    return balances;
  }

  Future<dynamic> getVotesByPillar(
      String pillar, int page, String searchText) async {
    List r = await _conn.query(
        '''SELECT T1.momentumHash, T1.momentumTimestamp, T2.url, T2.name, T3.name, T1.vote, T1.projectId
            FROM ${Table.votes} T1
            LEFT JOIN ${Table.projects} T2
                ON T2.id = T1.projectId
            LEFT JOIN ${Table.projectPhases} T3
                ON T3.id = T1.phaseId
            INNER JOIN ${Table.pillars} T4
	              ON T4.name = @pillar
            WHERE voterAddress = T4.ownerAddress and (T2.name ILIKE @search or T3.name ILIKE @search)
            ORDER BY T1.id DESC LIMIT 10
            OFFSET (@page - 1) * 10''',
        {'pillar': pillar, 'page': page, 'search': '%$searchText%'}).toList();

    List votes = [];
    if (r.isNotEmpty) {
      for (final Row row in r) {
        if (row.toList().length == 7 && row[3] != null) {
          votes.add({
            'momentumHash': row[0],
            'momentumTimestamp': row[1],
            'projectUrl': row[2],
            'projectName': row[3],
            'phaseName': row[4] ?? '',
            'vote': row[5],
            'projectId': row[6],
          });
        }
      }
    }
    return votes;
  }

  Future<dynamic> getAzProjects(int page, String searchText) async {
    List r = await _conn.query(
        '''SELECT T1.name, '' as phaseName, T1.id as projectId, T1.creationTimestamp, T1.url, T1.status, T1.yesVotes, T1.noVotes, T1.totalVotes, T1.znnFundsNeeded, T1.qsrFundsNeeded
            FROM ${Table.projects} T1
            WHERE T1.name ILIKE @search
            UNION ALL
            SELECT T3.name, T2.name, T2.projectId, T2.creationTimestamp, T2.url, T2.status, T2.yesVotes, T2.noVotes, T2.totalVotes, T2.znnFundsNeeded, T2.qsrFundsNeeded
            FROM ${Table.projectPhases} T2
            INNER JOIN ${Table.projects} T3
	            ON projectId = T3.id
            WHERE T2.name ILIKE @search
            ORDER BY creationTimestamp DESC LIMIT 10
            OFFSET (@page - 1) * 10''',
        {'page': page, 'search': '%$searchText%'}).toList();

    List proposals = [];
    if (r.isNotEmpty) {
      for (final Row row in r) {
        if (row.toList().length == 11) {
          proposals.add({
            'projectName': row[0],
            'phaseName': row[1],
            'projectId': row[2],
            'creationTimestamp': row[3],
            'url': row[4],
            'status': row[5],
            'yesVotes': row[6],
            'noVotes': row[7],
            'totalVotes': row[8],
            'znnFundsNeeded': row[9],
            'qsrFundsNeeded': row[10]
          });
        }
      }
    }
    return proposals;
  }

  Future<dynamic> getAzProjectById(String projectId) async {
    List r = await _conn.query(
        '''SELECT id, owner, name, description, url, znnFundsNeeded, qsrFundsNeeded, creationTimestamp, lastUpdateTimestamp, status, yesVotes, noVotes, totalVotes
           FROM ${Table.projects}
           WHERE id = @projectId
           ''', {'projectId': projectId}).toList();

    if (r.isNotEmpty) {
      Row row = r[0];
      return {
        'projectId': row[0],
        'owner': row[1],
        'name': row[2],
        'description': row[3],
        'url': row[4],
        'znnFundsNeeded': row[5],
        'qsrFundsNeeded': row[6],
        'creationTimestamp': row[7],
        'lastUpdateTimestamp': row[8],
        'status': row[9],
        'yesVotes': row[10],
        'noVotes': row[11],
        'totalVotes': row[12]
      };
    } else {
      return {};
    }
  }

  Future<dynamic> getAzPhasesByProjectId(String projectId) async {
    List r = await _conn.query(
        '''SELECT id, name, description, url, znnFundsNeeded, qsrFundsNeeded, creationTimestamp, status, yesVotes, noVotes, totalVotes
           FROM ${Table.projectPhases}
           WHERE projectId = @projectId
           ''', {'projectId': projectId}).toList();

    List phases = [];
    if (r.isNotEmpty) {
      for (final Row row in r) {
        phases.add({
          'phaseId': row[0],
          'name': row[1],
          'description': row[2],
          'url': row[3],
          'znnFundsNeeded': row[4],
          'qsrFundsNeeded': row[5],
          'creationTimestamp': row[6],
          'status': row[7],
          'yesVotes': row[8],
          'noVotes': row[9],
          'totalVotes': row[10]
        });
      }
    }
    return phases;
  }

  Future<dynamic> getAzVotesForProjectById(String projectId) async {
    List r = await _conn.query(
        '''SELECT DISTINCT ON (T2.name) T2.name, T1.vote, T1.momentumTimestamp
           FROM ${Table.votes} T1
           INNER JOIN ${Table.pillars} T2
	             ON T2.ownerAddress = T1.voterAddress
           WHERE T1.projectId = @projectId AND T1.phaseId = ''
           ORDER BY T2.name, T1.id DESC
           ''', {'projectId': projectId}).toList();

    List votes = [];
    if (r.isNotEmpty) {
      for (final Row row in r) {
        if (row.toList().length == 3) {
          votes.add({
            'pillarName': row[0],
            'vote': row[1],
            'momentumTimestamp': row[2]
          });
        }
      }
    }
    return votes;
  }

  Future<dynamic> getAzVotesForPhaseById(String phaseId) async {
    List r = await _conn.query(
        '''SELECT DISTINCT ON (T2.name) T2.name, T1.vote, T1.momentumTimestamp
           FROM ${Table.votes} T1
           INNER JOIN ${Table.pillars} T2
	             ON T2.ownerAddress = T1.voterAddress
           WHERE T1.phaseId = @phaseId
           ORDER BY T2.name, T1.id DESC
           ''', {'phaseId': phaseId}).toList();

    List votes = [];
    if (r.isNotEmpty) {
      for (final Row row in r) {
        if (row.toList().length == 3) {
          votes.add({
            'pillarName': row[0],
            'vote': row[1],
            'momentumTimestamp': row[2]
          });
        }
      }
    }
    return votes;
  }

  Future<dynamic> getPillarUpdateEvents(String pillar) async {
    List r = await _conn.query(
        '''SELECT T1.momentumheight, T1.momentumtimestamp, (T1.input::json->>'giveBlockRewardPercentage')::int as giveBlockRewardPercentage, (T1.input::json->>'giveDelegateRewardPercentage')::int as giveDelegateRewardPercentage
           FROM ${Table.accountBlocks} T1
           INNER JOIN ${Table.pillars} T2
	             ON T1.input::json->>'name' = T2.name and T2.name = @pillar
           WHERE method = 'UpdatePillar'
           ORDER BY T1.momentumheight ASC LIMIT 500
           ''', {'pillar': pillar}).toList();

    List events = [];
    if (r.isNotEmpty) {
      for (final Row row in r) {
        if (row.toList().length == 4) {
          events.add({
            'momentumHeight': row[0],
            'momentumTimestamp': row[1],
            'giveBlockRewardPercentage': row[2],
            'giveDelegateRewardPercentage': row[3]
          });
        }
      }
    }
    return events;
  }

  Future<dynamic> getPillarDelegators(String pillar) async {
    List r = await _conn
        .query('''SELECT T1.address, T1.delegationStartTimestamp, T2.balance
            FROM ${Table.accounts} T1
            INNER JOIN ${Table.balances} T2
	              ON T1.address = T2.address
            INNER JOIN pillars T3
                ON T3.name = @pillar
            WHERE T1.delegate = T3.ownerAddress and T2.tokenStandard = 'zts1znnxxxxxxxxxxxxx9z4ulx' and T2.balance >= 100000000
            ORDER BY T2.balance DESC LIMIT 1000
           ''', {'pillar': pillar}).toList();

    List delegators = [];
    if (r.isNotEmpty) {
      for (final Row row in r) {
        if (row.toList().length == 3) {
          delegators.add({
            'address': row[0],
            'delegationStartTimestamp': row[1],
            'delegationAmount': row[2]
          });
        }
      }
    }
    return delegators;
  }

  Future<dynamic> getPillarProfile(String pillar) async {
    List r = await _conn.query(
        '''SELECT ownerAddress, producerAddress, withdrawAddress, spawnTimestamp, slotCostQsr, votingActivity
            FROM ${Table.pillars}
            WHERE name = @pillar and isRevoked = false
            LIMIT 1
           ''', {'pillar': pillar}).toList();

    if (r.isNotEmpty) {
      Row row = r[0];
      return {
        'ownerAddress': row[0],
        'producerAddress': row[1],
        'withdrawAddress': row[2],
        'spawnTimestamp': row[3],
        'slotCostQsr': row[4],
        'votingActivity': row[5]
      };
    } else {
      return {};
    }
  }

  Future<dynamic> getPublicKeyByAddress(String address) async {
    List r = await _conn.query('''SELECT publicKey
            FROM ${Table.accounts}
            WHERE address = @address
            LIMIT 1
           ''', {'address': address}).toList();

    if (r.isNotEmpty) {
      return r[0][0];
    } else {
      return '';
    }
  }

  Future<dynamic> getDonations() async {
    List r = await _conn.query(
        '''SELECT T1.momentumTimestamp, T1.address, T1.amount, T2.symbol, T2.decimals, coalesce(T3.name, '')
            FROM ${Table.accountBlocks} T1
            INNER JOIN ${Table.tokens} T2
	              ON T2.tokenStandard = T1.tokenStandard
            LEFT JOIN pillars T3
                ON T1.address = T3.ownerAddress or T1.address = T3.withdrawAddress or T1.address = T3.producerAddress
            WHERE toAddress = @toAddress and address != 'z1qxemdeddedxt0kenxxxxxxxxxxxxxxxxh9amk0'
            ORDER BY momentumHeight DESC LIMIT 100
           ''', {'toAddress': Config.donationAddressZnn}).toList();

    List donations = [];
    if (r.isNotEmpty) {
      for (final Row row in r) {
        if (row.toList().length == 6) {
          donations.add({
            'momentumTimestamp': row[0],
            'address': row[1],
            'amount': row[2],
            'symbol': row[3],
            'decimals': row[4],
            'pillar': row[5]
          });
        }
      }
    }
    return donations;
  }
}
