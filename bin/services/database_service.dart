import 'package:postgresql2/postgresql.dart';

import '../config/config.dart';

class Table {
  static String get momentums => 'Momentums';
  static String get balances => 'Balances';
  static String get accounts => 'Accounts';
  static String get accountBlocks => 'AccountBlocks';
  static String get pillars => 'Pillars';
  static String get sentinels => 'Sentinels';
  static String get stakes => 'Stakes';
  static String get tokens => 'Tokens';
  static String get projects => 'Projects';
  static String get projectPhases => 'ProjectPhases';
  static String get votes => 'Votes';
  static String get fusions => 'Fusions';
  static String get rewardTransactions => 'RewardTransactions';
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
        '''SELECT startTimestamp, durationInSec, znnAmount
                FROM ${Table.stakes}
                WHERE isActive = true and address = @address
                ORDER BY startTimestamp DESC LIMIT 100''',
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

  Future<Map<String, dynamic>>
      getPillarVotingActivityAndProducedMomentums() async {
    List r = await _conn
        .query('''SELECT ownerAddress, votingActivity, producedMomentumCount
            FROM ${Table.pillars}
            WHERE isRevoked = false
            ''').toList();
    Map<String, dynamic> pillars = {};
    if (r.isNotEmpty) {
      for (final Row row in r) {
        pillars[row[0]] = {
          'votingActivity': row[1],
          'producedMomentumCount': row[2]
        };
      }
    }
    return pillars;
  }

  Future<dynamic> getAccountTokens(String address) async {
    List r = await _conn.query(
        '''SELECT T1.balance, T2.name, T2.symbol, T2.decimals, T2.tokenStandard
            FROM ${Table.balances} T1, tokens T2
            WHERE T1.balance > 0 and T1.address = @address and T2.tokenStandard = T1.tokenStandard
            ORDER BY T2.name ASC LIMIT 50''', {'address': address}).toList();
    List balances = [];
    if (r.isNotEmpty) {
      for (final Row row in r) {
        if (row.toList().length == 5) {
          balances.add({
            'balance': row[0],
            'name': row[1],
            'symbol': row[2],
            'decimals': row[3],
            'tokenStandard': row[4],
          });
        }
      }
    }
    return balances;
  }

  Future<dynamic> getAccounts(int page, String searchText) async {
    List r = await _conn.query(
        '''SELECT T1.address, coalesce(T2.balance, 0) as znnBalance, coalesce(T3.balance, 0) as qsrBalance, T1.blockCount
            FROM accounts T1
            LEFT JOIN balances T2
	            ON T2.address = T1.address and T2.tokenStandard = 'zts1znnxxxxxxxxxxxxx9z4ulx'
            LEFT JOIN balances T3
              ON T3.address = T1.address and T3.tokenStandard = 'zts1qsrxxxxxxxxxxxxxmrhjll'
            WHERE T1.address ILIKE @search
            ORDER BY znnBalance DESC LIMIT 20
            OFFSET (@page - 1) * 20''',
        {'page': page, 'search': '%$searchText%'}).toList();
    List accounts = [];
    if (r.isNotEmpty) {
      for (final Row row in r) {
        if (row.toList().length == 4) {
          accounts.add({
            'address': row[0],
            'znnBalance': row[1],
            'qsrBalance': row[2],
            'blockCount': row[3]
          });
        }
      }
    }
    return accounts;
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
        '''SELECT ownerAddress, producerAddress, withdrawAddress, spawnTimestamp, slotCostQsr, votingActivity, producedMomentumCount
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
        'votingActivity': row[5],
        'producedMomentumCount': row[6]
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

  Future<dynamic> getAccountDetails(String address) async {
    List r = await _conn.query(
        '''SELECT T1.blockCount, T1.publicKey, COALESCE(T2.balance, 0) as znnBalance, COALESCE(T3.balance, 0) as qsrBalance
          FROM ${Table.accounts} T1
          LEFT JOIN ${Table.balances} T2
	          ON T2.address = T1.address and T2.tokenStandard = 'zts1znnxxxxxxxxxxxxx9z4ulx'
          LEFT JOIN ${Table.balances} T3
	          ON T3.address = T1.address and T3.tokenStandard = 'zts1qsrxxxxxxxxxxxxxmrhjll'
          WHERE T1.address = @address
          ''', {'address': address}).toList();

    if (r.isNotEmpty) {
      Row row = r[0];
      return {
        'height': row[0],
        'publicKey': row[1],
        'znnBalance': row[2],
        'qsrBalance': row[3]
      };
    } else {
      return {'height': 0, 'publicKey': '', 'znnBalance': 0, 'qsrBalance': 0};
    }
  }

  Future<dynamic> getAddressActiveSince(String address) async {
    List r = await _conn
        .query('''SELECT address, MIN(momentumTimestamp) as momentumTimestamp
          FROM ${Table.accountBlocks}
          WHERE address = @address or toAddress = @address
          GROUP BY address
          ORDER BY momentumTimestamp ASC LIMIT 1
          ''', {'address': address}).toList();

    if (r.isNotEmpty) {
      return r[0][1];
    } else {
      return 0;
    }
  }

  Future<dynamic> getAddressTransactions(String address, int page,
      {int limit = 10}) async {
    List r = await _conn.query(
        '''SELECT T1.hash, T1.momentumTimestamp, T1.method, T1.amount, coalesce(T2.symbol, '') as symbol, coalesce(T2.decimals, 0) as decimals, T1.address, T1.toAddress, T1.pairedAccountBlock
            FROM ${Table.accountBlocks} T1
            LEFT JOIN tokens T2
	            ON T2.tokenStandard = T1.tokenStandard
            WHERE address = @address
            ORDER BY T1.height DESC LIMIT @limit
            OFFSET (@page - 1) * @limit
           ''', {'address': address, 'page': page, 'limit': limit}).toList();

    List txs = [];
    if (r.isNotEmpty) {
      for (final Row row in r) {
        if (row.toList().length == 9) {
          txs.add({
            'hash': row[0],
            'momentumTimestamp': row[1],
            'method': row[2].length == 0
                ? row[4].length == 0
                    ? 'Unknown'
                    : 'Transfer'
                : row[2],
            'amount': row[3],
            'symbol': row[4],
            'decimals': row[5],
            'address': row[6],
            'toAddress': row[7],
            'pairedAccountBlock': row[8]
          });
        }
      }
    }
    return txs;
  }

  Future<dynamic> getAddressReceivedTransactionData(String hash) async {
    List r = await _conn.query(
        '''SELECT T1.hash, T1.amount, coalesce(T2.symbol, '') as symbol, coalesce(T2.decimals, 0) as decimals, T1.method, T1.address, T1.toAddress
            FROM ${Table.accountBlocks} T1
            LEFT JOIN tokens T2
	            ON T2.tokenStandard = T1.tokenStandard
            WHERE T1.pairedAccountBlock = @hash
           ''', {'hash': hash}).toList();

    if (r.isNotEmpty) {
      for (final Row row in r) {
        if (row.toList().length == 7) {
          return {
            'amount': row[1],
            'symbol': row[2],
            'decimals': row[3],
            'method': row[4],
            'address': row[5],
            'toAddress': row[6]
          };
        }
      }
    }
    return {};
  }

  Future<dynamic> getAddressUnreceivedTransactions(
      String address, int page) async {
    List r = await _conn.query(
        '''SELECT T1.hash, T1.momentumTimestamp, T1.method, T1.amount, T2.symbol, T2.decimals, T1.address, T1.toAddress, T1.pairedAccountBlock
            FROM ${Table.accountBlocks} T1
            INNER JOIN tokens T2
	            ON T2.tokenStandard = T1.tokenStandard
            WHERE (toAddress = @address) and (pairedAccountBlock = '') IS NOT @isReceived
            ORDER BY momentumHeight DESC LIMIT 10
            OFFSET (@page - 1) * 10
           ''',
        {'address': address, 'page': page, 'isReceived': false}).toList();

    List txs = [];
    if (r.isNotEmpty) {
      for (final Row row in r) {
        if (row.toList().length == 9) {
          txs.add({
            'hash': row[0],
            'momentumTimestamp': row[1],
            'method': row[2].length == 0 ? 'Transfer' : row[2],
            'amount': row[3],
            'symbol': row[4],
            'decimals': row[5],
            'address': row[6],
            'toAddress': row[7]
          });
        }
      }
    }
    return txs;
  }

  Future<dynamic> getAddressRewardTransactions(
      String address, int startTimestamp, int endTimestamp, String timezone,
      {String ignoredToken = ''}) async {
    List r = await _conn.query('''SELECT hash, rewardType, momentumTimestamp,
            to_timestamp(momentumTimestamp)::timestamp WITH TIME ZONE AT TIME ZONE @timezone ||
            replace('+' || to_char(current_timestamp AT TIME ZONE @timezone - current_timestamp AT TIME ZONE 'UTC', 'HH24:MMFM'), '+-', '-'),
            momentumHeight, amount, sourceAddress, accountHeight, tokenStandard,
            CASE WHEN tokenStandard = 'zts1znnxxxxxxxxxxxxx9z4ulx' THEN 'ZNN' ELSE 'QSR' END as symbol
            FROM ${Table.rewardTransactions}
            WHERE address = @address and momentumTimestamp >= @startTimestamp and momentumTimestamp < @endTimestamp and tokenStandard != @ignoredToken
            ORDER BY accountHeight DESC LIMIT 10000''', {
      'address': address,
      'startTimestamp': startTimestamp,
      'endTimestamp': endTimestamp,
      'timezone': timezone,
      'ignoredToken': ignoredToken
    }).toList();
    List txs = [];
    if (r.isNotEmpty) {
      for (final Row row in r) {
        txs.add({
          'hash': row[0],
          'rewardType': row[1],
          'momentumTimestamp': row[2],
          'momentumDateTime': row[3],
          'momentumHeight': row[4],
          'amount': row[5],
          'sourceAddress': row[6],
          'accountHeight': row[7],
          'tokenStandard': row[8],
          'symbol': row[9],
        });
      }
    }
    return txs;
  }

  Future<dynamic> getAddressRewardTransactionsCount(String address) async {
    List r = await _conn.query('''SELECT COUNT(*)
            FROM ${Table.rewardTransactions}
            WHERE address = @address
            LIMIT 10000''', {'address': address}).toList();
    return r.isNotEmpty && r[0][0] != null ? r[0][0] : 0;
  }

  Future<dynamic> getAddressAzProposals(String address, int page) async {
    List r = await _conn.query(
        '''SELECT T1.name, '' as phaseName, T1.id as projectId, T1.creationTimestamp, T1.url, T1.status
            FROM ${Table.projects} T1
            WHERE owner = @owner and T1.name ILIKE '%%' 
            UNION ALL
            SELECT T3.name, T2.name, T2.projectId, T2.creationTimestamp, T2.url, T2.status
            FROM ${Table.projectPhases} T2
            INNER JOIN ${Table.projects} T3
	            ON projectId = T3.id
            WHERE owner = @owner and T2.name ILIKE '%%'
            ORDER BY creationTimestamp DESC LIMIT 10
            OFFSET (@page - 1) * 10
           ''', {'owner': address, 'page': page}).toList();

    List proposals = [];
    if (r.isNotEmpty) {
      for (final Row row in r) {
        if (row.toList().length == 6) {
          proposals.add({
            'projectName': row[0],
            'phaseName': row[1],
            'projectId': row[2],
            'creationTimestamp': row[3],
            'url': row[4],
            'status': row[5]
          });
        }
      }
    }
    return proposals;
  }

  Future<dynamic> getAddressFusions(String address, int page) async {
    List r = await _conn.query(
        '''SELECT momentumHeight, momentumTimestamp, qsrAmount, expirationHeight, address, beneficiary
            FROM ${Table.fusions}
            WHERE (address = @address or beneficiary = @address) and isActive = true
            ORDER BY momentumTimestamp DESC LIMIT 10
            OFFSET (@page - 1) * 10
           ''', {'address': address, 'page': page}).toList();

    List fusions = [];
    if (r.isNotEmpty) {
      for (final Row row in r) {
        if (row.toList().length == 6) {
          fusions.add({
            'momentumHeight': row[0],
            'momentumTimestamp': row[1],
            'qsrAmount': row[2],
            'expirationHeight': row[3],
            'address': row[4],
            'beneficiary': row[5]
          });
        }
      }
    }
    return fusions;
  }

  Future<dynamic> getAccountFusedQsr(String address) async {
    List r = await _conn.query('''SELECT qsrAmount
          FROM ${Table.fusions}
          WHERE beneficiary = @address and isActive = true
          LIMIT 100
          ''', {'address': address}).toList();

    if (r.isNotEmpty) {
      return r.fold<num>(0, (sum, row) => sum + row[0]);
    } else {
      return 0;
    }
  }

  Future<dynamic> getToken(String tokenStandard) async {
    List r = await _conn.query(
        '''SELECT name, symbol, domain, decimals, owner, totalSupply, maxSupply, isBurnable, isMintable, isUtility, totalBurned, lastUpdateTimestamp, holderCount
            FROM ${Table.tokens}
            WHERE tokenStandard = @tokenStandard
           ''', {'tokenStandard': tokenStandard}).toList();
    if (r.isNotEmpty) {
      Row row = r[0];
      return {
        'name': row[0],
        'symbol': row[1],
        'domain': row[2],
        'decimals': row[3],
        'owner': row[4],
        'totalSupply': row[5],
        'maxSupply': row[6],
        'isBurnable': row[7],
        'isMintable': row[8],
        'isUtility': row[9],
        'totalBurned': row[10],
        'lastUpdateTimestamp': row[11],
        'holderCount': row[12]
      };
    } else {
      return {};
    }
  }

  Future<dynamic> getTokenCreationTimestamp(String tokenStandard) async {
    if (tokenStandard == 'zts1znnxxxxxxxxxxxxx9z4ulx' ||
        tokenStandard == 'zts1qsrxxxxxxxxxxxxxmrhjll') {
      return 0;
    }

    List r = await _conn.query('''SELECT momentumTimestamp
            FROM ${Table.accountBlocks}
            WHERE tokenStandard = @tokenStandard
            ORDER BY momentumHeight ASC LIMIT 1
           ''', {'tokenStandard': tokenStandard}).toList();
    return r.isNotEmpty ? r[0][0] : 0;
  }

  Future<dynamic> getTokenLastUpdateTimestamp(String tokenStandard) async {
    List r = await _conn.query('''SELECT momentumTimestamp
            FROM ${Table.accountBlocks}
            WHERE method = 'UpdateToken' and input::json->>'tokenStandard' = @tokenStandard
            ORDER BY momentumHeight DESC LIMIT 1
           ''', {'tokenStandard': tokenStandard}).toList();
    return r.isNotEmpty ? r[0][0] : 0;
  }

  Future<dynamic> getTokens(int page, String searchText) async {
    List r = await _conn.query(
        '''SELECT tokenStandard, name, symbol, domain, owner, totalSupply, maxSupply, decimals, holderCount
            FROM ${Table.tokens}
            WHERE name ILIKE @search or symbol ILIKE @search or domain ILIKE @search or tokenStandard ILIKE @search or owner ILIKE @search
            ORDER BY holderCount DESC LIMIT 20
            OFFSET (@page - 1) * 20
           ''', {'page': page, 'search': '%$searchText%'}).toList();

    List tokens = [];
    if (r.isNotEmpty) {
      for (final Row row in r) {
        if (row.toList().length == 9) {
          tokens.add({
            'tokenStandard': row[0],
            'name': row[1],
            'symbol': row[2],
            'domain': row[3],
            'owner': row[4],
            'totalSupply': row[5],
            'maxSupply': row[6],
            'decimals': row[7],
            'holderCount': row[8]
          });
        }
      }
    }
    return tokens;
  }

  Future<dynamic> getTokenHolders(
      String tokenId, int page, String searchText) async {
    List r = await _conn.query('''SELECT address, balance
            FROM ${Table.balances}
            WHERE tokenStandard = @tokenStandard and balance > 0 and address ILIKE @search
            ORDER BY balance DESC LIMIT 10
            OFFSET (@page - 1) * 10''', {
      'tokenStandard': tokenId,
      'page': page,
      'search': '%$searchText%'
    }).toList();
    List holders = [];
    if (r.isNotEmpty) {
      for (final Row row in r) {
        holders.add({'address': row[0], 'balance': row[1]});
      }
    }
    return holders;
  }

  Future<dynamic> getTokenTransactions(
      String tokenId, int page, String searchText) async {
    List r = await _conn.query(
        '''SELECT hash, momentumTimestamp, method, amount, address, toAddress
            FROM ${Table.accountBlocks}
            WHERE tokenStandard = @tokenStandard and (address ILIKE @search or toAddress ILIKE @search)
            ORDER BY momentumTimestamp DESC LIMIT 10
            OFFSET (@page - 1) * 10''',
        {
          'tokenStandard': tokenId,
          'page': page,
          'search': '%$searchText%'
        }).toList();
    List txs = [];
    if (r.isNotEmpty) {
      for (final Row row in r) {
        txs.add({
          'hash': row[0],
          'momentumTimestamp': row[1],
          'method': row[2].length == 0
              ? row[4].length == 0
                  ? 'Unknown'
                  : 'Transfer'
              : row[2],
          'amount': row[3],
          'address': row[4],
          'toAddress': row[5]
        });
      }
    }
    return txs;
  }
}
