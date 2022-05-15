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
    List r = await _conn.query('SELECT MAX(height) FROM momentums').toList();
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
            FROM ${Table.accounts} T1, pillars T2, balances T3
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
            FROM sentinels T1
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
            WHERE T1.ownerAddress = @address
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
}
