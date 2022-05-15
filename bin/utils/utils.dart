import 'dart:convert';

class Utils {
  static String toJson(dynamic values) {
    return JsonEncoder.withIndent(' ').convert(values);
  }
}
