import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'package:http/http.dart' as http;
import 'package:mqtt_client/mqtt_client.dart';
import 'package:mqtt_client/mqtt_server_client.dart';
import '../models/app_settings.dart';
import '../models/relay_config.dart';

class MqttService {
  MqttServerClient? _client;
  StreamSubscription<List<MqttReceivedMessage<MqttMessage>>>? _updatesSubscription;
  AppSettings? _settings;

  final _connectionController = StreamController<bool>.broadcast();
  final _temperatureController = StreamController<String>.broadcast();
  final _relayStateController = StreamController<Map<String, bool>>.broadcast();
  final _espStatusController = StreamController<bool>.broadcast();

  Stream<bool> get connectionStream => _connectionController.stream;
  Stream<String> get temperatureStream => _temperatureController.stream;
  Stream<Map<String, bool>> get relayStateStream => _relayStateController.stream;
  Stream<bool> get espStatusStream => _espStatusController.stream;

  bool get isConnected =>
      _client?.connectionStatus?.state == MqttConnectionState.connected;

  String _webSocketServerAddress(String broker, String mode) {
    var host = broker.trim();
    host = host.replaceFirst(
      RegExp(r'^(?:mqtts?|wss?|https?)://', caseSensitive: false),
      '',
    );
    host = host.split('/').first;
    host = host.replaceFirst(RegExp(r':\d+$'), '');
    final scheme = mode == 'wss' ? 'wss' : 'ws';
    return '$scheme://$host/mqtt';
  }

  bool _isPrivateIp(String ip) {
    final parts = ip.split('.');
    if (parts.length != 4) return false;
    final a = int.tryParse(parts[0]) ?? 0;
    final b = int.tryParse(parts[1]) ?? 0;
    if (a == 10) return true;
    if (a == 172 && b >= 16 && b <= 31) return true;
    if (a == 192 && b == 168) return true;
    if (a == 127) return true;
    return false;
  }

  bool _isCertificateForDomain(X509Certificate cert, String expectedDomain) {
    final domain = expectedDomain.trim().toLowerCase();
    if (domain.isEmpty) return false;

    final subject = cert.subject.toLowerCase();
    if (subject.contains(domain)) return true;

    try {
      if (cert.pem.toLowerCase().contains(domain)) return true;
    } catch (_) {}

    return false;
  }

  /// Resolve دامنه با DNS-over-HTTPS (Cloudflare) تا از DNS خراب موبایل رد شویم
  Future<String?> _resolveWithDoH(String domain) async {
    try {
      final uri = Uri.parse(
        'https://cloudflare-dns.com/dns-query?name=$domain&type=A',
      );
      final response = await http.get(
        uri,
        headers: {'Accept': 'application/dns-json'},
      ).timeout(const Duration(seconds: 8));

      if (response.statusCode != 200) return null;

      final data = jsonDecode(response.body) as Map<String, dynamic>;
      final answers = data['Answer'] as List<dynamic>?;
      if (answers == null || answers.isEmpty) return null;

      for (final answer in answers) {
        final map = answer as Map<String, dynamic>;
        if (map['type'] == 1) {
          // نوع A
          final ip = map['data'] as String?;
          if (ip != null && !_isPrivateIp(ip)) {
            return ip;
          }
        }
      }
    } catch (_) {}
    return null;
  }

  /// Resolve دامنه: اول سیستم، اگر خراب بود از DoH استفاده کن
  Future<String?> _resolveToPublicIpv4(String domain) async {
    // ۱. اول با DNS سیستم (سریع‌تر)
    try {
      final addresses = await InternetAddress.lookup(
        domain,
        type: InternetAddressType.IPv4,
      );
      if (addresses.isNotEmpty) {
        final ip = addresses.first.address;
        if (!_isPrivateIp(ip)) return ip;
      }
    } catch (_) {}

    // ۲. اگر سیستم IP خصوصی داد یا شکست خورد → از Cloudflare DoH
    return await _resolveWithDoH(domain);
  }

  Future<void> connect(AppSettings settings) async {
    await disconnect();
    _settings = settings;

    final clientId = settings.clientId.trim().isEmpty
        ? 'khanjan_${DateTime.now().millisecondsSinceEpoch}'
        : settings.clientId.trim();

    final broker = settings.broker.trim();
    final port = settings.port == 0 ? 1883 : settings.port;

    final mode = settings.connectionMode;
    final useWs = mode == 'ws' ||
        mode == 'wss' ||
        (mode != 'tls' && mode != 'tcp' && settings.useWebSocket);

    final serverAddress =
        useWs ? _webSocketServerAddress(broker, mode) : broker;

    String hostForClient = serverAddress;
    bool connectedViaIp = false;
    final isAlreadyIp = RegExp(r'^\d{1,3}(\.\d{1,3}){3}$').hasMatch(broker);

    if (!useWs && mode == 'tls') {
      if (isAlreadyIp) {
        hostForClient = broker;
        connectedViaIp = true;
      } else {
        // دامنه را به IP عمومی resolve کن (با DoH در صورت نیاز)
        final resolvedIp = await _resolveToPublicIpv4(broker);
        if (resolvedIp != null) {
          hostForClient = resolvedIp;
          connectedViaIp = true;
        }
        // اگر resolve کامل شکست خورد، با دامنه ادامه می‌دهیم
      }
    }

    _client = MqttServerClient.withPort(hostForClient, clientId, port);
    _client!.logging(on: false);
    _client!.keepAlivePeriod = 60;
    _client!.autoReconnect = true;
    _client!.resubscribeOnAutoReconnect = true;
    _client!.secure = mode == 'tls';
    _client!.connectTimeoutPeriod = 20000;

    if (connectedViaIp && mode == 'tls') {
      final expectedDomain =
          isAlreadyIp ? 'khanjanhome.duckdns.org' : broker;

      _client!.onBadCertificate = (Object cert) {
        if (cert is X509Certificate) {
          return _isCertificateForDomain(cert, expectedDomain);
        }
        return false;
      };
    }

    if (useWs) {
      _client!.useWebSocket = true;
    }

    _client!.onConnected = () {
      _connectionController.add(true);
    };

    _client!.onDisconnected = () {
      _connectionController.add(false);
      _espStatusController.add(false);
    };

    _client!.onAutoReconnected = () {
      _connectionController.add(true);
      _subscribeAll();
    };

    final connMess = MqttConnectMessage()
        .withClientIdentifier(clientId)
        .startClean();

    if (settings.username.trim().isNotEmpty) {
      connMess.authenticateAs(
        settings.username.trim(),
        settings.password,
      );
    }
    _client!.connectionMessage = connMess;

    try {
      await _client!.connect();
    } catch (e) {
      _connectionController.add(false);
      _espStatusController.add(false);
      try {
        _client?.disconnect();
      } catch (_) {}
      _client = null;
      rethrow;
    }

    if (_client?.connectionStatus?.state == MqttConnectionState.connected) {
      await _updatesSubscription?.cancel();
      _updatesSubscription = _client!.updates?.listen(
        _onMessage,
        onError: (Object error, StackTrace stack) {},
      );
      _connectionController.add(true);
      _subscribeAll();
    } else {
      _connectionController.add(false);
      _espStatusController.add(false);
      throw Exception('MQTT connection failed');
    }
  }

  String _heartbeatTopicFor(String statusTopic) {
    final topic = statusTopic.trim();
    final slash = topic.lastIndexOf('/');
    if (slash < 0) return 'heartbeat';
    return '${topic.substring(0, slash)}/heartbeat';
  }

  void _subscribeAll() {
    if (_client == null || _settings == null) return;
    final s = _settings!;

    if (s.temperatureTopic.trim().isNotEmpty) {
      _client!.subscribe(s.temperatureTopic.trim(), MqttQos.atLeastOnce);
    }
    if (s.espStatusTopic.trim().isNotEmpty) {
      _client!.subscribe(s.espStatusTopic.trim(), MqttQos.atLeastOnce);
      final heartbeatTopic = _heartbeatTopicFor(s.espStatusTopic);
      if (heartbeatTopic != s.espStatusTopic.trim()) {
        _client!.subscribe(heartbeatTopic, MqttQos.atLeastOnce);
      }
    }
    for (final r in s.relays) {
      if (r.stateTopic.trim().isNotEmpty) {
        _client!.subscribe(r.stateTopic.trim(), MqttQos.atLeastOnce);
      }
    }
  }

  void _onMessage(List<MqttReceivedMessage<MqttMessage>> events) {
    for (final event in events) {
      final topic = event.topic;
      final recMess = event.payload as MqttPublishMessage;
      final payload =
          MqttPublishPayload.bytesToStringAsString(recMess.payload.message)
              .trim();

      if (_settings == null) continue;

      final heartbeatTopic = _heartbeatTopicFor(_settings!.espStatusTopic);
      if (topic == heartbeatTopic) {
        if (recMess.header?.retain == true) {
          continue;
        }
        _espStatusController.add(true);
        continue;
      }

      if (topic == _settings!.espStatusTopic) {
        final normalized = payload.toLowerCase();
        if (normalized.contains('offline')) {
          _espStatusController.add(false);
        } else if (normalized.contains('online') ||
            normalized == 'on' ||
            normalized == '1' ||
            normalized == 'true') {
          if (recMess.header?.retain != true) {
            _espStatusController.add(true);
          }
        } else {
          if (recMess.header?.retain != true) {
            _espStatusController.add(false);
          }
        }
        continue;
      }

      if (topic == _settings!.temperatureTopic) {
        if (recMess.header?.retain == true) {
          continue;
        }
        _temperatureController.add(payload);
        _espStatusController.add(true);
        continue;
      }

      for (final r in _settings!.relays) {
        if (topic == r.stateTopic) {
          final on = payload.toUpperCase() == r.onPayload.toUpperCase() ||
              payload == '1' ||
              payload.toLowerCase() == 'true';
          _relayStateController.add({r.id: on});
          break;
        }
      }
    }
  }

  Future<void> publish(String topic, String message) async {
    if (_client == null || !isConnected) return;
    final builder = MqttClientPayloadBuilder();
    builder.addString(message);
    _client!.publishMessage(topic, MqttQos.atLeastOnce, builder.payload!);
  }

  Future<void> toggleRelay(RelayConfig relay, bool turnOn) async {
    final payload = turnOn ? relay.onPayload : relay.offPayload;
    await publish(relay.commandTopic, payload);
  }

  Future<void> disconnect() async {
    try {
      await _updatesSubscription?.cancel();
    } catch (_) {}
    _updatesSubscription = null;
    try {
      _client?.disconnect();
    } catch (_) {}
    _client = null;
    _connectionController.add(false);
    _espStatusController.add(false);
  }

  void dispose() {
    disconnect();
    _connectionController.close();
    _temperatureController.close();
    _relayStateController.close();
    _espStatusController.close();
  }
}
