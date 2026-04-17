<?php
/**
 * cron_update.php
 * Автоматическое обновление статистики из Leads.su (запуск по крону каждый час)
 * Использование: php cron_update.php
 * Cron: 0 * * * * php /path/to/cron_update.php
 */

$token = '2104ce6ccd2ce95458a53926597416eb';

// Определяем базовый URL (протокол + хост) для вызова leads-proxy.php
$proxyUrl = (php_sapi_name() === 'cli')
    ? 'http://localhost' . dirname($_SERVER['SCRIPT_NAME'] ?? '') . '/leads-proxy.php'
    : '';

// При CLI-запуске вызываем leads-proxy.php напрямую через include
if (php_sapi_name() === 'cli') {
    // Обновляем за последние 3 дня (на случай задержки данных в API)
    $endDate = date('Y-m-d');
    $startDate = date('Y-m-d', strtotime('-3 days'));

    $_GET['action'] = 'update_stats';
    $_GET['token'] = $token;
    $_GET['start_date'] = $startDate;
    $_GET['end_date'] = $endDate;
    $_GET['method'] = 'reports/summary';
    $_GET['grouping'] = 'day';
    $_GET['fields'] = 'offer_id,source';
    $_SERVER['REQUEST_METHOD'] = 'GET';

    ob_start();
    include __DIR__ . '/leads-proxy.php';
    $response = ob_get_clean();

    $logMessage = date('Y-m-d H:i:s') . " [CLI] {$startDate} → {$endDate}: {$response}\n";
    file_put_contents(__DIR__ . '/cron.log', $logMessage, FILE_APPEND);
    echo $logMessage;
    exit;
}

// При HTTP-запуске используем cURL к самому серверу
$endDate = date('Y-m-d');
$startDate = date('Y-m-d', strtotime('-3 days'));

$selfBase = (isset($_SERVER['HTTPS']) && $_SERVER['HTTPS'] === 'on' ? 'https' : 'http')
    . '://' . ($_SERVER['HTTP_HOST'] ?? 'localhost')
    . dirname($_SERVER['SCRIPT_NAME']) . '/leads-proxy.php';

$url = "{$selfBase}?action=update_stats&token={$token}&start_date={$startDate}&end_date={$endDate}&method=reports/summary&grouping=day&fields=offer_id,source";

$ch = curl_init();
curl_setopt($ch, CURLOPT_URL, $url);
curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
curl_setopt($ch, CURLOPT_TIMEOUT, 120);
curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, false);
$response = curl_exec($ch);
$httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
$error = curl_error($ch);
curl_close($ch);

$logMessage = date('Y-m-d H:i:s') . " [HTTP] ";
if ($response === false) {
    $logMessage .= "Ошибка cURL: $error\n";
} else {
    $logMessage .= "HTTP $httpCode - $response\n";
}
file_put_contents(__DIR__ . '/cron.log', $logMessage, FILE_APPEND);

header('Content-Type: application/json');
echo json_encode(['status' => 'ok', 'log' => trim($logMessage)]);
?>