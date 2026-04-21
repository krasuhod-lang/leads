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
    // Окно: "вчера 00:00" → "сейчас 23:59:59" — последние 4 дня плюс текущий.
    // ВАЖНО: end_date в API leads.su эксклюзивен по умолчанию. Если передать
    // голый Y-m-d, текущий день обрезается. Поэтому формируем явное время:
    // start = "Y-m-d 00:00:00", end = "Y-m-d 23:59:59".
    $endDay = date('Y-m-d');
    $startDay = date('Y-m-d', strtotime('-3 days'));
    $startDate = "{$startDay} 00:00:00";
    $endDate   = "{$endDay} 23:59:59";

    // 1. Обновление статистики Leads.su
    $_GET['action'] = 'update_stats';
    $_GET['token'] = $token;
    $_GET['start_date'] = $startDate;
    $_GET['end_date'] = $endDate;
    $_GET['method'] = 'reports/summary';
    $_GET['grouping'] = 'day';
    $_GET['field'] = 'offer_id,source,aff_sub1';
    $_SERVER['REQUEST_METHOD'] = 'GET';

    ob_start();
    include __DIR__ . '/leads-proxy.php';
    $response = ob_get_clean();

    $logMessage = date('Y-m-d H:i:s') . " [CLI] Leads.su {$startDate} → {$endDate}: {$response}\n";
    file_put_contents(__DIR__ . '/cron.log', $logMessage, FILE_APPEND);
    echo $logMessage;

    // 2. Обновление данных Яндекс.Метрики (баннерная статистика).
    // YM ждёт даты вида YYYY-MM-DD без времени и обе границы инклюзивны,
    // поэтому передаём только даты.
    $_GET['action'] = 'ym_fetch_banner';
    $_GET['start_date'] = $startDay;
    $_GET['end_date'] = $endDay;
    unset($_GET['token'], $_GET['method'], $_GET['grouping'], $_GET['field']);

    ob_start();
    include __DIR__ . '/leads-proxy.php';
    $ymResponse = ob_get_clean();

    $ymLogMessage = date('Y-m-d H:i:s') . " [CLI] YM Banner {$startDay} → {$endDay}: {$ymResponse}\n";
    file_put_contents(__DIR__ . '/cron.log', $ymLogMessage, FILE_APPEND);
    echo $ymLogMessage;

    // 3. Обновление кэша офферов с рыночными показателями (раз в час достаточно).
    $_GET = [];
    $_GET['action'] = 'fetch_offers_market';
    $_GET['token'] = $token;

    ob_start();
    include __DIR__ . '/leads-proxy.php';
    $offersResponse = ob_get_clean();

    $offersLogMessage = date('Y-m-d H:i:s') . " [CLI] Offers market: {$offersResponse}\n";
    file_put_contents(__DIR__ . '/cron.log', $offersLogMessage, FILE_APPEND);
    echo $offersLogMessage;

    // 4. Подтянуть журнал событий (новые офферы, остановки, изменение выплат).
    $_GET = [];
    $_GET['action'] = 'fetch_notifications';
    $_GET['token'] = $token;

    ob_start();
    include __DIR__ . '/leads-proxy.php';
    $notifResponse = ob_get_clean();

    file_put_contents(__DIR__ . '/cron.log',
        date('Y-m-d H:i:s') . " [CLI] Notifications: {$notifResponse}\n", FILE_APPEND);
    echo "Notifications: {$notifResponse}\n";

    // 5. Проверить пороги для Telegram-уведомлений.
    $_GET = [];
    $_GET['action'] = 'tg_check_alerts';

    ob_start();
    include __DIR__ . '/leads-proxy.php';
    $alertsResponse = ob_get_clean();

    file_put_contents(__DIR__ . '/cron.log',
        date('Y-m-d H:i:s') . " [CLI] TG alerts: {$alertsResponse}\n", FILE_APPEND);
    echo "TG alerts: {$alertsResponse}\n";

    exit;
}

// При HTTP-запуске используем cURL к самому серверу
$endDay = date('Y-m-d');
$startDay = date('Y-m-d', strtotime('-3 days'));
$startDate = urlencode("{$startDay} 00:00:00");
$endDate   = urlencode("{$endDay} 23:59:59");

$selfBase = (isset($_SERVER['HTTPS']) && $_SERVER['HTTPS'] === 'on' ? 'https' : 'http')
    . '://' . ($_SERVER['HTTP_HOST'] ?? 'localhost')
    . dirname($_SERVER['SCRIPT_NAME']) . '/leads-proxy.php';

$url = "{$selfBase}?action=update_stats&token={$token}&start_date={$startDate}&end_date={$endDate}&method=reports/summary&grouping=day&field=offer_id,source,aff_sub1";

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