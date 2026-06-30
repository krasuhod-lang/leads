<?php
/**
 * cron_update.php
 * Автоматическое обновление статистики из Leads.su (запуск по крону каждый час)
 * Использование: php cron_update.php
 * Cron: 0 * * * * php /path/to/cron_update.php
 */

// Leads.su token берётся внутри leads-proxy.php из LEADS_API_TOKEN
// или app_settings.leads_api_token. Не храним секрет в исходниках.

// Определяем базовый URL (протокол + хост) для вызова leads-proxy.php
$proxyUrl = (php_sapi_name() === 'cli')
    ? 'http://localhost' . dirname($_SERVER['SCRIPT_NAME'] ?? '') . '/leads-proxy.php'
    : '';

// При CLI-запуске вызываем leads-proxy.php напрямую через include
if (php_sapi_name() === 'cli') {
    // Глобальный lock: тот же файл, что и в leads-proxy.php (см.
    // leadsApiAcquireLock / leadsApiLockFile). Если параллельный процесс
    // (другой крон / тяжёлый refresh из UI) уже синхронизируется с leads.su —
    // выходим, чтобы не дублировать запросы и не словить 429.
    $lockFp = @fopen(__DIR__ . '/leads_api_lock.txt', 'c');
    if (!$lockFp || !@flock($lockFp, LOCK_EX | LOCK_NB)) {
        if ($lockFp) fclose($lockFp);
        file_put_contents(__DIR__ . '/cron.log',
            date('Y-m-d H:i:s') . " [CLI] Process locked, exiting\n", FILE_APPEND);
        echo "Process locked, exiting\n";
        exit(0);
    }
    @ftruncate($lockFp, 0);
    @fwrite($lockFp, (string)getmypid() . "\n" . date('c') . "\n");
    @fflush($lockFp);
    // Помечаем глобально: leadsApiAcquireLock() в leads-proxy.php увидит флаг
    // и не будет пытаться повторно взять flock на тот же файл из этого же
    // процесса (иначе include вернёт {"status":"locked"} и крон ничего не
    // сделает).
    $GLOBALS['LEADS_API_LOCK_HELD'] = true;
    // Lock снимется автоматически при выходе скрипта. Не закрываем $lockFp
    // вручную, чтобы flock держал блокировку до конца cron-итерации.

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

    // 2b. Кэш Я.Метрики для воронки CJM (banner_impressions_cache).
    // Используется dashboard-эндпоинтами funnel_history / conversion_dynamics —
    // отдельный кэш, чтобы скорость отрисовки воронки не зависела от живого API.
    $_GET = [];
    $_GET['action'] = 'ym_cache_banner';
    $_GET['start_date'] = $startDay;
    $_GET['end_date']   = $endDay;

    ob_start();
    include __DIR__ . '/leads-proxy.php';
    $ymCacheResponse = ob_get_clean();
    file_put_contents(__DIR__ . '/cron.log',
        date('Y-m-d H:i:s') . " [CLI] YM Cache {$startDay} → {$endDay}: {$ymCacheResponse}\n", FILE_APPEND);
    echo "YM Cache: {$ymCacheResponse}\n";

    // 2c. Ретроспективный пересчёт когорт CJM (dormant/24m).
    $_GET = [];
    $_GET['action'] = 'client_journey_recompute';

    ob_start();
    include __DIR__ . '/leads-proxy.php';
    $cjmResponse = ob_get_clean();
    file_put_contents(__DIR__ . '/cron.log',
        date('Y-m-d H:i:s') . " [CLI] CJM recompute: {$cjmResponse}\n", FILE_APPEND);
    echo "CJM recompute: {$cjmResponse}\n";

    // 3. Обновление кэша офферов с рыночными показателями (раз в час достаточно).
    $_GET = [];
    $_GET['action'] = 'fetch_offers_market';

    ob_start();
    include __DIR__ . '/leads-proxy.php';
    $offersResponse = ob_get_clean();

    $offersLogMessage = date('Y-m-d H:i:s') . " [CLI] Offers market: {$offersResponse}\n";
    file_put_contents(__DIR__ . '/cron.log', $offersLogMessage, FILE_APPEND);
    echo $offersLogMessage;

    // 4. Подтянуть журнал событий (новые офферы, остановки, изменение выплат).
    $_GET = [];
    $_GET['action'] = 'fetch_notifications';

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

    // 6. Бэктест точности AI-прогнозов. Идемпотентен — для каждой пары
    // (baseline_date, horizon) считается ровно один раз. Безопасно
    // запускать каждый час: если горизонт ещё не истёк, строка не
    // создаётся; после истечения мы получаем MAPE по revenue/clicks/EPC.
    $_GET = [];
    $_GET['action'] = 'ai_backtest_run';

    ob_start();
    include __DIR__ . '/leads-proxy.php';
    $backtestResponse = ob_get_clean();

    file_put_contents(__DIR__ . '/cron.log',
        date('Y-m-d H:i:s') . " [CLI] AI backtest: {$backtestResponse}\n", FILE_APPEND);
    echo "AI backtest: {$backtestResponse}\n";

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