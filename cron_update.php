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

// При CLI-запуске выполняем каждый action leads-proxy.php в отдельном PHP-процессе.
// Важно: leads-proxy.php завершает обработчик через exit; include в одном процессе
// останавливал cron после первого update_stats, и Яндекс/прочие шаги не доходили.
if (php_sapi_name() === 'cli') {
    $lockFp = @fopen(__DIR__ . '/leads_api_lock.txt', 'c');
    if (!$lockFp || !@flock($lockFp, LOCK_EX | LOCK_NB)) {
        if ($lockFp) fclose($lockFp);
        file_put_contents(__DIR__ . '/cron.log', date('Y-m-d H:i:s') . " [CLI] Process locked, exiting\n", FILE_APPEND);
        echo "Process locked, exiting\n";
        exit(0);
    }
    @ftruncate($lockFp, 0);
    @fwrite($lockFp, (string)getmypid() . "\n" . date('c') . "\n");
    @fflush($lockFp);

    $endDay = date('Y-m-d');
    $startDay = date('Y-m-d', strtotime('-3 days'));
    $startDate = "{$startDay} 00:00:00";
    $endDate   = "{$endDay} 23:59:59";

    $runProxyAction = function (array $params, string $label, int $timeout = 900) {
        $query = http_build_query($params, '', '&', PHP_QUERY_RFC3986);
        $cmd = escapeshellcmd(PHP_BINARY) . ' ' . escapeshellarg(__DIR__ . '/leads-proxy.php') . ' ' . escapeshellarg($query);
        $descriptors = [
            1 => ['pipe', 'w'],
            2 => ['pipe', 'w'],
        ];
        $proc = @proc_open($cmd, $descriptors, $pipes, __DIR__);
        if (!is_resource($proc)) {
            $line = date('Y-m-d H:i:s') . " [CLI] {$label}: failed to start\n";
            file_put_contents(__DIR__ . '/cron.log', $line, FILE_APPEND);
            echo $line;
            return false;
        }
        foreach ($pipes as $pipe) stream_set_blocking($pipe, false);
        $out = '';
        $err = '';
        $started = time();
        while (true) {
            $status = proc_get_status($proc);
            $out .= stream_get_contents($pipes[1]);
            $err .= stream_get_contents($pipes[2]);
            if (!$status['running']) break;
            if ((time() - $started) > $timeout) {
                proc_terminate($proc);
                $err .= "\nTIMEOUT after {$timeout}s";
                break;
            }
            usleep(200000);
        }
        $out .= stream_get_contents($pipes[1]);
        $err .= stream_get_contents($pipes[2]);
        foreach ($pipes as $pipe) fclose($pipe);
        $code = proc_close($proc);
        $line = date('Y-m-d H:i:s') . " [CLI] {$label}: exit={$code}; " . trim($out ?: $err) . "\n";
        if ($err && $out) $line .= date('Y-m-d H:i:s') . " [CLI] {$label} stderr: " . trim($err) . "\n";
        file_put_contents(__DIR__ . '/cron.log', $line, FILE_APPEND);
        echo $line;
        return $code === 0;
    };

    $runProxyAction([
        'action' => 'update_stats',
        'start_date' => $startDate,
        'end_date' => $endDate,
        'method' => 'reports/summary',
        'grouping' => 'day',
        'field' => 'offer_id,source,aff_sub1',
    ], "Leads.su {$startDate} → {$endDate}");

    $runProxyAction(['action' => 'ym_fetch_banner', 'start_date' => $startDay, 'end_date' => $endDay], "YM Banner {$startDay} → {$endDay}");
    $runProxyAction(['action' => 'ym_cache_banner', 'start_date' => $startDay, 'end_date' => $endDay], "YM Cache {$startDay} → {$endDay}");
    $runProxyAction(['action' => 'client_journey_recompute'], 'CJM recompute');
    $runProxyAction(['action' => 'fetch_offers_market'], 'Offers market');
    $runProxyAction(['action' => 'fetch_notifications'], 'Notifications');
    $runProxyAction(['action' => 'tg_check_alerts'], 'TG alerts');
    $runProxyAction(['action' => 'ai_backtest_run'], 'AI backtest');

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