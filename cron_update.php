<?php
// ============================================================
// 1. Замените 'ВАШ_ТОКЕН' на реальный токен Leads.su
// 2. Замените 'https://ваш-сайт/leads-proxy.php' на реальный URL вашего leads-proxy.php
// ============================================================

$token = 'ВАШ_ТОКЕН'; // замените на свой токен
$date = date('Y-m-d', strtotime('yesterday'));

$url = "https://ваш-сайт/leads-proxy.php?action=update_stats&token={$token}&start_date={$date}&end_date={$date}&method=reports/summary&grouping=day&fields=offer_id,source";

$ch = curl_init();
curl_setopt($ch, CURLOPT_URL, $url);
curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
curl_setopt($ch, CURLOPT_TIMEOUT, 120);
$response = curl_exec($ch);
$httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
$error = curl_error($ch);
curl_close($ch);

$logMessage = date('Y-m-d H:i:s') . ' ';
if ($response === false) {
    $logMessage .= "Ошибка cURL: $error\n";
} else {
    $logMessage .= "HTTP $httpCode - $response\n";
}
file_put_contents(__DIR__ . '/cron.log', $logMessage, FILE_APPEND);
?>