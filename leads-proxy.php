<?php
/**
 * leads-proxy.php
 * Прокси-сервер для Leads.su API с кэшированием в SQLite
 * Версия: 3.0 (полностью рабочая, поддержка офферов)
 */

error_reporting(E_ALL);
ini_set('display_errors', 0); // не показываем ошибки в JSON, логируем в файл
ini_set('log_errors', 1);
ini_set('error_log', __DIR__ . '/php_errors.log');

header('Content-Type: application/json; charset=utf-8');
header('Access-Control-Allow-Origin: *');
header('Access-Control-Allow-Methods: GET, POST, OPTIONS');
header('Access-Control-Allow-Headers: Content-Type');

if ($_SERVER['REQUEST_METHOD'] === 'OPTIONS') {
    http_response_code(200);
    exit;
}

// ====================== ПАРАМЕТРЫ ======================
$method = $_GET['method'] ?? '';
$action = $_GET['action'] ?? '';
$token  = $_GET['token'] ?? '';
$start_date = $_GET['start_date'] ?? '';
$end_date   = $_GET['end_date']   ?? '';
$grouping   = $_GET['grouping']   ?? 'day';
$fields     = $_GET['field']      ?? $_GET['fields'] ?? '';
$platform_id= $_GET['platform_id']?? '';
$offset     = (int)($_GET['offset'] ?? 0);
$limit      = (int)($_GET['limit']  ?? 500);

// Yandex Metrika config
$YM_CLIENT_ID  = 'afb919b40a0d4963bd37a931829c8f34';
$YM_CLIENT_SECRET = '5311f2a8dd9543138ea4730bf25e7a06';
$YM_COUNTER_ID = '19405381';
$YM_IMPRESSION_GOAL_ID = 181; // "показ отказной заявки - новый счетчик"
// =======================================================

// ---- Подключение к SQLite (один файл stats.db) ----
$dbFile = __DIR__ . '/stats.db';
$db = null;
try {
    $db = new SQLite3($dbFile);
    $db->exec('PRAGMA journal_mode=WAL'); // улучшает производительность
    $db->exec('PRAGMA synchronous=NORMAL');
    
    // Таблица для статистики по площадкам и офферам
    $db->exec('CREATE TABLE IF NOT EXISTS daily_stats (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT NOT NULL,
        source_id TEXT,
        source_name TEXT,
        offer_name TEXT NOT NULL DEFAULT \'Unknown\',
        sub1 TEXT NOT NULL DEFAULT \'\',
        clicks INTEGER DEFAULT 0,
        conversions INTEGER DEFAULT 0,
        approved INTEGER DEFAULT 0,
        revenue REAL DEFAULT 0,
        UNIQUE(date, source_id, offer_name, sub1)
    )');
    
    // Добавляем колонку offer_name, если её нет (миграция)
    $res = $db->query("PRAGMA table_info(daily_stats)");
    $hasOffer = false;
    $hasSub1 = false;
    while ($row = $res->fetchArray(SQLITE3_ASSOC)) {
        if ($row['name'] === 'offer_name') $hasOffer = true;
        if ($row['name'] === 'sub1') $hasSub1 = true;
    }
    if (!$hasOffer) {
        $db->exec('ALTER TABLE daily_stats ADD COLUMN offer_name TEXT');
    }
    
    // Миграция: добавляем sub1 и обновляем UNIQUE constraint
    if (!$hasSub1) {
        $db->exec('BEGIN TRANSACTION');
        $db->exec('CREATE TABLE daily_stats_new (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            source_id TEXT,
            source_name TEXT,
            offer_name TEXT NOT NULL DEFAULT \'Unknown\',
            sub1 TEXT NOT NULL DEFAULT \'\',
            clicks INTEGER DEFAULT 0,
            conversions INTEGER DEFAULT 0,
            approved INTEGER DEFAULT 0,
            revenue REAL DEFAULT 0,
            UNIQUE(date, source_id, offer_name, sub1)
        )');
        $db->exec('INSERT INTO daily_stats_new (date, source_id, source_name, offer_name, sub1, clicks, conversions, approved, revenue)
            SELECT date, source_id, source_name, offer_name, \'\', clicks, conversions, approved, revenue FROM daily_stats');
        $db->exec('DROP TABLE daily_stats');
        $db->exec('ALTER TABLE daily_stats_new RENAME TO daily_stats');
        $db->exec('COMMIT');
    }
    
    // Таблица для отказов
    $db->exec('CREATE TABLE IF NOT EXISTS bounce_stats (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT NOT NULL,
        source_name TEXT NOT NULL,
        bounces INTEGER DEFAULT 0,
        UNIQUE(date, source_name)
    )');
    
    // Таблица для баннерной статистики
    $db->exec('CREATE TABLE IF NOT EXISTS banner_stats (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT NOT NULL UNIQUE,
        impressions INTEGER DEFAULT 0,
        clicks INTEGER DEFAULT 0,
        conversion_rate REAL DEFAULT 0
    )');
    
    // Таблица для настроек Яндекс.Метрики
    $db->exec('CREATE TABLE IF NOT EXISTS ym_settings (
        key TEXT PRIMARY KEY,
        value TEXT
    )');
    
} catch (Exception $e) {
    http_response_code(500);
    echo json_encode(['error' => 'Database init failed', 'message' => $e->getMessage()]);
    exit;
}

// ========== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ==========

/**
 * Выполняет GET-запрос к API Leads.su с обработкой ошибок
 */
function apiGet($url, $timeout = 30) {
    $ch = curl_init();
    curl_setopt_array($ch, [
        CURLOPT_URL => $url,
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_FOLLOWLOCATION => true,
        CURLOPT_TIMEOUT => $timeout,
        CURLOPT_SSL_VERIFYPEER => false,
        CURLOPT_HTTPHEADER => ['Accept: application/json', 'User-Agent: leads-proxy/3.0']
    ]);
    $response = curl_exec($ch);
    $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
    $error = curl_error($ch);
    curl_close($ch);
    
    if ($error) return ['error' => "CURL error: $error"];
    if ($httpCode !== 200) return ['error' => "HTTP $httpCode", 'response' => substr($response, 0, 500)];
    
    $data = json_decode($response, true);
    if (json_last_error() !== JSON_ERROR_NONE) {
        return ['error' => 'Invalid JSON response', 'raw' => substr($response, 0, 500)];
    }
    return $data;
}

/**
 * Убирает числовой ID из названия площадки: "1318157, Займон" → "Займон"
 */
function cleanSourceName($name) {
    if (!$name) return $name;
    if (preg_match('/^\s*\d+[\s,.:;\-]+(.+)$/', trim($name), $m)) {
        return trim($m[1]);
    }
    return trim($name);
}

/**
 * Загружает список офферов и возвращает массив [offer_id => offer_name]
 */
function fetchOffersMap($token) {
    $url = "https://api.leads.su/webmaster/offers?token={$token}&limit=1000";
    $result = apiGet($url);
    if (isset($result['error'])) {
        error_log('fetchOffersMap error: ' . json_encode($result['error']));
        return [];
    }
    // Handle multiple response formats
    $offers = $result['data'] ?? $result['offers'] ?? $result['items'] ?? [];
    if (!is_array($offers) && is_array($result)) {
        // Response might be a direct array
        $offers = $result;
    }
    $map = [];
    foreach ($offers as $offer) {
        if (!is_array($offer)) continue;
        $id = (string)($offer['id'] ?? $offer['offer_id'] ?? $offer['offerId'] ?? '');
        $name = $offer['name'] ?? $offer['title'] ?? '';
        if ($id && $name) $map[$id] = $name;
        elseif ($id) $map[$id] = "Offer #{$id}";
    }
    return $map;
}

/**
 * Выполняет GET-запрос к Yandex Metrika API
 */
function ymApiGet($url, $ymToken) {
    $ch = curl_init();
    curl_setopt_array($ch, [
        CURLOPT_URL => $url,
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_FOLLOWLOCATION => true,
        CURLOPT_TIMEOUT => 30,
        CURLOPT_SSL_VERIFYPEER => false,
        CURLOPT_HTTPHEADER => [
            'Authorization: OAuth ' . $ymToken,
            'Accept: application/json',
            'User-Agent: leads-proxy/3.0'
        ]
    ]);
    $response = curl_exec($ch);
    $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
    $error = curl_error($ch);
    curl_close($ch);

    if ($error) return ['error' => "CURL error: $error"];
    if ($httpCode !== 200) {
        $detail = "HTTP $httpCode";
        if ($httpCode === 403) {
            $detail = "HTTP 403 — доступ запрещён. Убедитесь, что OAuth-токен имеет права на доступ к счётчику (counter_id), и что счётчик принадлежит вашему аккаунту.";
        } elseif ($httpCode === 401) {
            $detail = "HTTP 401 — невалидный или истёкший OAuth-токен. Переподключите Яндекс.Метрику.";
        }
        return ['error' => $detail, 'response' => substr($response, 0, 500)];
    }

    $data = json_decode($response, true);
    if (json_last_error() !== JSON_ERROR_NONE) {
        return ['error' => 'Invalid JSON response', 'raw' => substr($response, 0, 500)];
    }
    return $data;
}

/**
 * Сохраняет строки отчёта в daily_stats (с учётом offer_name)
 */
function saveReportRows($db, $rows, $offerMap) {
    $inserted = 0;
    $stmt = $db->prepare('INSERT INTO daily_stats (date, source_id, source_name, offer_name, sub1, clicks, conversions, approved, revenue)
        VALUES (:date, :source_id, :source_name, :offer_name, :sub1, :clicks, :conversions, :approved, :revenue)
        ON CONFLICT(date, source_id, offer_name, sub1) DO UPDATE SET
            source_name = excluded.source_name,
            clicks = excluded.clicks,
            conversions = excluded.conversions,
            approved = excluded.approved,
            revenue = excluded.revenue');
    
    foreach ($rows as $row) {
        $date = $row['period_day'] ?? $row['period'] ?? null;
        if (!$date) continue;
        $sourceId = $row['platform_id'] ?? 'all';
        $sourceName = cleanSourceName($row['source'] ?? $row['platform_name'] ?? 'Unknown');
        $offerId = (string)($row['offer_id'] ?? $row['offerid'] ?? '');
        // Try to resolve offer name from map, then from row data, then fallback
        $offerName = '';
        if ($offerId && isset($offerMap[$offerId])) {
            $offerName = $offerMap[$offerId];
        }
        if (!$offerName) {
            $offerName = $row['offer_name'] ?? $row['offername'] ?? $row['offer'] ?? '';
        }
        if (!$offerName) {
            $offerName = $offerId ? "Offer #{$offerId}" : 'Unknown';
        }
        $sub1 = $row['aff_sub1'] ?? $row['sub1'] ?? '';
        $clicks = (int)($row['unique_clicks'] ?? $row['clicks'] ?? 0);
        $conversions = (int)($row['unique_conversions'] ?? $row['conversions'] ?? 0);
        $approved = (int)($row['conversions_approved'] ?? $row['conversionsapproved'] ?? 0);
        $revenue = (float)($row['payout'] ?? 0);
        
        $stmt->bindValue(':date', $date, SQLITE3_TEXT);
        $stmt->bindValue(':source_id', $sourceId, SQLITE3_TEXT);
        $stmt->bindValue(':source_name', $sourceName, SQLITE3_TEXT);
        $stmt->bindValue(':offer_name', $offerName, SQLITE3_TEXT);
        $stmt->bindValue(':sub1', $sub1, SQLITE3_TEXT);
        $stmt->bindValue(':clicks', $clicks, SQLITE3_INTEGER);
        $stmt->bindValue(':conversions', $conversions, SQLITE3_INTEGER);
        $stmt->bindValue(':approved', $approved, SQLITE3_INTEGER);
        $stmt->bindValue(':revenue', $revenue, SQLITE3_FLOAT);
        if ($stmt->execute()) $inserted++;
    }
    return $inserted;
}

// ========== ОБРАБОТЧИКИ ЗАПРОСОВ ==========

// 1. Получение сохранённых данных из БД (для дашборда)
if ($action === 'get_stats') {
    if (!$start_date || !$end_date) {
        echo json_encode(['error' => 'start_date and end_date required']);
        exit;
    }
    $stmt = $db->prepare('SELECT date, source_id, source_name, offer_name, sub1, clicks, conversions, approved, revenue 
        FROM daily_stats WHERE date BETWEEN :start AND :end ORDER BY date');
    $stmt->bindValue(':start', $start_date, SQLITE3_TEXT);
    $stmt->bindValue(':end', $end_date, SQLITE3_TEXT);
    $res = $stmt->execute();
    $rows = [];
    while ($row = $res->fetchArray(SQLITE3_ASSOC)) {
        $rows[] = $row;
    }
    echo json_encode(['status' => 'success', 'data' => $rows]);
    exit;
}

// 2. Сохранение статистики из интерфейса (при ручной загрузке через API)
if ($action === 'save_stats') {
    $input = json_decode(file_get_contents('php://input'), true);
    if (!$input || !isset($input['data']) || !is_array($input['data'])) {
        echo json_encode(['error' => 'Invalid data format']);
        exit;
    }
    $inserted = 0;
    foreach ($input['data'] as $row) {
        $date = $row['date'] ?? null;
        if (!$date) continue;
        $sourceId = $row['source_id'] ?? 'all';
        $sourceName = $row['source'] ?? $row['source_name'] ?? 'Unknown';
        $offerName = $row['offer'] ?? 'Unknown';
        $sub1 = $row['sub1'] ?? '';
        $clicks = (int)($row['clicks'] ?? 0);
        $conversions = (int)($row['conversions'] ?? 0);
        $approved = (int)($row['approved'] ?? 0);
        $revenue = (float)($row['revenue'] ?? 0);
        
        $stmt = $db->prepare('INSERT INTO daily_stats (date, source_id, source_name, offer_name, sub1, clicks, conversions, approved, revenue)
            VALUES (:date, :source_id, :source_name, :offer_name, :sub1, :clicks, :conversions, :approved, :revenue)
            ON CONFLICT(date, source_id, offer_name, sub1) DO UPDATE SET
                source_name = excluded.source_name,
                clicks = excluded.clicks,
                conversions = excluded.conversions,
                approved = excluded.approved,
                revenue = excluded.revenue');
        $stmt->bindValue(':date', $date, SQLITE3_TEXT);
        $stmt->bindValue(':source_id', $sourceId, SQLITE3_TEXT);
        $stmt->bindValue(':source_name', $sourceName, SQLITE3_TEXT);
        $stmt->bindValue(':offer_name', $offerName, SQLITE3_TEXT);
        $stmt->bindValue(':sub1', $sub1, SQLITE3_TEXT);
        $stmt->bindValue(':clicks', $clicks, SQLITE3_INTEGER);
        $stmt->bindValue(':conversions', $conversions, SQLITE3_INTEGER);
        $stmt->bindValue(':approved', $approved, SQLITE3_INTEGER);
        $stmt->bindValue(':revenue', $revenue, SQLITE3_FLOAT);
        if ($stmt->execute()) $inserted++;
    }
    echo json_encode(['status' => 'success', 'inserted' => $inserted]);
    exit;
}

// 3. Обновление статистики из Leads.su (для крона)
if ($action === 'update_stats') {
    if (empty($start_date) || empty($end_date)) {
        $yesterday = new DateTime('yesterday');
        $start_date = $yesterday->format('Y-m-d');
        $end_date = $yesterday->format('Y-m-d');
    }
    if (!$token) {
        echo json_encode(['error' => 'token required']);
        exit;
    }
    
    // Получаем офферы один раз
    $offerMap = fetchOffersMap($token);
    
    // Загружаем отчёт с пагинацией (чтобы не зависнуть на больших объёмах)
    $allRows = [];
    $offset = 0;
    $limit = 500;
    while (true) {
        // Build field[] parameters for Leads.su API
        $fieldQuery = '';
        if ($fields) {
            $fieldArr = array_filter(array_map('trim', explode(',', $fields)));
            foreach ($fieldArr as $f) {
                $fieldQuery .= '&field[]=' . urlencode($f);
            }
        }
        $url = "https://api.leads.su/webmaster/reports/summary?token={$token}"
             . "&start_date={$start_date}&end_date={$end_date}&grouping={$grouping}"
             . $fieldQuery
             . "&offset={$offset}&limit={$limit}";
        if ($platform_id) $url .= "&platform_id={$platform_id}";
        
        $data = apiGet($url);
        if (isset($data['error'])) {
            echo json_encode(['error' => $data['error']]);
            exit;
        }
        $rows = $data['data'] ?? [];
        $allRows = array_merge($allRows, $rows);
        if (count($rows) < $limit) break;
        $offset += $limit;
    }
    
    $saved = saveReportRows($db, $allRows, $offerMap);
    echo json_encode(['status' => 'success', 'saved' => $saved]);
    exit;
}

// 4. Получение отказов
if ($action === 'get_bounces') {
    $start = $_GET['start_date'] ?? '';
    $end = $_GET['end_date'] ?? '';
    if (!$start || !$end) {
        echo json_encode(['error' => 'start_date and end_date required']);
        exit;
    }
    $stmt = $db->prepare('SELECT date, source_name, bounces FROM bounce_stats WHERE date BETWEEN :start AND :end');
    $stmt->bindValue(':start', $start, SQLITE3_TEXT);
    $stmt->bindValue(':end', $end, SQLITE3_TEXT);
    $res = $stmt->execute();
    $rows = [];
    while ($row = $res->fetchArray(SQLITE3_ASSOC)) {
        $rows[] = $row;
    }
    echo json_encode(['status' => 'success', 'data' => $rows]);
    exit;
}

// 5. Сохранение отказов (ручной ввод)
if ($action === 'save_bounces') {
    $input = json_decode(file_get_contents('php://input'), true);
    $source = $input['source'] ?? '';
    $date = $input['date'] ?? '';
    $bounces = (int)($input['bounces'] ?? 0);
    if (!$source || !$date) {
        echo json_encode(['status' => 'error', 'message' => 'Не указаны площадка или дата']);
        exit;
    }
    $stmt = $db->prepare('INSERT INTO bounce_stats (date, source_name, bounces)
        VALUES (:date, :source_name, :bounces)
        ON CONFLICT(date, source_name) DO UPDATE SET bounces = excluded.bounces');
    $stmt->bindValue(':date', $date, SQLITE3_TEXT);
    $stmt->bindValue(':source_name', $source, SQLITE3_TEXT);
    $stmt->bindValue(':bounces', $bounces, SQLITE3_INTEGER);
    $stmt->execute();
    echo json_encode(['status' => 'success']);
    exit;
}

// 6. Получение баннерной статистики
if ($action === 'get_banner_stats') {
    $start = $_GET['start_date'] ?? '';
    $end = $_GET['end_date'] ?? '';
    if (!$start || !$end) {
        echo json_encode(['error' => 'start_date and end_date required']);
        exit;
    }
    $stmt = $db->prepare('SELECT date, impressions, clicks, conversion_rate FROM banner_stats WHERE date BETWEEN :start AND :end ORDER BY date');
    $stmt->bindValue(':start', $start, SQLITE3_TEXT);
    $stmt->bindValue(':end', $end, SQLITE3_TEXT);
    $res = $stmt->execute();
    $rows = [];
    while ($row = $res->fetchArray(SQLITE3_ASSOC)) {
        $rows[] = $row;
    }
    echo json_encode(['status' => 'success', 'data' => $rows]);
    exit;
}

// 7. Сохранение баннерной статистики (из Excel)
if ($action === 'save_banner_stats') {
    $input = json_decode(file_get_contents('php://input'), true);
    if (!$input || !isset($input['data']) || !is_array($input['data'])) {
        echo json_encode(['error' => 'Invalid data format']);
        exit;
    }
    $inserted = 0;
    foreach ($input['data'] as $row) {
        $date = $row['date'] ?? null;
        $impressions = (int)($row['impressions'] ?? 0);
        $clicks = (int)($row['clicks'] ?? 0);
        if (!$date) continue;
        $conv = $impressions > 0 ? ($clicks / $impressions * 100) : 0;
        $stmt = $db->prepare('INSERT INTO banner_stats (date, impressions, clicks, conversion_rate)
            VALUES (:date, :impressions, :clicks, :conversion_rate)
            ON CONFLICT(date) DO UPDATE SET
                impressions = excluded.impressions,
                clicks = excluded.clicks,
                conversion_rate = excluded.conversion_rate');
        $stmt->bindValue(':date', $date, SQLITE3_TEXT);
        $stmt->bindValue(':impressions', $impressions, SQLITE3_INTEGER);
        $stmt->bindValue(':clicks', $clicks, SQLITE3_INTEGER);
        $stmt->bindValue(':conversion_rate', $conv, SQLITE3_FLOAT);
        if ($stmt->execute()) $inserted++;
    }
    echo json_encode(['status' => 'success', 'inserted' => $inserted]);
    exit;
}

// 8. Yandex Metrika: получить URL авторизации
if ($action === 'ym_auth_url') {
    $url = "https://oauth.yandex.ru/authorize?response_type=token&client_id={$YM_CLIENT_ID}";
    echo json_encode(['status' => 'success', 'url' => $url]);
    exit;
}

// 9. Yandex Metrika: сохранить OAuth токен
if ($action === 'ym_save_token') {
    $input = json_decode(file_get_contents('php://input'), true);
    $ymToken = $input['token'] ?? '';
    if (!$ymToken) {
        echo json_encode(['error' => 'token required']);
        exit;
    }
    $stmt = $db->prepare('INSERT OR REPLACE INTO ym_settings (key, value) VALUES (:key, :value)');
    $stmt->bindValue(':key', 'oauth_token', SQLITE3_TEXT);
    $stmt->bindValue(':value', $ymToken, SQLITE3_TEXT);
    $stmt->execute();
    echo json_encode(['status' => 'success']);
    exit;
}

// 9b. Yandex Metrika: удалить токен
if ($action === 'ym_delete_token') {
    $db->exec("DELETE FROM ym_settings WHERE key = 'oauth_token'");
    echo json_encode(['status' => 'success']);
    exit;
}

// 10. Yandex Metrika: проверить наличие токена
if ($action === 'ym_get_token') {
    $stmt = $db->prepare('SELECT value FROM ym_settings WHERE key = :key');
    $stmt->bindValue(':key', 'oauth_token', SQLITE3_TEXT);
    $res = $stmt->execute();
    $row = $res->fetchArray(SQLITE3_ASSOC);
    echo json_encode(['status' => 'success', 'has_token' => !empty($row['value'])]);
    exit;
}

// 11. Yandex Metrika: получить список целей
if ($action === 'ym_goals') {
    $stmt = $db->prepare('SELECT value FROM ym_settings WHERE key = :key');
    $stmt->bindValue(':key', 'oauth_token', SQLITE3_TEXT);
    $res = $stmt->execute();
    $row = $res->fetchArray(SQLITE3_ASSOC);
    $ymToken = $row['value'] ?? '';
    if (!$ymToken) {
        echo json_encode(['error' => 'Yandex Metrika token not set']);
        exit;
    }
    $counterId = $YM_COUNTER_ID;
    $data = ymApiGet("https://api-metrika.yandex.net/management/v1/counter/{$counterId}/goals", $ymToken);
    if (isset($data['error'])) {
        echo json_encode(['error' => 'Failed to fetch goals', 'details' => $data['error']]);
        exit;
    }
    echo json_encode(['status' => 'success', 'goals' => $data['goals'] ?? []]);
    exit;
}

// 12. Yandex Metrika: загрузить данные баннера из целей
if ($action === 'ym_fetch_banner') {
    if (!$start_date || !$end_date) {
        echo json_encode(['error' => 'start_date and end_date required']);
        exit;
    }
    $stmt = $db->prepare('SELECT value FROM ym_settings WHERE key = :key');
    $stmt->bindValue(':key', 'oauth_token', SQLITE3_TEXT);
    $res = $stmt->execute();
    $row = $res->fetchArray(SQLITE3_ASSOC);
    $ymToken = $row['value'] ?? '';
    if (!$ymToken) {
        echo json_encode(['error' => 'Yandex Metrika token not set']);
        exit;
    }

    $counterId = $YM_COUNTER_ID;
    $impressionGoalId = $YM_IMPRESSION_GOAL_ID;

    // Найти ID цели "Клик на банер Партнера"
    $goalsData = ymApiGet("https://api-metrika.yandex.net/management/v1/counter/{$counterId}/goals", $ymToken);
    if (isset($goalsData['error'])) {
        echo json_encode(['error' => 'Failed to fetch goals', 'details' => $goalsData['error']]);
        exit;
    }

    $clickGoalId = null;
    $goals = $goalsData['goals'] ?? [];
    foreach ($goals as $goal) {
        $goalName = mb_strtolower($goal['name'] ?? '', 'UTF-8');
        if (mb_strpos($goalName, 'клик на банер') !== false || mb_strpos($goalName, 'клик на баннер') !== false) {
            $clickGoalId = $goal['id'];
            break;
        }
    }

    if (!$clickGoalId) {
        $goalsList = array_map(function($g) { return $g['id'] . ': ' . ($g['name'] ?? ''); }, $goals);
        echo json_encode(['error' => 'Goal "Клик на баннер Партнера" not found', 'available_goals' => $goalsList]);
        exit;
    }

    // Запрос данных по обеим целям
    $metrics = "ym:s:goal{$impressionGoalId}reaches,ym:s:goal{$clickGoalId}reaches";
    $apiUrl = "https://api-metrika.yandex.net/stat/v1/data"
        . "?ids={$counterId}"
        . "&metrics=" . urlencode($metrics)
        . "&dimensions=ym:s:date"
        . "&date1={$start_date}&date2={$end_date}"
        . "&sort=ym:s:date";

    $data = ymApiGet($apiUrl, $ymToken);
    if (isset($data['error'])) {
        echo json_encode(['error' => 'Failed to fetch metrika data', 'details' => $data['error']]);
        exit;
    }

    $dataRows = $data['data'] ?? [];
    $saved = 0;
    foreach ($dataRows as $dRow) {
        $date = $dRow['dimensions'][0]['name'] ?? null;
        if (!$date) continue;
        // Формат даты из Метрики может быть "YYYYMMDD" или "YYYY-MM-DD"
        if (strlen($date) === 8 && ctype_digit($date)) {
            $date = substr($date, 0, 4) . '-' . substr($date, 4, 2) . '-' . substr($date, 6, 2);
        }
        $impressions = (int)($dRow['metrics'][0] ?? 0);
        $clicks = (int)($dRow['metrics'][1] ?? 0);
        $convRate = $impressions > 0 ? ($clicks / $impressions * 100) : 0;

        $stmt = $db->prepare('INSERT INTO banner_stats (date, impressions, clicks, conversion_rate)
            VALUES (:date, :impressions, :clicks, :conv)
            ON CONFLICT(date) DO UPDATE SET
                impressions = excluded.impressions,
                clicks = excluded.clicks,
                conversion_rate = excluded.conversion_rate');
        $stmt->bindValue(':date', $date, SQLITE3_TEXT);
        $stmt->bindValue(':impressions', $impressions, SQLITE3_INTEGER);
        $stmt->bindValue(':clicks', $clicks, SQLITE3_INTEGER);
        $stmt->bindValue(':conv', $convRate, SQLITE3_FLOAT);
        if ($stmt->execute()) $saved++;
    }

    echo json_encode(['status' => 'success', 'saved' => $saved, 'click_goal_id' => $clickGoalId]);
    exit;
}

// 13. Прокси для остальных запросов (offers, platforms и т.д.)
if (!$method) {
    http_response_code(400);
    echo json_encode(['error' => 'method required']);
    exit;
}

$params = $_GET;
unset($params['method'], $params['action']);

// Handle field parameter: split comma-separated into field[] for Leads.su API
$fieldParts = '';
if (isset($params['field']) && is_string($params['field']) && strpos($params['field'], ',') !== false) {
    $fieldValues = array_filter(array_map('trim', explode(',', $params['field'])));
    unset($params['field']);
    foreach ($fieldValues as $f) {
        $fieldParts .= '&field[]=' . urlencode($f);
    }
}

$url = "https://api.leads.su/webmaster/" . $method . (empty($params) ? '' : '?' . http_build_query($params)) . $fieldParts;
$result = apiGet($url);
if (isset($result['error'])) {
    http_response_code(502);
    echo json_encode(['error' => $result['error']]);
} else {
    echo json_encode($result);
}
?>