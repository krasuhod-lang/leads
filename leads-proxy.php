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
$fields     = $_GET['fields']     ?? '';
$platform_id= $_GET['platform_id']?? '';
$offset     = (int)($_GET['offset'] ?? 0);
$limit      = (int)($_GET['limit']  ?? 500);
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
        offer_name TEXT,
        clicks INTEGER DEFAULT 0,
        conversions INTEGER DEFAULT 0,
        approved INTEGER DEFAULT 0,
        revenue REAL DEFAULT 0,
        UNIQUE(date, source_id, offer_name)
    )');
    
    // Добавляем колонку offer_name, если её нет (миграция)
    $res = $db->query("PRAGMA table_info(daily_stats)");
    $hasOffer = false;
    while ($row = $res->fetchArray(SQLITE3_ASSOC)) {
        if ($row['name'] === 'offer_name') $hasOffer = true;
    }
    if (!$hasOffer) {
        $db->exec('ALTER TABLE daily_stats ADD COLUMN offer_name TEXT');
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
 * Загружает список офферов и возвращает массив [offer_id => offer_name]
 */
function fetchOffersMap($token) {
    $url = "https://api.leads.su/webmaster/offers?token={$token}&limit=1000";
    $result = apiGet($url);
    if (isset($result['error'])) return [];
    $offers = $result['data'] ?? [];
    $map = [];
    foreach ($offers as $offer) {
        $id = (string)($offer['id'] ?? '');
        if ($id) $map[$id] = $offer['name'] ?? "Offer #{$id}";
    }
    return $map;
}

/**
 * Сохраняет строки отчёта в daily_stats (с учётом offer_name)
 */
function saveReportRows($db, $rows, $offerMap) {
    $inserted = 0;
    $stmt = $db->prepare('INSERT INTO daily_stats (date, source_id, source_name, offer_name, clicks, conversions, approved, revenue)
        VALUES (:date, :source_id, :source_name, :offer_name, :clicks, :conversions, :approved, :revenue)
        ON CONFLICT(date, source_id, offer_name) DO UPDATE SET
            clicks = clicks + excluded.clicks,
            conversions = conversions + excluded.conversions,
            approved = approved + excluded.approved,
            revenue = revenue + excluded.revenue');
    
    foreach ($rows as $row) {
        $date = $row['period_day'] ?? $row['period'] ?? null;
        if (!$date) continue;
        $sourceId = $row['platform_id'] ?? 'all';
        $sourceName = $row['source'] ?? $row['platform_name'] ?? 'Unknown';
        $offerId = (string)($row['offer_id'] ?? $row['offerid'] ?? '');
        $offerName = $offerMap[$offerId] ?? ($offerId ? "Offer #{$offerId}" : 'Unknown');
        $clicks = (int)($row['unique_clicks'] ?? $row['clicks'] ?? 0);
        $conversions = (int)($row['unique_conversions'] ?? $row['conversions'] ?? 0);
        $approved = (int)($row['conversions_approved'] ?? $row['conversionsapproved'] ?? 0);
        $revenue = (float)($row['payout'] ?? 0);
        
        $stmt->bindValue(':date', $date, SQLITE3_TEXT);
        $stmt->bindValue(':source_id', $sourceId, SQLITE3_TEXT);
        $stmt->bindValue(':source_name', $sourceName, SQLITE3_TEXT);
        $stmt->bindValue(':offer_name', $offerName, SQLITE3_TEXT);
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
    $stmt = $db->prepare('SELECT date, source_id, source_name, offer_name, clicks, conversions, approved, revenue 
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
        $clicks = (int)($row['clicks'] ?? 0);
        $conversions = (int)($row['conversions'] ?? 0);
        $approved = (int)($row['approved'] ?? 0);
        $revenue = (float)($row['revenue'] ?? 0);
        
        $stmt = $db->prepare('INSERT INTO daily_stats (date, source_id, source_name, offer_name, clicks, conversions, approved, revenue)
            VALUES (:date, :source_id, :source_name, :offer_name, :clicks, :conversions, :approved, :revenue)
            ON CONFLICT(date, source_id, offer_name) DO UPDATE SET
                source_name = excluded.source_name,
                clicks = excluded.clicks,
                conversions = excluded.conversions,
                approved = excluded.approved,
                revenue = excluded.revenue');
        $stmt->bindValue(':date', $date, SQLITE3_TEXT);
        $stmt->bindValue(':source_id', $sourceId, SQLITE3_TEXT);
        $stmt->bindValue(':source_name', $sourceName, SQLITE3_TEXT);
        $stmt->bindValue(':offer_name', $offerName, SQLITE3_TEXT);
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
        $url = "https://api.leads.su/webmaster/reports/summary?token={$token}"
             . "&start_date={$start_date}&end_date={$end_date}&grouping={$grouping}&fields={$fields}"
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

// 8. Прокси для остальных запросов (offers, platforms и т.д.)
if (!$method) {
    http_response_code(400);
    echo json_encode(['error' => 'method required']);
    exit;
}

$params = $_GET;
unset($params['method'], $params['action']);
$url = "https://api.leads.su/webmaster/" . $method . (empty($params) ? '' : '?' . http_build_query($params));
$result = apiGet($url);
if (isset($result['error'])) {
    http_response_code(502);
    echo json_encode(['error' => $result['error']]);
} else {
    echo json_encode($result);
}
?>