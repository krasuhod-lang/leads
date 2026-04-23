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
$YM_IMPRESSION_GOAL_ID = 468033799; // Показы
$YM_CLICK_GOAL_ID     = 468033800; // Клики

// ---- AI-аналитика (внутренняя конфигурация). ----
// Ключ и название провайдера никогда не отдаются клиенту и не пишутся в UI.
// Можно переопределить переменными окружения AI_API_KEY / AI_API_URL / AI_MODEL,
// если потребуется ротация без правки кода.
if (!defined('AI_API_KEY')) {
    define('AI_API_KEY', getenv('AI_API_KEY') ?: 'sk-e3d9e424edf649858d901c2c97b91958');
}
if (!defined('AI_API_URL')) {
    define('AI_API_URL', getenv('AI_API_URL') ?: 'https://api.deepseek.com/chat/completions');
}
if (!defined('AI_MODEL')) {
    // По умолчанию — `deepseek-chat`: поддерживает response_format=json_object,
    // быстрый, отвечает в пределах 30-60 сек. `deepseek-reasoner` НЕ поддерживает
    // ни JSON-mode, ни temperature → отдаём его только как fallback.
    define('AI_MODEL', getenv('AI_MODEL') ?: 'deepseek-chat');
}
if (!defined('AI_FALLBACK_MODEL')) {
    // Резерв — reasoner той же линейки (без JSON-mode и без temperature).
    define('AI_FALLBACK_MODEL', getenv('AI_FALLBACK_MODEL') ?: 'deepseek-reasoner');
}
// =======================================================

// ---- Подключение к SQLite (один файл stats.db) ----
$dbFile = __DIR__ . '/stats.db';
$db = null;
try {
    $db = new SQLite3($dbFile);
    $db->exec('PRAGMA journal_mode=WAL'); // улучшает производительность
    $db->exec('PRAGMA synchronous=NORMAL');
    
    // Таблица для статистики по площадкам и офферам.
    // UNIQUE-ключ — (date, source_id, offer_id, sub1). offer_name теперь —
    // изменяемый атрибут; чтобы не было дублей, когда первый раз оффер пришёл
    // как «Offer #123», а второй — как «Микрозайм24», ключ строится по offer_id.
    // Когда реальный offer_id отсутствует в строке, мы синтезируем стабильный
    // суррогат `name:<md5(name)>` — см. syntheticOfferId().
    $db->exec('CREATE TABLE IF NOT EXISTS daily_stats (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT NOT NULL,
        source_id TEXT NOT NULL DEFAULT \'all\',
        source_name TEXT,
        offer_id TEXT NOT NULL DEFAULT \'\',
        offer_name TEXT NOT NULL DEFAULT \'Unknown\',
        sub1 TEXT NOT NULL DEFAULT \'\',
        clicks INTEGER DEFAULT 0,
        raw_clicks INTEGER DEFAULT 0,
        conversions INTEGER DEFAULT 0,
        approved INTEGER DEFAULT 0,
        revenue REAL DEFAULT 0,
        UNIQUE(date, source_id, offer_id, sub1)
    )');
    
    // Миграция: добавляем недостающие колонки (offer_name, offer_id, sub1, raw_clicks).
    $res = $db->query("PRAGMA table_info(daily_stats)");
    $hasOffer = false;
    $hasOfferId = false;
    $hasSub1 = false;
    $hasRawClicks = false;
    $existingCols = [];
    while ($row = $res->fetchArray(SQLITE3_ASSOC)) {
        $existingCols[$row['name']] = true;
        if ($row['name'] === 'offer_name') $hasOffer = true;
        if ($row['name'] === 'offer_id') $hasOfferId = true;
        if ($row['name'] === 'sub1') $hasSub1 = true;
        if ($row['name'] === 'raw_clicks') $hasRawClicks = true;
    }
    if (!$hasOffer) {
        $db->exec('ALTER TABLE daily_stats ADD COLUMN offer_name TEXT');
    }
    if (!$hasOfferId) {
        $db->exec('ALTER TABLE daily_stats ADD COLUMN offer_id TEXT NOT NULL DEFAULT \'\'');
    }
    if (!$hasRawClicks) {
        $db->exec('ALTER TABLE daily_stats ADD COLUMN raw_clicks INTEGER DEFAULT 0');
    }
    
    // Кэш офферов (offer_id → имя + рыночные показатели EPC).
    // Используется для (а) повторного резолва имён в "Unknown"-записях,
    // (б) хранения общерыночной статистики (detail_stats.system.other_epc и пр.).
    $db->exec('CREATE TABLE IF NOT EXISTS offers_cache (
        offer_id TEXT PRIMARY KEY,
        name TEXT NOT NULL DEFAULT \'\',
        market_epc REAL DEFAULT 0,
        market_cr REAL DEFAULT 0,
        market_ar REAL DEFAULT 0,
        your_epc REAL DEFAULT 0,
        your_cr REAL DEFAULT 0,
        updated_at TEXT
    )');
    // Миграция: добавляем колонки рыночной статистики, если их нет
    $res = $db->query("PRAGMA table_info(offers_cache)");
    $existing = [];
    while ($row = $res->fetchArray(SQLITE3_ASSOC)) { $existing[$row['name']] = true; }
    foreach (['market_epc','market_cr','market_ar','your_epc','your_cr'] as $col) {
        if (!isset($existing[$col])) $db->exec("ALTER TABLE offers_cache ADD COLUMN {$col} REAL DEFAULT 0");
    }
    if (!isset($existing['updated_at'])) $db->exec("ALTER TABLE offers_cache ADD COLUMN updated_at TEXT");
    
    // Миграция: добавляем sub1 (если его не было — это означает старую схему).
    if (!$hasSub1) {
        $db->exec('BEGIN TRANSACTION');
        $db->exec('CREATE TABLE daily_stats_new (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            source_id TEXT NOT NULL DEFAULT \'all\',
            source_name TEXT,
            offer_id TEXT NOT NULL DEFAULT \'\',
            offer_name TEXT NOT NULL DEFAULT \'Unknown\',
            sub1 TEXT NOT NULL DEFAULT \'\',
            clicks INTEGER DEFAULT 0,
            raw_clicks INTEGER DEFAULT 0,
            conversions INTEGER DEFAULT 0,
            approved INTEGER DEFAULT 0,
            revenue REAL DEFAULT 0,
            UNIQUE(date, source_id, offer_id, sub1)
        )');
        // При миграции синтезируем offer_id из имени, если он пустой,
        // чтобы не потерять строки и не схлопнуть разные офферы в один UNIQUE.
        $db->exec('INSERT OR IGNORE INTO daily_stats_new
            (date, source_id, source_name, offer_id, offer_name, sub1, clicks, raw_clicks, conversions, approved, revenue)
            SELECT date,
                   COALESCE(NULLIF(source_id, \'\'), \'all\'),
                   source_name,
                   CASE
                       WHEN offer_id IS NOT NULL AND offer_id != \'\'
                           THEN offer_id
                       ELSE \'name:\' || printf(\'%08x\', length(offer_name))
                            || substr(replace(lower(offer_name), \' \', \'_\'), 1, 24)
                   END,
                   offer_name, \'\', clicks, clicks, conversions, approved, revenue
            FROM daily_stats');
        $db->exec('DROP TABLE daily_stats');
        $db->exec('ALTER TABLE daily_stats_new RENAME TO daily_stats');
        $db->exec('COMMIT');
    } else {
        // Миграция со старого UNIQUE(date, source_id, offer_name, sub1)
        // на новый UNIQUE(date, source_id, offer_id, sub1).
        // Делаем единоразово: собираем список UNIQUE-индексов и их колонки
        // ДО любой DDL-операции — открытый PRAGMA-итератор держит блокировку
        // и в SQLite приводит к "database table is locked" при попытке
        // DROP/ALTER в той же транзакции.
        $uniqueIndexCols = [];
        $idxRes = $db->query("PRAGMA index_list(daily_stats)");
        while ($idxRow = $idxRes->fetchArray(SQLITE3_ASSOC)) {
            if ((int)$idxRow['unique'] !== 1) continue;
            $uniqueIndexCols[$idxRow['name']] = [];
        }
        $idxRes->finalize();
        foreach (array_keys($uniqueIndexCols) as $idxName) {
            $colRes = $db->query("PRAGMA index_info(" . SQLite3::escapeString($idxName) . ")");
            while ($c = $colRes->fetchArray(SQLITE3_ASSOC)) $uniqueIndexCols[$idxName][] = $c['name'];
            $colRes->finalize();
        }
        $needsKeyMigration = false;
        foreach ($uniqueIndexCols as $cols) {
            if (in_array('offer_name', $cols, true) && !in_array('offer_id', $cols, true)) {
                $needsKeyMigration = true;
                break;
            }
        }
        if ($needsKeyMigration) {
            $db->exec('BEGIN TRANSACTION');
            $db->exec('CREATE TABLE daily_stats_v2 (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                source_id TEXT NOT NULL DEFAULT \'all\',
                source_name TEXT,
                offer_id TEXT NOT NULL DEFAULT \'\',
                offer_name TEXT NOT NULL DEFAULT \'Unknown\',
                sub1 TEXT NOT NULL DEFAULT \'\',
                clicks INTEGER DEFAULT 0,
                raw_clicks INTEGER DEFAULT 0,
                conversions INTEGER DEFAULT 0,
                approved INTEGER DEFAULT 0,
                revenue REAL DEFAULT 0,
                UNIQUE(date, source_id, offer_id, sub1)
            )');
            // Синтезируем offer_id для старых строк, у которых он пустой —
            // используем стабильный 'name:' + lower(hex(md5)) от offer_name.
            // SQLite до 3.41 не имеет встроенного MD5 → делаем грубую, но
            // детерминированную свёртку: берём первые 32 символа hex от
            // последовательного OR / AND по байтам нет → используем
            // соединение из length()+первые символы.  Достаточно стабильно
            // для миграции (имя оффера почти не содержит коллизий).
            $db->exec('INSERT OR IGNORE INTO daily_stats_v2
                (date, source_id, source_name, offer_id, offer_name, sub1, clicks, raw_clicks, conversions, approved, revenue)
                SELECT date,
                       COALESCE(NULLIF(source_id, \'\'), \'all\'),
                       source_name,
                       CASE
                           WHEN offer_id IS NOT NULL AND offer_id != \'\'
                               THEN offer_id
                           ELSE \'name:\' || printf(\'%08x\', length(offer_name))
                                || substr(replace(lower(offer_name), \' \', \'_\'), 1, 24)
                       END,
                       offer_name, COALESCE(sub1, \'\'),
                       clicks,
                       -- raw_clicks для старых строк всегда 0 (DEFAULT после ALTER TABLE),
                       -- поэтому используем clicks как лучший доступный источник «сырых» кликов.
                       CASE WHEN raw_clicks IS NOT NULL AND raw_clicks > 0 THEN raw_clicks ELSE clicks END,
                       conversions, approved, revenue
                FROM daily_stats');
            $db->exec('DROP TABLE daily_stats');
            $db->exec('ALTER TABLE daily_stats_v2 RENAME TO daily_stats');
            $db->exec('COMMIT');
        }
    }
    
    // Индекс для быстрых выборок по диапазону дат (диапазонные WHERE — наш основной паттерн).
    $db->exec('CREATE INDEX IF NOT EXISTS idx_daily_stats_date ON daily_stats(date)');
    $db->exec('CREATE INDEX IF NOT EXISTS idx_daily_stats_offer_id ON daily_stats(offer_id)');
    
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
    
    // Журнал событий из /webmaster/notifications.
    // Храним только новые события (по composite ключу источник+id+date+тип),
    // чтобы при повторных опросах не плодить дубли.
    $db->exec('CREATE TABLE IF NOT EXISTS notifications_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source TEXT NOT NULL DEFAULT \'leads\',
        ext_id TEXT NOT NULL DEFAULT \'\',
        event_type TEXT NOT NULL DEFAULT \'\',
        event_date TEXT NOT NULL,
        title TEXT,
        body TEXT,
        severity TEXT DEFAULT \'info\',
        offer_id TEXT,
        payload_json TEXT,
        created_at TEXT NOT NULL,
        UNIQUE(source, ext_id, event_type, event_date)
    )');
    $db->exec('CREATE INDEX IF NOT EXISTS idx_notifications_date ON notifications_log(event_date)');
    
    // Telegram-уведомления + журнал отправок (anti-spam: один и тот же
    // alert_key за сутки шлём максимум один раз).
    $db->exec('CREATE TABLE IF NOT EXISTS tg_alerts_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        alert_key TEXT NOT NULL,
        sent_date TEXT NOT NULL,
        message TEXT,
        UNIQUE(alert_key, sent_date)
    )');
    
    // Универсальная таблица настроек (план выручки на месяц, конфиг алертов и пр.).
    // key/value — текст; для чисел сериализуем сами при чтении.
    $db->exec('CREATE TABLE IF NOT EXISTS app_settings (
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
    $trimmed = trim($name);
    // Remove leading numeric ID with separator: "1318157, Займон" → "Займон"
    if (preg_match('/^\s*\d+[\s,.:;\-]+(.+)$/', $trimmed, $m)) {
        return trim($m[1]);
    }
    // Remove trailing ID in parentheses: "Займон (1318157)" → "Займон"
    if (preg_match('/^(.+?)\s*\(\d+\)\s*$/', $trimmed, $m)) {
        return trim($m[1]);
    }
    // Remove standalone numeric-only name
    if (preg_match('/^\d+$/', $trimmed)) {
        return "Platform #{$trimmed}";
    }
    return $trimmed;
}

/**
 * Стабильный детерминированный source_id для строк, в которых API не вернул
 * platform_id. Используется и в saveReportRows (cron) и в save_stats (FE) —
 * за счёт совпадения функций строки из обоих источников схлопываются в одну
 * по UNIQUE(date, source_id, offer_id, sub1).
 *
 * Формат: 'src:' + crc32(name) (8 hex). 'all' оставляем как есть, чтобы
 * исторический агрегат «все площадки» не был перепутан с реальной площадкой.
 */
function syntheticSourceId($name) {
    $name = trim((string)$name);
    if ($name === '' || strcasecmp($name, 'all') === 0 || strcasecmp($name, 'unknown') === 0) {
        return 'all';
    }
    return 'src:' . sprintf('%08x', crc32($name));
}

/**
 * Стабильный детерминированный offer_id для строк, в которых API его не дал.
 * Возвращает 'name:' + 8 hex от crc32(name) — этого достаточно, чтобы
 * различные имена не сливались, а одно и то же имя стабильно мапилось.
 */
function syntheticOfferId($name) {
    $name = trim((string)$name);
    if ($name === '') return '';
    return 'name:' . sprintf('%08x', crc32($name));
}

/**
 * Возвращает список активных площадок [{id, name}, ...] из API
 * /webmaster/platforms (с пагинацией). Используется в update_stats,
 * чтобы крон обходил каждую площадку отдельно — иначе API схлопывает
 * клики между площадками и теряется group-by по platform_id.
 */
function fetchPlatformsList($token) {
    $all = [];
    $offset = 0;
    $limit = 500;
    for ($page = 0; $page < 20; $page++) {
        $url = "https://api.leads.su/webmaster/platforms?token={$token}&limit={$limit}&offset={$offset}";
        $res = apiGet($url);
        if (isset($res['error'])) {
            error_log('fetchPlatformsList error: ' . json_encode($res['error']));
            break;
        }
        $rows = $res['data'] ?? $res['platforms'] ?? $res['items'] ?? [];
        if (!is_array($rows) || count($rows) === 0) break;
        foreach ($rows as $p) {
            if (!is_array($p)) continue;
            $id = $p['id'] ?? $p['platform_id'] ?? null;
            if ($id === null) continue;
            $all[] = ['id' => (string)$id, 'name' => $p['name'] ?? "Platform #{$id}"];
        }
        if (count($rows) < $limit) break;
        $offset += $limit;
    }
    return $all;
}


/**
 * Загружает все офферы с постраничной выгрузкой (API limit max = 500).
 * Возвращает плоский массив объектов офферов из ответа API.
 * $extended=true → добавляет extendedFields=1 (детальная статистика рынка).
 */
function fetchOffersListAll($token, $extended = false) {
    $all = [];
    $offset = 0;
    $limit = 500; // hard API limit per docs
    $maxPages = 20; // safety net (10000 offers max)
    for ($page = 0; $page < $maxPages; $page++) {
        $url = "https://api.leads.su/webmaster/offers?token={$token}"
             . "&limit={$limit}&offset={$offset}"
             . ($extended ? '&extendedFields=1' : '');
        $result = apiGet($url);
        if (isset($result['error'])) {
            error_log('fetchOffersListAll error: ' . json_encode($result['error']));
            // Возвращаем то, что успели набрать (если первая страница упала — пустой массив).
            if (!$all) return ['error' => $result['error']];
            break;
        }
        $offers = $result['data'] ?? $result['offers'] ?? $result['items'] ?? [];
        if (!is_array($offers) && is_array($result)) $offers = $result;
        if (!is_array($offers) || count($offers) === 0) break;
        $all = array_merge($all, $offers);
        if (count($offers) < $limit) break;
        $offset += $limit;
    }
    return ['offers' => $all];
}

/**
 * Загружает список офферов и возвращает массив [offer_id => offer_name].
 * Также сохраняет имена в offers_cache, чтобы потом резолвить «Unknown»-записи.
 */
function fetchOffersMap($token, $db = null) {
    $res = fetchOffersListAll($token, false);
    if (isset($res['error'])) {
        return [];
    }
    $offers = $res['offers'];
    $map = [];
    $cacheStmt = null;
    if ($db) {
        $cacheStmt = $db->prepare('INSERT INTO offers_cache (offer_id, name, updated_at)
            VALUES (:id, :name, :ts)
            ON CONFLICT(offer_id) DO UPDATE SET
                name = CASE WHEN excluded.name != \'\' THEN excluded.name ELSE offers_cache.name END,
                updated_at = excluded.updated_at');
    }
    $now = date('Y-m-d H:i:s');
    foreach ($offers as $offer) {
        if (!is_array($offer)) continue;
        $id = (string)($offer['id'] ?? $offer['offer_id'] ?? $offer['offerId'] ?? '');
        $name = $offer['name'] ?? $offer['title'] ?? '';
        if ($id && $name) $map[$id] = $name;
        elseif ($id) $map[$id] = "Offer #{$id}";
        if ($cacheStmt && $id) {
            $cacheStmt->bindValue(':id', $id, SQLITE3_TEXT);
            $cacheStmt->bindValue(':name', $name ?: '', SQLITE3_TEXT);
            $cacheStmt->bindValue(':ts', $now, SQLITE3_TEXT);
            $cacheStmt->execute();
            $cacheStmt->reset();
        }
    }
    return $map;
}

/**
 * Загружает офферы вместе с расширенными показателями рынка (detail_stats)
 * и сохраняет в offers_cache. Возвращает массив офферов с полями market_epc/your_epc.
 */
function fetchOffersExtended($token, $db) {
    $res = fetchOffersListAll($token, true);
    if (isset($res['error'])) {
        return ['error' => $res['error']];
    }
    $offers = $res['offers'];
    $now = date('Y-m-d H:i:s');
    $stmt = $db->prepare('INSERT INTO offers_cache
        (offer_id, name, market_epc, market_cr, market_ar, your_epc, your_cr, updated_at)
        VALUES (:id, :name, :mepc, :mcr, :mar, :yepc, :ycr, :ts)
        ON CONFLICT(offer_id) DO UPDATE SET
            name = CASE WHEN excluded.name != \'\' THEN excluded.name ELSE offers_cache.name END,
            market_epc = excluded.market_epc,
            market_cr = excluded.market_cr,
            market_ar = excluded.market_ar,
            your_epc = excluded.your_epc,
            your_cr = excluded.your_cr,
            updated_at = excluded.updated_at');
    $list = [];
    foreach ($offers as $o) {
        if (!is_array($o)) continue;
        $id = (string)($o['id'] ?? $o['offer_id'] ?? '');
        if (!$id) continue;
        $name = $o['name'] ?? $o['title'] ?? '';
        $sys = $o['detail_stats']['system'] ?? [];
        $you = $o['detail_stats']['your'] ?? [];
        $mepc = (float)($sys['other_epc'] ?? 0);
        $mcr  = (float)($sys['other_cr']  ?? 0);
        $mar  = (float)($sys['other_ar']  ?? 0);
        $yepc = (float)($you['other_epc'] ?? 0);
        $ycr  = (float)($you['other_cr']  ?? 0);
        $stmt->bindValue(':id', $id, SQLITE3_TEXT);
        $stmt->bindValue(':name', $name ?: '', SQLITE3_TEXT);
        $stmt->bindValue(':mepc', $mepc, SQLITE3_FLOAT);
        $stmt->bindValue(':mcr', $mcr, SQLITE3_FLOAT);
        $stmt->bindValue(':mar', $mar, SQLITE3_FLOAT);
        $stmt->bindValue(':yepc', $yepc, SQLITE3_FLOAT);
        $stmt->bindValue(':ycr', $ycr, SQLITE3_FLOAT);
        $stmt->bindValue(':ts', $now, SQLITE3_TEXT);
        $stmt->execute();
        $stmt->reset();
        $list[] = [
            'offer_id' => $id, 'name' => $name,
            'market_epc' => $mepc, 'market_cr' => $mcr, 'market_ar' => $mar,
            'your_epc' => $yepc, 'your_cr' => $ycr
        ];
    }
    return ['offers' => $list];
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
            $detail = "HTTP 403 — доступ запрещён. Убедитесь, что OAuth-токен имеет права на доступ к счетчику (counter_id), и что счетчик принадлежит вашему аккаунту.";
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
 * Сохраняет строки отчёта в daily_stats (с учётом offer_name и offer_id).
 *
 * UPSERT-ключ — (date, source_id, offer_id, sub1). offer_name становится
 * изменяемым атрибутом: если повторно пришла та же запись, но с лучшим
 * именем (раньше было "Offer #123", теперь "Микрозайм24") — обновляем имя,
 * не плодя новые строки.
 *
 * Если в строке нет offer_id — берём syntheticOfferId(имя). Если нет
 * platform_id — syntheticSourceId(source_name). Те же функции применяются
 * в save_stats, поэтому оба пути сходятся.
 *
 * `clicks`     — unique_clicks (для UI и расчёта AR/CR).
 * `raw_clicks` — обычные clicks (для честного сравнения с рыночным EPC,
 *                который провайдер считает по сырым кликам).
 */
function saveReportRows($db, $rows, $offerMap) {
    $inserted = 0;
    $stmt = $db->prepare('INSERT INTO daily_stats
        (date, source_id, source_name, offer_id, offer_name, sub1, clicks, raw_clicks, conversions, approved, revenue)
        VALUES (:date, :source_id, :source_name, :offer_id, :offer_name, :sub1, :clicks, :raw_clicks, :conversions, :approved, :revenue)
        ON CONFLICT(date, source_id, offer_id, sub1) DO UPDATE SET
            source_name = excluded.source_name,
            offer_name = CASE
                WHEN excluded.offer_name != \'\' AND excluded.offer_name != \'Unknown\'
                     AND excluded.offer_name NOT LIKE \'Offer #%\'
                    THEN excluded.offer_name
                ELSE daily_stats.offer_name
            END,
            clicks = excluded.clicks,
            raw_clicks = excluded.raw_clicks,
            conversions = excluded.conversions,
            approved = excluded.approved,
            revenue = excluded.revenue');
    
    foreach ($rows as $row) {
        // Нормализация даты: API может вернуть "2024-01-15" или "2024-01-15 00:00:00".
        // В UNIQUE-ключе нужна именно дата без времени, иначе один и тот же день
        // схлопнется в две разные строки при разных форматах.
        $rawDate = $row['period_day'] ?? $row['period'] ?? null;
        if (!$rawDate) continue;
        $date = substr((string)$rawDate, 0, 10);
        if (!preg_match('/^\d{4}-\d{2}-\d{2}$/', $date)) continue;
        
        $sourceName = cleanSourceName($row['source'] ?? $row['platform_name'] ?? 'Unknown');
        // Сначала смотрим на явный platform_id из API, затем — синтезируем из имени.
        $rawSourceId = $row['platform_id'] ?? $row['platformid'] ?? '';
        $sourceId = ($rawSourceId !== '' && $rawSourceId !== null)
            ? (string)$rawSourceId
            : syntheticSourceId($sourceName);
        
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
        // Если реального offer_id нет — синтезируем из имени, чтобы UNIQUE
        // не превратился в (date, source_id, '', sub1) и не схлопнул разные офферы.
        if (!$offerId) {
            $offerId = syntheticOfferId($offerName);
        }
        
        $sub1 = $row['aff_sub1'] ?? $row['sub1'] ?? '';
        $clicks = (int)($row['unique_clicks'] ?? $row['clicks'] ?? 0);
        // raw clicks: сначала clicks (если API дал и unique_clicks, и clicks),
        // иначе используем то же значение (хотя бы не ноль).
        $rawClicks = (int)($row['clicks'] ?? $row['unique_clicks'] ?? 0);
        $conversions = (int)($row['unique_conversions'] ?? $row['conversions'] ?? 0);
        $approved = (int)($row['conversions_approved'] ?? $row['conversionsapproved'] ?? 0);
        $revenue = (float)($row['payout'] ?? 0);
        
        $stmt->bindValue(':date', $date, SQLITE3_TEXT);
        $stmt->bindValue(':source_id', $sourceId, SQLITE3_TEXT);
        $stmt->bindValue(':source_name', $sourceName, SQLITE3_TEXT);
        $stmt->bindValue(':offer_id', $offerId, SQLITE3_TEXT);
        $stmt->bindValue(':offer_name', $offerName, SQLITE3_TEXT);
        $stmt->bindValue(':sub1', $sub1, SQLITE3_TEXT);
        $stmt->bindValue(':clicks', $clicks, SQLITE3_INTEGER);
        $stmt->bindValue(':raw_clicks', $rawClicks, SQLITE3_INTEGER);
        $stmt->bindValue(':conversions', $conversions, SQLITE3_INTEGER);
        $stmt->bindValue(':approved', $approved, SQLITE3_INTEGER);
        $stmt->bindValue(':revenue', $revenue, SQLITE3_FLOAT);
        if ($stmt->execute()) $inserted++;
        $stmt->reset();
    }
    return $inserted;
}

// ========== AI-АНАЛИТИКА (DSPy-style) ==========
// Идея, заимствованная из DSPy: вместо "сырого" prompt-а описываем «сигнатуру»
// (типизированные input/output поля + назначения), формируем по ней system-prompt
// с JSON-схемой, просим модель сначала кратко рассуждать (chain-of-thought),
// затем отдать строго JSON. Полученный JSON валидируем; при несоответствии —
// один retry с явным указанием на проблему (Refine).

function aiBuildSignature() {
    return [
        // Описание модуля и его контракт.
        'task' => 'Глубокий бизнес-анализ статистики аффилиатной CPA-площадки с целью увеличения выручки.',
        'goals' => [
            'Дать конкретные, исполнимые рекомендации (а не общие фразы).',
            'Опираться строго на полученные цифры, явно ссылаться на них в выводах.',
            'Сделать прогноз ключевых показателей (выручка, EPC, клики, approve %, конверсия) на следующие 7 и 30 дней.',
            'Анализировать в разрезе площадок, дающих суммарно ~99% выручки (top-площадки, мелкие игнорировать).',
            'Помочь принять ключевые решения, влияющие на рост выручки.',
            'Структурировано отделить: пути конверсий и слабые точки; анализ офферов vs рынок (с прогнозом); кросс-сейл для займовой аудитории; диагностику резких просадок EPC ото дня ко дню.',
        ],
        // Схема выходного JSON. Используем её одновременно как часть промта и для валидации.
        'output_schema' => [
            'reasoning' => 'string — краткое рассуждение по данным (3-6 предложений). НЕ копировать в ответ ничего, кроме фактов.',
            'summary' => 'string — деловое резюме периода (2-4 предложения), без воды.',
            'recommendations' => 'array<{title:string, action:string, expected_impact:string, priority:"high"|"medium"|"low", evidence:string}> — 3-7 конкретных рекомендаций, каждая с действием и ожидаемым эффектом, привязкой к цифрам в evidence',
            'forecast' => '{period_7d:{revenue:number, clicks:number, epc:number, approve_rate:number, confidence:"low"|"medium"|"high", basis:string}, period_30d:{revenue:number, clicks:number, epc:number, approve_rate:number, confidence:"low"|"medium"|"high", basis:string}}',
            'platforms_breakdown' => 'array<{name:string, revenue_share_pct:number, status:"grow"|"stable"|"watch"|"risk", insight:string, action:string}> — разбор только по площадкам из top_platforms (до 10), с долей в выручке',
            'key_decisions' => 'array<{decision:string, rationale:string, kpi_impact:string}> — 2-5 ключевых управленческих решений, направленных на рост выручки',
            'risks' => 'array<string> — риски и аномалии, требующие внимания (фрод, концентрация, просадки)',
            // НОВЫЕ структурированные блоки.
            'conversion_paths' => '{funnel:{clicks:number, conversions:number, approved:number, revenue:number, cr_pct:number, approve_rate_pct:number, epc:number}, weak_points:array<{stage:"click_to_conversion"|"conversion_to_approve"|"approve_to_revenue", where:string, metric:string, value:number, benchmark:number, severity:"high"|"medium"|"low", root_cause:string, fix:string, expected_uplift:string}>, summary:string} — пути конверсии и слабые точки воронки. weak_points — 2-6 шт.',
            'offers_market_analysis' => '{offers:array<{name:string, your_epc:number, market_epc:number, delta_pct:number, verdict:"scale"|"hold"|"replace"|"test", recommendation:string, forecast_epc:number, forecast_revenue_uplift:string, confidence:"low"|"medium"|"high", evidence:string}>, watchlist:array<{name:string, reason:string}>, summary:string} — поофферный анализ vs рыночный EPC, прогноз и список офферов, на которые стоит обратить внимание для увеличения доходности трафика. offers — 5-12 шт.',
            'cross_sell' => '{audience:string, products:array<{product:string, why:string, fit_score:"low"|"medium"|"high", suggested_offer_types:array<string>, expected_epc_range:string, kpi_impact:string}>, summary:string} — закономерности и идеи кросс-сейла другим продуктам займовой аудитории (страхование, карты, рефинанс, БКИ, мед.услуги, телеком и т.п.). 3-6 продуктов.',
            'epc_drops' => '{detected:boolean, drops:array<{date:string, prev_date:string, prev_epc:number, curr_epc:number, drop_pct:number, affected_offers:array<string>, affected_platforms:array<string>, evidence_based_reasons:array<string>, recommended_replacement:{offer:string, basis:string, expected_epc:number, historical_period:string}, confidence:"low"|"medium"|"high"}>, summary:string} — диагностика резких просадок EPC ото дня ко дню. Если просадок >=20% не обнаружено — detected=false, drops=[]. Иначе для каждой просадки даётся аргументированное обоснование (на основании daily_recent / weekly_trend / sub1_anomalies / market_compare) и рекомендуется оффер для замены, опираясь на ретроданные прошлых периодов.',
        ],
    ];
}

function aiBuildSystemPrompt($sig) {
    $schemaJson = json_encode($sig['output_schema'], JSON_UNESCAPED_UNICODE | JSON_PRETTY_PRINT);
    $goals = "- " . implode("\n- ", $sig['goals']);
    return <<<PROMPT
Ты — старший бизнес-аналитик и консультант по росту выручки в performance-маркетинге (CPA, affiliate, займовая вертикаль).
Задача: {$sig['task']}

Цели вывода:
{$goals}

Жёсткие правила:
1. Отвечай ТОЛЬКО валидным JSON в одной строке/блоке (без markdown, без ```), который точно соответствует схеме ниже.
2. Все числовые значения в forecast — числа (не строки), валюта = ₽, проценты — числа в виде N (например 27.4 для 27.4%).
3. В platforms_breakdown анализируй ТОЛЬКО площадки из массива top_platforms (это уже отфильтрованные ~99% выручки). Мелкие игнорируй.
4. Каждая recommendation должна:
   - быть конкретной (что именно сделать на следующей неделе),
   - содержать evidence — цитату/значение из переданных данных (например «EPC оффера X = 1.2₽ при среднем 4.7₽»),
   - иметь expected_impact с примерной оценкой (например «+8-12% к выручке за 14 дней»).
5. Прогноз строй на основании weekly_trend / monthly_trend и текущих KPI; осознанно учитывай сезонность и снижение/рост последних недель. Если данных мало — confidence=«low».
6. Никогда не упоминай свою модель, провайдера или "AI". Пиши как аналитик.
7. Язык вывода — русский, профессиональный, без воды и без «возможно стоит подумать».

Дополнительные требования по структурированным блокам (обязательно заполни все, даже если данных мало):
A. conversion_paths — построй сквозную воронку (clicks → conversions → approved → revenue) на основании kpi и offers_top/sub1_anomalies. Выдели 2-6 слабых точек. Для каждой укажи: на какой стадии («click_to_conversion», «conversion_to_approve», «approve_to_revenue»), где конкретно (площадка/оффер/sub1), фактическое значение метрики, бенчмарк (средний по портфелю или рыночный), severity, корневую причину и конкретный fix с ожидаемым uplift.
B. offers_market_analysis — пройди по market_compare и offers_top: для каждого оффера сравни your_epc с market_epc, поставь verdict (scale/hold/replace/test), дай рекомендацию и прогноз forecast_epc на ближайший период + диапазон uplift к выручке. Сформируй watchlist — офферы, на которые стоит обратить внимание для роста доходности (большой объём + положительный delta_pct, либо растущий тренд EPC). Используй цифры из payload в evidence.
C. cross_sell — определи аудиторию (займы / микрозаймы / PDL / installment) и предложи 3-6 смежных продуктов, которые можно предлагать той же аудитории (например: страхование жизни/здоровья, дебетовые/кредитные карты, рефинансирование, БКИ, юр.помощь по долгам, телеком/онлайн-сервисы). Для каждого продукта — fit_score, типы офферов, ожидаемый диапазон EPC и эффект на KPI. Привязывай к закономерностям, видимым в данных (sub1, площадки, сезонность).
D. epc_drops — пройди по daily_recent и offers_daily_epc: найди дни, где EPC просел >=20% относительно предыдущего дня (и/или относительно скользящего среднего). Для каждой просадки укажи затронутые офферы/площадки, аргументированные причины (на основании данных: падение approve %, рост дешёвых sub1, перераспределение трафика, расхождение с market_epc и т.п.) и рекомендуемый оффер для замены — выбирая из ретроданных прошлых периодов (offers_top / market_compare с устойчиво высоким EPC) с указанием периода, на котором этот оффер показывал хороший результат. Если значимых просадок нет — detected=false, drops=[], в summary это явно укажи.

Схема выходного JSON (ключи и типы строго такие):
{$schemaJson}
PROMPT;
}

function aiBuildUserPrompt($payload, $sig) {
    $data = json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_PRETTY_PRINT);
    return "Входные данные за выбранный период (агрегированные):\n```json\n{$data}\n```\n\nВерни ответ строго по схеме. Никакого текста вне JSON.";
}

function aiTrimPayload($p) {
    // Защита от слишком больших prompt-ов: ограничиваем массивы.
    $caps = [
        'top_platforms' => 12,        // ~99% выручки, кап 10-12
        'offers_top' => 25,
        'offers_underperforming' => 15,
        'market_compare' => 25,
        'sub1_anomalies' => 15,
        'weekly_trend' => 26,         // полгода по неделям
        'monthly_trend' => 18,        // полтора года по месяцам
        'daily_recent' => 30,
        'offers_daily_epc' => 12,     // топ-12 офферов по выручке с дневным EPC
        'epc_drops_signals' => 15,    // подсказки о просадках EPC день-к-дню
    ];
    foreach ($caps as $k => $n) {
        if (isset($p[$k]) && is_array($p[$k]) && count($p[$k]) > $n) {
            $p[$k] = array_slice($p[$k], 0, $n);
        }
    }
    // У offers_daily_epc внутри есть массив series — обрежем по 30 точек на оффер.
    if (isset($p['offers_daily_epc']) && is_array($p['offers_daily_epc'])) {
        foreach ($p['offers_daily_epc'] as &$o) {
            if (isset($o['series']) && is_array($o['series']) && count($o['series']) > 30) {
                $o['series'] = array_slice($o['series'], -30);
            }
        }
        unset($o);
    }
    return $p;
}

function aiValidateOutput($obj, $sig) {
    if (!is_array($obj)) return 'Output is not a JSON object';
    foreach (['summary', 'recommendations', 'forecast', 'platforms_breakdown', 'key_decisions'] as $req) {
        if (!array_key_exists($req, $obj)) return "Missing required field: {$req}";
    }
    if (!is_array($obj['recommendations']) || !count($obj['recommendations'])) return 'recommendations must be non-empty array';
    if (!is_array($obj['forecast']) || !isset($obj['forecast']['period_7d'], $obj['forecast']['period_30d'])) return 'forecast.period_7d/period_30d required';
    if (!is_array($obj['platforms_breakdown'])) return 'platforms_breakdown must be array';
    if (!is_array($obj['key_decisions'])) return 'key_decisions must be array';
    // Новые блоки — проверяем тип, если присутствуют (мягкая валидация: пустые
    // блоки допустимы, чтобы не ронять весь анализ при дефиците данных).
    foreach (['conversion_paths', 'offers_market_analysis', 'cross_sell', 'epc_drops'] as $opt) {
        if (array_key_exists($opt, $obj) && !is_array($obj[$opt])) {
            return "{$opt} must be an object";
        }
    }
    return null;
}

function aiCallProviderStructured($sysPrompt, $userPrompt, $sig) {
    // Шаг 1 — основной запрос.
    $resp = aiCallProvider($sysPrompt, $userPrompt, AI_MODEL);
    if (isset($resp['error'])) {
        // Один fallback на более лёгкую модель той же линейки.
        $resp = aiCallProvider($sysPrompt, $userPrompt, AI_FALLBACK_MODEL);
        if (isset($resp['error'])) return $resp;
    }
    $obj = aiExtractJson($resp['content'] ?? '');
    $err = aiValidateOutput($obj, $sig);
    if ($err === null) return $obj;

    // Шаг 2 (Refine) — один retry с явной коррекцией.
    $refineUser = $userPrompt . "\n\nПредыдущий ответ был невалиден: {$err}. Верни ИСПРАВЛЕННЫЙ JSON строго по схеме, без markdown.";
    $resp2 = aiCallProvider($sysPrompt, $refineUser, AI_MODEL);
    if (isset($resp2['error'])) {
        $resp2 = aiCallProvider($sysPrompt, $refineUser, AI_FALLBACK_MODEL);
        if (isset($resp2['error'])) return $resp2;
    }
    $obj2 = aiExtractJson($resp2['content'] ?? '');
    $err2 = aiValidateOutput($obj2, $sig);
    if ($err2 === null) return $obj2;

    return ['error' => 'Validation failed: ' . $err2];
}

function aiCallProvider($sysPrompt, $userPrompt, $model) {
    // deepseek-reasoner НЕ поддерживает temperature, top_p, presence_penalty,
    // frequency_penalty, response_format и tool/function calling.
    // Передача любого из этих полей даёт HTTP 400 → "AI service unavailable".
    $isReasoner = (stripos((string)$model, 'reasoner') !== false);

    $body = [
        'model' => $model,
        'messages' => [
            ['role' => 'system', 'content' => $sysPrompt],
            ['role' => 'user',   'content' => $userPrompt],
        ],
        // Для reasoner поднимаем лимит: модель кладёт chain-of-thought в
        // отдельное поле reasoning_content, но этот текст ТОЖЕ учитывается
        // в max_tokens. На 3000 итоговый JSON часто обрывается → JSON-парс
        // падает → fallback тоже валится. С добавлением 4 структурированных
        // блоков (conversion_paths, offers_market_analysis, cross_sell, epc_drops)
        // объём ответа вырос, поэтому базовый лимит подняли до 5000,
        // а для reasoner — до 12000 с учётом скрытого reasoning_content.
        'max_tokens' => $isReasoner ? 12000 : 5000,
        'stream' => false,
    ];
    if (!$isReasoner) {
        $body['temperature'] = 0.2;
        // У провайдера есть JSON-mode — попросим, если поддерживается моделью.
        $body['response_format'] = ['type' => 'json_object'];
    }

    $ch = curl_init();
    curl_setopt_array($ch, [
        CURLOPT_URL => AI_API_URL,
        CURLOPT_POST => true,
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_TIMEOUT => $isReasoner ? 240 : 120,
        CURLOPT_SSL_VERIFYPEER => true,
        CURLOPT_SSL_VERIFYHOST => 2,
        CURLOPT_HTTPHEADER => [
            'Content-Type: application/json',
            'Accept: application/json',
            'Authorization: Bearer ' . AI_API_KEY,
        ],
        CURLOPT_POSTFIELDS => json_encode($body, JSON_UNESCAPED_UNICODE),
    ]);
    $raw = curl_exec($ch);
    $code = curl_getinfo($ch, CURLINFO_HTTP_CODE);
    $cerr = curl_error($ch);
    curl_close($ch);

    if ($cerr) {
        error_log('AI provider CURL error: ' . $cerr);
        return ['error' => 'transport'];
    }
    if ($code < 200 || $code >= 300) {
        error_log("AI provider HTTP {$code}: " . substr((string)$raw, 0, 500));
        return ['error' => "http_{$code}"];
    }
    $data = json_decode($raw, true);
    if (!is_array($data)) return ['error' => 'invalid_json'];
    // Для reasoner итоговый ответ — в content; reasoning_content игнорируем
    // (это служебная цепочка размышлений, не должна попадать в UI).
    $content = $data['choices'][0]['message']['content'] ?? '';
    if ($content === '') return ['error' => 'empty_content'];
    return ['content' => $content];
}

function aiExtractJson($text) {
    $t = trim((string)$text);
    if ($t === '') return null;
    // Срежем возможные markdown-обёртки.
    $t = preg_replace('/^```(?:json)?\s*/i', '', $t);
    $t = preg_replace('/\s*```$/', '', $t);
    $obj = json_decode($t, true);
    if (is_array($obj)) return $obj;
    // Попробуем выцепить первый {...} блок.
    if (preg_match('/\{[\s\S]*\}/', $t, $m)) {
        $obj = json_decode($m[0], true);
        if (is_array($obj)) return $obj;
    }
    return null;
}

// ========== ОБРАБОТЧИКИ ЗАПРОСОВ ==========

// 1. Получение сохранённых данных из БД (для дашборда).
// LEFT JOIN с offers_cache позволяет переопределить «Unknown» / «Offer #X» актуальным именем,
// если оно есть в кэше (резолвится по offer_id).
if ($action === 'get_stats') {
    if (!$start_date || !$end_date) {
        echo json_encode(['error' => 'start_date and end_date required']);
        exit;
    }
    // Скрываем синтетические `name:...` offer_id от клиента (отдаём пустую строку),
    // чтобы FE не пытался показать их как «номер оффера». В UNIQUE-ключе и LEFT JOIN
    // они всё равно работают.
    $stmt = $db->prepare('SELECT d.date, d.source_id, d.source_name,
        CASE WHEN d.offer_id LIKE \'name:%\' THEN \'\' ELSE d.offer_id END AS offer_id,
        CASE
            WHEN o.name IS NOT NULL AND o.name != \'\'
                 AND (d.offer_name = \'Unknown\' OR d.offer_name = \'\' OR d.offer_name LIKE \'Offer #%\')
            THEN o.name
            ELSE d.offer_name
        END AS offer_name,
        d.sub1, d.clicks, d.raw_clicks, d.conversions, d.approved, d.revenue,
        o.market_epc, o.market_cr, o.market_ar, o.your_epc
        FROM daily_stats d
        LEFT JOIN offers_cache o ON o.offer_id = d.offer_id AND d.offer_id != \'\' AND d.offer_id NOT LIKE \'name:%\'
        WHERE d.date BETWEEN :start AND :end ORDER BY d.date');
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
    $stmt = $db->prepare('INSERT INTO daily_stats
        (date, source_id, source_name, offer_id, offer_name, sub1, clicks, raw_clicks, conversions, approved, revenue)
        VALUES (:date, :source_id, :source_name, :offer_id, :offer_name, :sub1, :clicks, :raw_clicks, :conversions, :approved, :revenue)
        ON CONFLICT(date, source_id, offer_id, sub1) DO UPDATE SET
            source_name = excluded.source_name,
            offer_name = CASE
                WHEN excluded.offer_name != \'\' AND excluded.offer_name != \'Unknown\'
                     AND excluded.offer_name NOT LIKE \'Offer #%\'
                    THEN excluded.offer_name
                ELSE daily_stats.offer_name
            END,
            clicks = excluded.clicks,
            raw_clicks = excluded.raw_clicks,
            conversions = excluded.conversions,
            approved = excluded.approved,
            revenue = excluded.revenue');
    foreach ($input['data'] as $row) {
        $date = $row['date'] ?? null;
        if (!$date) continue;
        $date = substr((string)$date, 0, 10);
        if (!preg_match('/^\d{4}-\d{2}-\d{2}$/', $date)) continue;
        
        $sourceName = cleanSourceName($row['source'] ?? $row['source_name'] ?? 'Unknown');
        $rawSourceId = $row['source_id'] ?? '';
        $sourceId = ($rawSourceId !== '' && $rawSourceId !== 'all' && $rawSourceId !== null)
            ? (string)$rawSourceId
            : syntheticSourceId($sourceName);
        
        $offerId = (string)($row['offer_id'] ?? '');
        $offerName = $row['offer'] ?? 'Unknown';
        if (!$offerId) {
            $offerId = syntheticOfferId($offerName);
        }
        $sub1 = $row['sub1'] ?? '';
        $clicks = (int)($row['clicks'] ?? 0);
        $rawClicks = (int)($row['raw_clicks'] ?? $row['clicks'] ?? 0);
        $conversions = (int)($row['conversions'] ?? 0);
        $approved = (int)($row['approved'] ?? 0);
        $revenue = (float)($row['revenue'] ?? 0);
        
        $stmt->bindValue(':date', $date, SQLITE3_TEXT);
        $stmt->bindValue(':source_id', $sourceId, SQLITE3_TEXT);
        $stmt->bindValue(':source_name', $sourceName, SQLITE3_TEXT);
        $stmt->bindValue(':offer_id', $offerId, SQLITE3_TEXT);
        $stmt->bindValue(':offer_name', $offerName, SQLITE3_TEXT);
        $stmt->bindValue(':sub1', $sub1, SQLITE3_TEXT);
        $stmt->bindValue(':clicks', $clicks, SQLITE3_INTEGER);
        $stmt->bindValue(':raw_clicks', $rawClicks, SQLITE3_INTEGER);
        $stmt->bindValue(':conversions', $conversions, SQLITE3_INTEGER);
        $stmt->bindValue(':approved', $approved, SQLITE3_INTEGER);
        $stmt->bindValue(':revenue', $revenue, SQLITE3_FLOAT);
        if ($stmt->execute()) $inserted++;
        $stmt->reset();
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
    
    // Нормализация дат: API leads.su интерпретирует end_date как ИСКЛЮЧИТЕЛЬНУЮ
    // границу, если время не указано → день обрезается. Если пришли голые
    // YYYY-MM-DD — добиваем "00:00:00" / "23:59:59", чтобы сегодняшний день
    // тоже попал в ответ. Если время уже есть — оставляем как прислали.
    $apiStart = (strlen($start_date) === 10) ? "{$start_date} 00:00:00" : $start_date;
    $apiEnd   = (strlen($end_date)   === 10) ? "{$end_date} 23:59:59"   : $end_date;
    
    // Получаем офферы один раз и параллельно сохраняем в offers_cache
    $offerMap = fetchOffersMap($token, $db);
    
    // Список площадок: чтобы корректно сгруппировать /webmaster/reports/summary
    // по площадкам, нужно явно передавать platform_id (см. бэг #4: иначе клики
    // одного оффера сливаются между площадками). Если platform_id уже передан
    // в URL — работаем строго по нему. Иначе обходим список площадок,
    // как это делает фронт.
    $platforms = [];
    if ($platform_id) {
        $platforms[] = ['id' => (string)$platform_id, 'name' => ''];
    } else {
        $platforms = fetchPlatformsList($token);
        // Если получить список не удалось — работаем "одним разом" по всем
        // площадкам. Даже без разбивки данные сохранятся, просто source_id
        // у строк будет 'all'.
        if (!$platforms) $platforms = [['id' => '', 'name' => '']];
    }
    
    $fieldsParam = '';
    if ($fields) {
        $fieldArr = array_filter(array_map('trim', explode(',', $fields)));
        if ($fieldArr) {
            $fieldsParam = '&fields=' . urlencode(implode(',', $fieldArr));
        }
    }
    
    $allRows = [];
    $apiErrors = [];
    foreach ($platforms as $p) {
        $pid = $p['id'] ?? '';
        $offset = 0;
        $limit = 500;
        while (true) {
            $url = "https://api.leads.su/webmaster/reports/summary?token={$token}"
                 . "&start_date=" . urlencode($apiStart)
                 . "&end_date="   . urlencode($apiEnd)
                 . "&grouping={$grouping}"
                 . $fieldsParam
                 . "&offset={$offset}&limit={$limit}";
            if ($pid !== '') $url .= "&platform_id=" . urlencode($pid);
            
            $data = apiGet($url);
            if (isset($data['error'])) {
                $apiErrors[] = ['platform_id' => $pid, 'error' => $data['error']];
                error_log("update_stats: platform={$pid} error " . json_encode($data['error']));
                break;
            }
            $rows = $data['data'] ?? [];
            // Если API не дал platform_id внутри строки — проставим явно из URL,
            // чтобы saveReportRows записал правильный source_id и не схлопнул
            // площадки в 'all'. Имя площадки тоже добавим, если есть.
            if ($pid !== '') {
                foreach ($rows as &$r) {
                    if (!isset($r['platform_id']) || $r['platform_id'] === '' || $r['platform_id'] === null) {
                        $r['platform_id'] = $pid;
                    }
                    if ((!isset($r['source']) || $r['source'] === '') && !empty($p['name'])) {
                        $r['source'] = $p['name'];
                    }
                }
                unset($r);
            }
            $allRows = array_merge($allRows, $rows);
            if (count($rows) < $limit) break;
            $offset += $limit;
        }
    }
    
    $saved = saveReportRows($db, $allRows, $offerMap);
    echo json_encode([
        'status' => 'success',
        'saved' => $saved,
        'platforms_processed' => count($platforms),
        'errors' => $apiErrors,
    ]);
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
    $clickGoalId = $YM_CLICK_GOAL_ID;

    // Запрос данных по обеим целям (показы и клики)
    // Используем goalXvisits (целевые визиты) — соответствует данным в интерфейсе Яндекс.Метрики.
    // goalXreaches считает все достижения цели (включая повторные в одном визите),
    // а goalXvisits — количество визитов, в которых цель была достигнута хотя бы раз.
    $metrics = "ym:s:goal{$impressionGoalId}visits,ym:s:goal{$clickGoalId}visits";
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

    echo json_encode(['status' => 'success', 'saved' => $saved, 'impression_goal_id' => $impressionGoalId, 'click_goal_id' => $clickGoalId]);
    exit;
}

// 13. Загрузить расширенную статистику офферов (рыночный EPC и пр.) и положить в кэш.
if ($action === 'fetch_offers_market') {
    if (!$token) {
        echo json_encode(['error' => 'token required']);
        exit;
    }
    $res = fetchOffersExtended($token, $db);
    if (isset($res['error'])) {
        echo json_encode(['error' => 'Failed to fetch offers', 'details' => $res['error']]);
        exit;
    }
    echo json_encode(['status' => 'success', 'count' => count($res['offers']), 'offers' => $res['offers']]);
    exit;
}

// 14. Получить кэш офферов с рыночными показателями (без обращения к Leads.su).
if ($action === 'get_offers_market') {
    $res = $db->query('SELECT offer_id, name, market_epc, market_cr, market_ar, your_epc, your_cr, updated_at FROM offers_cache ORDER BY market_epc DESC');
    $rows = [];
    while ($row = $res->fetchArray(SQLITE3_ASSOC)) $rows[] = $row;
    echo json_encode(['status' => 'success', 'data' => $rows]);
    exit;
}

// ============ Журнал событий из Leads.su ============
// 17. Подтянуть свежие события (новые офферы, изменение выплат, остановки и т.п.)
//     из /webmaster/notifications и положить в notifications_log без дублей.
if ($action === 'fetch_notifications') {
    if (!$token) { echo json_encode(['error' => 'token required']); exit; }
    $offset = 0;
    $limit = 100;
    $total = 0;
    $stmt = $db->prepare('INSERT OR IGNORE INTO notifications_log
        (source, ext_id, event_type, event_date, title, body, severity, offer_id, payload_json, created_at)
        VALUES (\'leads\', :ext_id, :etype, :edate, :title, :body, :sev, :offer_id, :payload, :ts)');
    $now = date('Y-m-d H:i:s');
    for ($page = 0; $page < 30; $page++) {
        $url = "https://api.leads.su/webmaster/notifications?token={$token}&limit={$limit}&offset={$offset}";
        $data = apiGet($url);
        if (isset($data['error'])) {
            echo json_encode(['error' => $data['error']]);
            exit;
        }
        $rows = $data['data'] ?? $data['notifications'] ?? $data['items'] ?? [];
        if (!is_array($rows) || count($rows) === 0) break;
        foreach ($rows as $n) {
            if (!is_array($n)) continue;
            // Пытаемся вытащить разумные поля; реальные имена в API могут отличаться,
            // поэтому делаем устойчивую нормализацию с фоллбеками.
            $extId = (string)($n['id'] ?? $n['notification_id'] ?? $n['uuid'] ?? '');
            $type  = (string)($n['type'] ?? $n['event'] ?? $n['kind'] ?? 'event');
            $date  = (string)($n['date'] ?? $n['created_at'] ?? $n['datetime'] ?? date('Y-m-d H:i:s'));
            $title = (string)($n['title'] ?? $n['subject'] ?? $n['name'] ?? '');
            $body  = (string)($n['text'] ?? $n['body'] ?? $n['message'] ?? '');
            $sev   = (string)($n['severity'] ?? $n['level'] ?? 'info');
            $offerId = (string)($n['offer_id'] ?? '');
            // Если ext_id пустой — считаем хеш от полей, чтобы UNIQUE сработал.
            if ($extId === '') $extId = sprintf('%08x', crc32($type . '|' . $date . '|' . $title . '|' . $body));
            $stmt->bindValue(':ext_id', $extId, SQLITE3_TEXT);
            $stmt->bindValue(':etype', $type, SQLITE3_TEXT);
            $stmt->bindValue(':edate', $date, SQLITE3_TEXT);
            $stmt->bindValue(':title', $title, SQLITE3_TEXT);
            $stmt->bindValue(':body', $body, SQLITE3_TEXT);
            $stmt->bindValue(':sev', $sev, SQLITE3_TEXT);
            $stmt->bindValue(':offer_id', $offerId, SQLITE3_TEXT);
            $stmt->bindValue(':payload', json_encode($n, JSON_UNESCAPED_UNICODE), SQLITE3_TEXT);
            $stmt->bindValue(':ts', $now, SQLITE3_TEXT);
            $stmt->execute();
            if ($db->changes() > 0) $total++;
            $stmt->reset();
        }
        if (count($rows) < $limit) break;
        $offset += $limit;
    }
    echo json_encode(['status' => 'success', 'new_events' => $total]);
    exit;
}

// 18. Получить журнал событий из БД (для UI). Без вызова внешнего API.
if ($action === 'get_notifications') {
    $limitGet = max(1, min(500, (int)($_GET['limit'] ?? 100)));
    $stmt = $db->prepare('SELECT source, ext_id, event_type, event_date, title, body, severity, offer_id, created_at
        FROM notifications_log ORDER BY event_date DESC, id DESC LIMIT :lim');
    $stmt->bindValue(':lim', $limitGet, SQLITE3_INTEGER);
    $res = $stmt->execute();
    $rows = [];
    while ($row = $res->fetchArray(SQLITE3_ASSOC)) $rows[] = $row;
    echo json_encode(['status' => 'success', 'data' => $rows]);
    exit;
}

// ============ Настройки приложения (план выручки и пр.) ============
if ($action === 'get_settings') {
    $res = $db->query('SELECT key, value FROM app_settings');
    $out = [];
    while ($row = $res->fetchArray(SQLITE3_ASSOC)) $out[$row['key']] = $row['value'];
    // Маскируем чувствительные значения, оставляя признак "задано".
    $masked = $out;
    if (!empty($masked['tg_bot_token'])) $masked['tg_bot_token'] = '***SET***';
    echo json_encode(['status' => 'success', 'data' => $masked]);
    exit;
}
if ($action === 'save_settings') {
    $input = json_decode(file_get_contents('php://input'), true);
    if (!is_array($input)) { echo json_encode(['error' => 'invalid payload']); exit; }
    $stmt = $db->prepare('INSERT INTO app_settings (key, value) VALUES (:k, :v)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value');
    foreach ($input as $k => $v) {
        if (!is_string($k)) continue;
        // Не перезаписываем bot_token маркером "***SET***".
        if ($v === '***SET***') continue;
        $stmt->bindValue(':k', $k, SQLITE3_TEXT);
        $stmt->bindValue(':v', is_scalar($v) ? (string)$v : json_encode($v, JSON_UNESCAPED_UNICODE), SQLITE3_TEXT);
        $stmt->execute();
        $stmt->reset();
    }
    echo json_encode(['status' => 'success']);
    exit;
}

// ============ Telegram-уведомления ============
/** Достаёт значение настройки по ключу или возвращает $default. */
function settingGet($db, $key, $default = '') {
    $stmt = $db->prepare('SELECT value FROM app_settings WHERE key = :k');
    $stmt->bindValue(':k', $key, SQLITE3_TEXT);
    $res = $stmt->execute();
    $row = $res->fetchArray(SQLITE3_ASSOC);
    return $row ? (string)$row['value'] : $default;
}

/**
 * Отправляет одно сообщение в Telegram. anti-spam: пара (alert_key, today)
 * пишется в tg_alerts_log с UNIQUE — повторно за один день одно и то же
 * уведомление не уйдёт.
 */
function tgSend($db, $alertKey, $text) {
    $token = settingGet($db, 'tg_bot_token');
    $chat  = settingGet($db, 'tg_chat_id');
    if ($token === '' || $chat === '') return ['skipped' => 'not_configured'];
    $today = date('Y-m-d');
    $stmt = $db->prepare('INSERT OR IGNORE INTO tg_alerts_log (alert_key, sent_date, message) VALUES (:k, :d, :m)');
    $stmt->bindValue(':k', $alertKey, SQLITE3_TEXT);
    $stmt->bindValue(':d', $today, SQLITE3_TEXT);
    $stmt->bindValue(':m', mb_substr($text, 0, 4000), SQLITE3_TEXT);
    $stmt->execute();
    if ($db->changes() === 0) return ['skipped' => 'already_sent_today'];
    
    $url = "https://api.telegram.org/bot{$token}/sendMessage";
    $ch = curl_init();
    curl_setopt_array($ch, [
        CURLOPT_URL => $url,
        CURLOPT_POST => true,
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_TIMEOUT => 15,
        CURLOPT_SSL_VERIFYPEER => true,
        CURLOPT_POSTFIELDS => http_build_query([
            'chat_id' => $chat,
            'text' => $text,
            'parse_mode' => 'HTML',
            'disable_web_page_preview' => 'true',
        ]),
    ]);
    $resp = curl_exec($ch);
    $code = curl_getinfo($ch, CURLINFO_HTTP_CODE);
    curl_close($ch);
    return ['sent' => $code === 200, 'http' => $code, 'response' => substr((string)$resp, 0, 200)];
}

if ($action === 'tg_test') {
    $r = tgSend($db, 'manual_test_' . time(), '✅ Тестовое сообщение от Leads.su Dashboard. Всё работает.');
    echo json_encode(['status' => 'success', 'result' => $r]);
    exit;
}

if ($action === 'tg_check_alerts') {
    $alerts = [];
    
    // Правило 1: просадка дневной выручки относительно того же дня недели
    // прошлой недели (учитывает день-недели сезонность). Триггер — снижение >= 20%.
    // Берём последний полностью закрытый день: вчера (или, если нет данных за
    // вчера, — последний день с данными). Сравниваем с днём -7д от него.
    $maxDateRow = $db->querySingle('SELECT MAX(date) as d FROM daily_stats', true);
    $maxDate = $maxDateRow['d'] ?? null;
    if ($maxDate) {
        // Последний полный день: если max == today, берём today-1, иначе max.
        $today = date('Y-m-d');
        $refDay = ($maxDate >= $today) ? date('Y-m-d', strtotime($today . ' -1 day')) : $maxDate;
        $prevWeekDay = date('Y-m-d', strtotime($refDay . ' -7 days'));
        $stmt = $db->prepare('SELECT date, SUM(revenue) as rev, SUM(clicks) as cl FROM daily_stats WHERE date IN (:d1, :d2) GROUP BY date');
        $stmt->bindValue(':d1', $refDay, SQLITE3_TEXT);
        $stmt->bindValue(':d2', $prevWeekDay, SQLITE3_TEXT);
        $res = $stmt->execute();
        $byDate = [];
        while ($row = $res->fetchArray(SQLITE3_ASSOC)) $byDate[$row['date']] = $row;
        $cur = (float)($byDate[$refDay]['rev'] ?? 0);
        $prev = (float)($byDate[$prevWeekDay]['rev'] ?? 0);
        if ($prev > 0 && $cur > 0) {
            $delta = ($cur - $prev) / $prev;
            if ($delta <= -0.20) {
                $pct = round($delta * 100, 1);
                $msg = "⚠️ <b>Просадка выручки</b>\n"
                     . "День {$refDay}: " . number_format($cur, 0, '.', ' ') . " ₽\n"
                     . "Тот же день недели ранее ({$prevWeekDay}): " . number_format($prev, 0, '.', ' ') . " ₽\n"
                     . "Δ: <b>{$pct}%</b>";
                $alerts[] = ['key' => "rev_drop_{$refDay}", 'msg' => $msg];
            }
        }
    }
    
    // Правило 2: новый «топ-Sub1» — Sub1 с долей >= 5% выручки за последние 7 дней,
    // которого 7 днями ранее в данных не было. Помогает не пропустить новый канал.
    $end = date('Y-m-d');
    $start = date('Y-m-d', strtotime('-7 days'));
    $prevStart = date('Y-m-d', strtotime('-14 days'));
    $prevEnd = date('Y-m-d', strtotime('-8 days'));
    $totalStmt = $db->prepare("SELECT SUM(revenue) FROM daily_stats WHERE date BETWEEN :s AND :e");
    $totalStmt->bindValue(':s', $start, SQLITE3_TEXT);
    $totalStmt->bindValue(':e', $end, SQLITE3_TEXT);
    $totalRev = (float)$totalStmt->execute()->fetchArray(SQLITE3_NUM)[0];
    if ($totalRev > 0) {
        $stmt = $db->prepare('SELECT sub1, SUM(revenue) as rev FROM daily_stats
            WHERE date BETWEEN :s AND :e AND sub1 != \'\'
            GROUP BY sub1 HAVING rev > 0 ORDER BY rev DESC LIMIT 20');
        $stmt->bindValue(':s', $start, SQLITE3_TEXT);
        $stmt->bindValue(':e', $end, SQLITE3_TEXT);
        $res = $stmt->execute();
        while ($row = $res->fetchArray(SQLITE3_ASSOC)) {
            $share = $row['rev'] / $totalRev;
            if ($share < 0.05) continue;
            // Был ли этот sub1 в предыдущем 7-дневном окне?
            $prevStmt = $db->prepare('SELECT COUNT(*) as c FROM daily_stats WHERE sub1 = :s AND date BETWEEN :ps AND :pe');
            $prevStmt->bindValue(':s', $row['sub1'], SQLITE3_TEXT);
            $prevStmt->bindValue(':ps', $prevStart, SQLITE3_TEXT);
            $prevStmt->bindValue(':pe', $prevEnd, SQLITE3_TEXT);
            $prevRes = $prevStmt->execute();
            $prevRow = $prevRes->fetchArray(SQLITE3_ASSOC);
            if ((int)($prevRow['c'] ?? 0) === 0) {
                $pct = round($share * 100, 1);
                $msg = "🚀 <b>Новый топ-Sub1</b>\n"
                     . "Sub1: <code>" . htmlspecialchars($row['sub1'], ENT_QUOTES, 'UTF-8') . "</code>\n"
                     . "Доля выручки за 7 дней: <b>{$pct}%</b>\n"
                     . "Не было в предыдущем 7-дневном окне.";
                $alerts[] = ['key' => "new_sub1_" . md5($row['sub1']) . '_' . $end, 'msg' => $msg];
            }
        }
    }
    
    // Правило 3: достижение месячного плана выручки (если задан в settings.monthly_revenue_plan).
    $plan = (float)settingGet($db, 'monthly_revenue_plan', '0');
    if ($plan > 0) {
        $monthStart = date('Y-m-01');
        $monthEnd = date('Y-m-d');
        $factStmt = $db->prepare("SELECT SUM(revenue) FROM daily_stats WHERE date BETWEEN :s AND :e");
        $factStmt->bindValue(':s', $monthStart, SQLITE3_TEXT);
        $factStmt->bindValue(':e', $monthEnd, SQLITE3_TEXT);
        $factRev = (float)$factStmt->execute()->fetchArray(SQLITE3_NUM)[0];
        if ($factRev >= $plan) {
            $pct = round($factRev / $plan * 100);
            $msg = "🎯 <b>План месяца выполнен!</b>\n"
                 . "Месяц " . date('Y-m') . ": " . number_format($factRev, 0, '.', ' ') . " ₽ из "
                 . number_format($plan, 0, '.', ' ') . " ₽ ({$pct}%)";
            $alerts[] = ['key' => "plan_done_" . date('Y-m'), 'msg' => $msg];
        }
    }
    
    // Отправляем накопленные алерты (с защитой от повторов через UNIQUE).
    $sent = 0;
    $skipped = 0;
    foreach ($alerts as $a) {
        $r = tgSend($db, $a['key'], $a['msg']);
        if (!empty($r['sent'])) $sent++;
        else $skipped++;
    }
    echo json_encode(['status' => 'success', 'checked' => count($alerts), 'sent' => $sent, 'skipped' => $skipped]);
    exit;
}

// 16. AI-аналитика. Принимает агрегированный payload по POST,
// формирует структурированный (DSPy-style) промт и вызывает внешнего AI-провайдера
// серверным запросом. Ключ/название модели никогда не возвращаются клиенту.
if ($action === 'ai_analyze') {
    $raw = file_get_contents('php://input');
    $payload = json_decode($raw, true);
    if (!is_array($payload)) {
        echo json_encode(['status' => 'error', 'error' => 'Invalid payload']);
        exit;
    }
    // Жёстко ограничиваем размер payload, чтобы не раздувать prompt и стоимость.
    $payload = aiTrimPayload($payload);

    $signature = aiBuildSignature();
    $sysPrompt = aiBuildSystemPrompt($signature);
    $userPrompt = aiBuildUserPrompt($payload, $signature);

    $result = aiCallProviderStructured($sysPrompt, $userPrompt, $signature);
    if (isset($result['error'])) {
        // Никогда не возвращаем сырое имя провайдера/модели в текстах ошибок.
        echo json_encode(['status' => 'error', 'error' => 'AI service is temporarily unavailable. Try again later.']);
        exit;
    }
    echo json_encode(['status' => 'success', 'data' => $result]);
    exit;
}

// 15. Прокси для остальных запросов (offers, platforms и т.д.)
if (!$method) {
    http_response_code(400);
    echo json_encode(['error' => 'method required']);
    exit;
}

$params = $_GET;
unset($params['method'], $params['action']);

// Нормализация поля `fields` для /webmaster/reports/summary.
// API ждёт ОДИН параметр `fields=offer_id,source,aff_sub1` (через запятую),
// а не `field[]=...`. Frontend исторически шлёт `field=offer_id,source,aff_sub1`,
// поэтому принимаем оба варианта (`field`, `fields`, и массив `field[]`)
// и склеиваем их в единственный `fields=` со значениями через запятую.
$fieldValues = [];
foreach (['field', 'fields'] as $key) {
    if (!isset($params[$key])) continue;
    $val = $params[$key];
    if (is_array($val)) {
        foreach ($val as $v) {
            if (is_string($v) || is_numeric($v)) $fieldValues[] = (string)$v;
        }
    } elseif (is_string($val) || is_numeric($val)) {
        foreach (explode(',', (string)$val) as $v) $fieldValues[] = $v;
    }
    unset($params[$key]);
}
$fieldsQuery = '';
if ($fieldValues) {
    $clean = [];
    foreach ($fieldValues as $v) {
        $v = trim((string)$v);
        if ($v !== '' && !in_array($v, $clean, true)) $clean[] = $v;
    }
    if ($clean) $fieldsQuery = '&fields=' . urlencode(implode(',', $clean));
}

$url = "https://api.leads.su/webmaster/" . $method . (empty($params) ? '' : '?' . http_build_query($params)) . $fieldsQuery;
$result = apiGet($url);
if (isset($result['error'])) {
    http_response_code(502);
    echo json_encode(['error' => $result['error']]);
} else {
    echo json_encode($result);
}
?>