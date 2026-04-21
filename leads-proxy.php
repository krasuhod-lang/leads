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
    // Самая «глубокая» актуальная модель провайдера.
    define('AI_MODEL', getenv('AI_MODEL') ?: 'deepseek-reasoner');
}
if (!defined('AI_FALLBACK_MODEL')) {
    // На случай, если reasoner недоступен — быстрый chat-поток той же линейки.
    define('AI_FALLBACK_MODEL', getenv('AI_FALLBACK_MODEL') ?: 'deepseek-chat');
}
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
        offer_id TEXT NOT NULL DEFAULT \'\',
        offer_name TEXT NOT NULL DEFAULT \'Unknown\',
        sub1 TEXT NOT NULL DEFAULT \'\',
        clicks INTEGER DEFAULT 0,
        conversions INTEGER DEFAULT 0,
        approved INTEGER DEFAULT 0,
        revenue REAL DEFAULT 0,
        UNIQUE(date, source_id, offer_name, sub1)
    )');
    
    // Добавляем колонку offer_name, offer_id если их нет (миграция)
    $res = $db->query("PRAGMA table_info(daily_stats)");
    $hasOffer = false;
    $hasOfferId = false;
    $hasSub1 = false;
    while ($row = $res->fetchArray(SQLITE3_ASSOC)) {
        if ($row['name'] === 'offer_name') $hasOffer = true;
        if ($row['name'] === 'offer_id') $hasOfferId = true;
        if ($row['name'] === 'sub1') $hasSub1 = true;
    }
    if (!$hasOffer) {
        $db->exec('ALTER TABLE daily_stats ADD COLUMN offer_name TEXT');
    }
    if (!$hasOfferId) {
        $db->exec('ALTER TABLE daily_stats ADD COLUMN offer_id TEXT NOT NULL DEFAULT \'\'');
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
 * Загружает список офферов и возвращает массив [offer_id => offer_name].
 * Также сохраняет имена в offers_cache, чтобы потом резолвить «Unknown»-записи.
 */
function fetchOffersMap($token, $db = null) {
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
    $url = "https://api.leads.su/webmaster/offers?token={$token}&limit=1000&extendedFields=1";
    $result = apiGet($url);
    if (isset($result['error'])) {
        error_log('fetchOffersExtended error: ' . json_encode($result['error']));
        return ['error' => $result['error']];
    }
    $offers = $result['data'] ?? $result['offers'] ?? $result['items'] ?? [];
    if (!is_array($offers) && is_array($result)) $offers = $result;
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
 * Сохраняет строки отчёта в daily_stats (с учётом offer_name и offer_id)
 */
function saveReportRows($db, $rows, $offerMap) {
    $inserted = 0;
    $stmt = $db->prepare('INSERT INTO daily_stats (date, source_id, source_name, offer_id, offer_name, sub1, clicks, conversions, approved, revenue)
        VALUES (:date, :source_id, :source_name, :offer_id, :offer_name, :sub1, :clicks, :conversions, :approved, :revenue)
        ON CONFLICT(date, source_id, offer_name, sub1) DO UPDATE SET
            source_name = excluded.source_name,
            offer_id = CASE WHEN excluded.offer_id != \'\' THEN excluded.offer_id ELSE daily_stats.offer_id END,
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
        $stmt->bindValue(':offer_id', $offerId, SQLITE3_TEXT);
        $stmt->bindValue(':offer_name', $offerName, SQLITE3_TEXT);
        $stmt->bindValue(':sub1', $sub1, SQLITE3_TEXT);
        $stmt->bindValue(':clicks', $clicks, SQLITE3_INTEGER);
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
        ],
    ];
}

function aiBuildSystemPrompt($sig) {
    $schemaJson = json_encode($sig['output_schema'], JSON_UNESCAPED_UNICODE | JSON_PRETTY_PRINT);
    $goals = "- " . implode("\n- ", $sig['goals']);
    return <<<PROMPT
Ты — старший бизнес-аналитик и консультант по росту выручки в performance-маркетинге (CPA, affiliate).
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
5. Прогноз стройи на основании weekly_trend / monthly_trend и текущих KPI; consciously учитывай сезонность и снижение/рост последних недель. Если данных мало — confidence=«low».
6. Никогда не упоминай свою модель, провайдера или "AI". Пиши как аналитик.
7. Язык вывода — русский, профессиональный, без воды и без «возможно стоит подумать».

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
    ];
    foreach ($caps as $k => $n) {
        if (isset($p[$k]) && is_array($p[$k]) && count($p[$k]) > $n) {
            $p[$k] = array_slice($p[$k], 0, $n);
        }
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
    $body = [
        'model' => $model,
        'messages' => [
            ['role' => 'system', 'content' => $sysPrompt],
            ['role' => 'user',   'content' => $userPrompt],
        ],
        'temperature' => 0.2,
        'max_tokens' => 3000,
        'stream' => false,
    ];
    // У провайдера есть JSON-mode — попросим, если поддерживается.
    $body['response_format'] = ['type' => 'json_object'];

    $ch = curl_init();
    curl_setopt_array($ch, [
        CURLOPT_URL => AI_API_URL,
        CURLOPT_POST => true,
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_TIMEOUT => 120,
        CURLOPT_SSL_VERIFYPEER => false,
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
    $stmt = $db->prepare('SELECT d.date, d.source_id, d.source_name, d.offer_id,
        CASE
            WHEN o.name IS NOT NULL AND o.name != \'\'
                 AND (d.offer_name = \'Unknown\' OR d.offer_name = \'\' OR d.offer_name LIKE \'Offer #%\')
            THEN o.name
            ELSE d.offer_name
        END AS offer_name,
        d.sub1, d.clicks, d.conversions, d.approved, d.revenue,
        o.market_epc, o.market_cr, o.market_ar, o.your_epc
        FROM daily_stats d
        LEFT JOIN offers_cache o ON o.offer_id = d.offer_id AND d.offer_id != \'\'
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
    $stmt = $db->prepare('INSERT INTO daily_stats (date, source_id, source_name, offer_id, offer_name, sub1, clicks, conversions, approved, revenue)
        VALUES (:date, :source_id, :source_name, :offer_id, :offer_name, :sub1, :clicks, :conversions, :approved, :revenue)
        ON CONFLICT(date, source_id, offer_name, sub1) DO UPDATE SET
            source_name = excluded.source_name,
            offer_id = CASE WHEN excluded.offer_id != \'\' THEN excluded.offer_id ELSE daily_stats.offer_id END,
            clicks = excluded.clicks,
            conversions = excluded.conversions,
            approved = excluded.approved,
            revenue = excluded.revenue');
    foreach ($input['data'] as $row) {
        $date = $row['date'] ?? null;
        if (!$date) continue;
        $sourceId = $row['source_id'] ?? 'all';
        $sourceName = cleanSourceName($row['source'] ?? $row['source_name'] ?? 'Unknown');
        $offerId = (string)($row['offer_id'] ?? '');
        $offerName = $row['offer'] ?? 'Unknown';
        $sub1 = $row['sub1'] ?? '';
        $clicks = (int)($row['clicks'] ?? 0);
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
    
    // Получаем офферы один раз и параллельно сохраняем в offers_cache
    $offerMap = fetchOffersMap($token, $db);
    
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