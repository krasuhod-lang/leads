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

// ---- Глобальная страховка от HTML-ответов вместо JSON. ----
// Любая fatal/parse ошибка PHP по умолчанию приводит к HTML-странице
// (или вовсе пустому телу + 500 от веб-сервера) → фронт ловит
// "Unexpected token '<'" в JSON.parse. Перехватываем shutdown и
// нефатальные ошибки, чтобы клиент всегда получал валидный JSON.
set_error_handler(function ($severity, $message, $file, $line) {
    // Уважаем @-операторы и текущий error_reporting.
    if (!(error_reporting() & $severity)) {
        return false;
    }
    // Только фактические ошибки превращаем в исключения; warning/notice
    // оставляем как есть, чтобы не ломать существующее поведение.
    // Примечание: настоящие E_ERROR сюда не попадают (фаталы перехватывает
    // register_shutdown_function ниже), но E_USER_ERROR и E_RECOVERABLE_ERROR
    // обработчик увидит — для них генерируем исключение.
    if ($severity === E_USER_ERROR || $severity === E_RECOVERABLE_ERROR) {
        throw new ErrorException($message, 0, $severity, $file, $line);
    }
    return false;
});

register_shutdown_function(function () {
    $err = error_get_last();
    if (!$err) return;
    $fatalTypes = [E_ERROR, E_PARSE, E_CORE_ERROR, E_COMPILE_ERROR, E_USER_ERROR];
    if (!in_array($err['type'], $fatalTypes, true)) return;
    // Если что-то уже было отдано — дописывать JSON поздно, просто логируем.
    error_log(sprintf('Fatal in leads-proxy.php: %s in %s:%d', $err['message'], $err['file'], $err['line']));
    if (headers_sent()) return;
    // Сбросим возможные ранее выставленные коды/буферы, чтобы отдать чистый JSON.
    while (ob_get_level() > 0) { @ob_end_clean(); }
    http_response_code(500);
    header('Content-Type: application/json; charset=utf-8');
    echo json_encode(['status' => 'error', 'error' => 'internal']);
});

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
// если потребуется ротация без правки кода. Если переменных окружения нет
// (типичный shared-хостинг), подхватываем локальный `ai_config.php` —
// он добавлен в .gitignore и не утечёт в публичный репозиторий.
$aiLocalConfig = __DIR__ . '/ai_config.php';
$AI_CONFIG_LOAD_STATUS = 'absent'; // диагностика для ai_diag: absent|loaded|unreadable
if (is_file($aiLocalConfig)) {
    if (is_readable($aiLocalConfig)) {
        // Файл сам объявит нужные `define(...)`. Используем require_once,
        // чтобы повторное подключение не привело к notice о редефайне.
        require_once $aiLocalConfig;
        $AI_CONFIG_LOAD_STATUS = 'loaded';
    } else {
        // Самая частая «тихая» проблема: файл лежит, но 0600 от другого
        // пользователя — apache его не прочитает. Хорошо пишем в лог.
        error_log('AI: ai_config.php exists but is NOT readable by web server. Check file permissions (chmod 644).');
        $AI_CONFIG_LOAD_STATUS = 'unreadable';
    }
}
if (!defined('AI_API_KEY')) {
    // Ключ ДОЛЖЕН задаваться переменной окружения AI_API_KEY либо
    // в локальном ai_config.php. Никаких дефолтных значений в исходниках —
    // иначе ключ утекает в публичный git.
    define('AI_API_KEY', getenv('AI_API_KEY') ?: '');
}
// Дополнительная диагностика: файл подключился, но define() вернул пустое
// значение (например, забыли вписать ключ или вписали пустую строку).
if ($AI_CONFIG_LOAD_STATUS === 'loaded' && aiSanitizeApiKey(AI_API_KEY) === '') {
    error_log('AI: ai_config.php was loaded but AI_API_KEY is empty. Check the define(\'AI_API_KEY\', \'sk-...\') line.');
}
if (!defined('AI_API_URL')) {
    define('AI_API_URL', getenv('AI_API_URL') ?: 'https://api.deepseek.com/chat/completions');
}
if (!defined('AI_MODEL')) {
    // По умолчанию — `deepseek-v4-flash`: быстрая модель DeepSeek V4,
    // отвечает в пределах 30-60 сек, поддерживает response_format=json_object
    // и temperature. Заменила legacy `deepseek-chat` (тот ещё работает как
    // алиас, но будет удалён 24 июля 2026).
    //
    // ВАЖНО: основной моделью НЕЛЬЗЯ ставить `deepseek-v4-pro`. У pro
    // включён thinking mode → ответ занимает ~150-240с, плюс refine-проход
    // (ещё ~60с). Суммарное время уходит за proxy_read_timeout вышестоящего
    // nginx/Cloudflare (обычно 100с), и клиент получает HTML 504 вместо
    // JSON → фронт показывает общую фразу «Анализ временно недоступен.
    // Попробуйте ещё раз через минуту.» (см. catch-блок runDeepAIAnalysis
    // в index.html — он подставляет fallback-текст, если в ответе нет
    // `uiReason`). Pro доступен как fallback и через AI_MODEL=...env.
    define('AI_MODEL', getenv('AI_MODEL') ?: 'deepseek-v4-flash');
}
if (!defined('AI_FALLBACK_MODEL')) {
    // Резерв — флагман `deepseek-v4-pro` (1.6T параметров, расширенное
    // рассуждение через thinking mode). Поддерживает JSON-mode/temperature,
    // но из-за thinking тратит больше токенов и времени → выше
    // max_tokens/timeout (см. aiCallProvider). Используем только как
    // escape-hatch при transport/HTTP-ошибке быстрой модели, чтобы дать
    // пользователю хоть какой-то ответ вместо «AI service unavailable».
    define('AI_FALLBACK_MODEL', getenv('AI_FALLBACK_MODEL') ?: 'deepseek-v4-pro');
}
// Версия промпта/схемы. Меняется ВРУЧНУЮ при изменении aiBuildSignature() /
// aiBuildSystemPrompt(). Включается в hash payload и сохраняется в
// `ai_analysis_cache.prompt_version` — позволяет различать кэш разных
// версий схемы и при необходимости инвалидировать его.
if (!defined('AI_PROMPT_VERSION')) {
    // 2026-05-12.v3 — структурированные recommendations (target/action_type/
    // объектный expected_impact/implementation_steps/revoke_if/evidence), блок
    // [CONTROL CONTEXT] (профиль/пороги/фильтры/KPI), allowed_entities в payload,
    // расширенная пост-валидация anomalies_detected, telemetry counters.
    define('AI_PROMPT_VERSION', '2026-05-12.v3-controllable-actionable');
}

// Закрытый список значений action_type. Используется и в схеме промпта, и в
// фильтре общих фраз: рекомендация без action_type считается «общей» и помечается
// как low-quality в UI. Список заведомо узкий — добавление нового типа требует
// смены AI_PROMPT_VERSION (иначе старый кэш не инвалидируется).
if (!defined('AI_ACTION_TYPES_JSON')) {
    define('AI_ACTION_TYPES_JSON', json_encode([
        'scale_traffic', 'reduce_traffic', 'replace_offer', 'pause_offer',
        'add_to_showcase', 'remove_from_showcase', 'change_placement',
        'change_sms_copy', 'launch_sms_campaign', 'reroute_to_offline',
        'reroute_to_app', 'block_sub1', 'investigate_sub1',
        'negotiate_payout', 'request_creative_refresh', 'monitor',
    ]));
}

// ---- Файловый лог AI-пайплайна. ----
// Отдельный от php_errors.log файл, чтобы было видно, на каком шаге
// (конфиг, сеть, HTTP-код провайдера, валидация JSON) падает генерация
// «дипсика». Формат — JSON-line на каждое событие, чтобы можно было читать
// и глазами, и парсить. API-ключ и сырые промпты в лог НЕ попадают
// (только длины/превью), чтобы файл не разрастался и не утекал секрет.
if (!defined('AI_LOG_FILE')) {
    define('AI_LOG_FILE', __DIR__ . '/ai.log');
}
if (!defined('AI_LOG_MAX_BYTES')) {
    // Авто-ротация: при достижении лимита переименовываем в ai.log.1
    // и начинаем новый файл. 2 МБ хватает на сотни прогонов и не раздувает диск.
    define('AI_LOG_MAX_BYTES', 2 * 1024 * 1024);
}

/**
 * Пишет одну запись в ai.log (JSON-line) и дублирует в системный error_log.
 * $event — короткое имя события (config_loaded, ai_diag_start, provider_http, ...).
 * $context — произвольный массив с деталями (без секретов).
 */
function aiLog($event, array $context = []) {
    $line = [
        'ts'    => date('c'),
        'event' => (string)$event,
        'pid'   => getmypid(),
    ];
    // Никогда не пишем ключ целиком — только префикс и длину, если он вдруг
    // попал в $context (защита от случайного логирования).
    if (isset($context['api_key'])) {
        $k = (string)$context['api_key'];
        $context['api_key'] = ($k === '') ? '' : substr($k, 0, 4) . '…(' . strlen($k) . ')';
    }
    foreach ($context as $k => $v) {
        $line[$k] = $v;
    }
    $json = json_encode($line, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
    if ($json === false) {
        $json = json_encode(['ts' => date('c'), 'event' => $event, 'json_error' => json_last_error_msg()]);
    }
    // Простейшая ротация по размеру.
    if (is_file(AI_LOG_FILE) && @filesize(AI_LOG_FILE) > AI_LOG_MAX_BYTES) {
        @rename(AI_LOG_FILE, AI_LOG_FILE . '.1');
    }
    @file_put_contents(AI_LOG_FILE, $json . "\n", FILE_APPEND | LOCK_EX);
    // Дублируем в php_errors.log только важные/ошибочные события, иначе
    // успешные прогоны быстро забивают системный лог хостинга.
    static $importantEvents = [
        'config_loaded'                 => true, // первый запуск после деплоя — полезно видеть
        'ai_diag_no_key'                => true,
        'ai_analyze_no_key'             => true,
        'ai_analyze_bad_payload'        => true,
        'ai_analyze_error'              => true,
        'provider_curl_error'           => true,
        'provider_http_error'           => true,
        'provider_invalid_json'         => true,
        'provider_empty_content'        => true,
        'structured_validation_failed'  => true,
        'structured_fallback'           => true,
    ];
    if (isset($importantEvents[$event])) {
        error_log('AI[' . $event . ']: ' . $json);
    }
}

// Сразу зафиксируем итог загрузки конфигурации — это первая точка,
// на которой может «тихо» сломаться весь AI-пайплайн.
aiLog('config_loaded', [
    'config_load_status'   => $AI_CONFIG_LOAD_STATUS,
    'config_file_present'  => is_file($aiLocalConfig),
    'config_file_readable' => is_readable($aiLocalConfig),
    'api_key_present'      => (AI_API_KEY !== ''),
    'api_key_length'       => strlen(AI_API_KEY),
    'env_key_set'          => (getenv('AI_API_KEY') !== false && getenv('AI_API_KEY') !== ''),
    'model'                => AI_MODEL,
    'fallback_model'       => AI_FALLBACK_MODEL,
    'api_url'              => AI_API_URL,
]);

/**
 * Возвращает действующий API-ключ DeepSeek с учётом всех источников.
 *
 * Приоритет:
 *   1) AI_API_KEY из переменной окружения или ai_config.php (если задан);
 *   2) ключ, введённый пользователем через интерфейс и сохранённый в
 *      app_settings (key='ai_api_key'). Это нужно для shared-хостинга, где
 *      нельзя задать env-переменные и неудобно править ai_config.php.
 *
 * Ключ хранится только на сервере (SQLite), наружу никогда не отдаётся
 * целиком (см. get_settings — он маскируется как '***SET***').
 *
 * Результат кэшируется в статической переменной на время запроса, чтобы не
 * дёргать БД при каждом вызове (provider request + diag + guard).
 */
/**
 * Чистит API-ключ перед использованием в заголовке Authorization.
 *
 * trim() убирает пробелы/переводы строки только по краям, но при копировании
 * ключа из мессенджеров/PDF/таблиц в него часто попадают НЕВИДИМЫЕ символы,
 * которые trim() не трогает: неразрывный пробел (U+00A0), zero-width space
 * (U+200B), внутренние табы/переводы строки. Любой такой символ делает
 * заголовок Authorization невалидным, и DeepSeek отвечает 401.
 *
 * Валидный ключ DeepSeek состоит только из печатаемых ASCII-символов
 * (`sk-`, латиница, цифры, дефис), поэтому безопасно выбросить всё, что не
 * попадает в диапазон 0x21–0x7E (печатаемый ASCII без пробела).
 */
function aiSanitizeApiKey($key) {
    return preg_replace('/[^\x21-\x7E]/', '', (string)$key);
}

function aiEffectiveApiKey() {
    static $cached = null;
    if ($cached !== null) {
        return $cached;
    }
    // Чистим ключ независимо от источника: при копировании в ai_config.php или
    // env часто попадают пробелы/перевод строки и невидимые символы, из-за
    // которых заголовок Authorization становится невалидным и провайдер
    // отвечает 401 (см. aiSanitizeApiKey).
    if (defined('AI_API_KEY') && aiSanitizeApiKey(AI_API_KEY) !== '') {
        $cached = aiSanitizeApiKey(AI_API_KEY);
        return $cached;
    }
    global $db;
    if ($db instanceof SQLite3) {
        $cached = aiSanitizeApiKey(settingGet($db, 'ai_api_key', ''));
        return $cached;
    }
    $cached = '';
    return $cached;
}

/** Откуда взят действующий ключ: 'config'|'db'|'none' (для диагностики). */
function aiApiKeySource() {
    if (defined('AI_API_KEY') && aiSanitizeApiKey(AI_API_KEY) !== '') {
        return 'config';
    }
    return aiEffectiveApiKey() !== '' ? 'db' : 'none';
}
// =======================================================

// ---- Подключение к SQLite (один файл stats.db) ----
$dbFile = __DIR__ . '/stats.db';
$db = null;
try {
    $db = new SQLite3($dbFile);
    // КРИТИЧНО: busy_timeout ДО любого exec/query. Без него параллельные
    // запросы (cron + UI + AI) сразу падают с "database is locked" → fatal.
    // 5 секунд достаточно, чтобы конкурирующая транзакция завершилась.
    // Делаем это и через PHP API (надёжнее, не зависит от парсера PRAGMA),
    // и через PRAGMA — на случай если версия PHP не поддерживает метод.
    if (method_exists($db, 'busyTimeout')) {
        @$db->busyTimeout(5000);
    }
    @$db->exec('PRAGMA busy_timeout=5000');
    // Маленький helper: безопасный exec с одной повторной попыткой,
    // чтобы PRAGMA/CREATE/ALTER не валили весь запрос из-за короткой блокировки.
    $safeExec = function ($sql) use ($db) {
        for ($i = 0; $i < 3; $i++) {
            if (@$db->exec($sql)) return true;
            $msg = $db->lastErrorMsg();
            if (stripos($msg, 'locked') === false && stripos($msg, 'busy') === false) {
                error_log("SQLite exec failed: {$msg} | SQL: " . substr($sql, 0, 120));
                return false;
            }
            usleep(200000); // 200 ms
        }
        error_log("SQLite exec gave up after retries: " . substr($sql, 0, 120));
        return false;
    };
    $safeExec('PRAGMA journal_mode=WAL'); // улучшает производительность
    $safeExec('PRAGMA synchronous=NORMAL');
    
    // Таблица для статистики по площадкам и офферам.
    // UNIQUE-ключ — (date, source_id, offer_id, sub1). offer_name теперь —
    // изменяемый атрибут; чтобы не было дублей, когда первый раз оффер пришёл
    // как «Offer #123», а второй — как «Микрозайм24», ключ строится по offer_id.
    // Когда реальный offer_id отсутствует в строке, мы синтезируем стабильный
    // суррогат `name:<md5(name)>` — см. syntheticOfferId().
    $safeExec('CREATE TABLE IF NOT EXISTS daily_stats (
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
    // ВАЖНО: query() может вернуть false при кратковременной блокировке БД —
    // тогда вызов fetchArray() на bool даст fatal. Защищаемся явной проверкой.
    $res = @$db->query("PRAGMA table_info(daily_stats)");
    $hasOffer = false;
    $hasOfferId = false;
    $hasSub1 = false;
    $hasRawClicks = false;
    $existingCols = [];
    if ($res instanceof SQLite3Result) {
        while ($row = $res->fetchArray(SQLITE3_ASSOC)) {
            $existingCols[$row['name']] = true;
            if ($row['name'] === 'offer_name') $hasOffer = true;
            if ($row['name'] === 'offer_id') $hasOfferId = true;
            if ($row['name'] === 'sub1') $hasSub1 = true;
            if ($row['name'] === 'raw_clicks') $hasRawClicks = true;
        }
    } else {
        // PRAGMA не выполнился — пропускаем миграцию, чтобы не делать ALTER
        // вслепую. Колонки уже могут существовать; следующий запрос их создаст
        // или найдёт. Логируем для отладки.
        error_log('SQLite: PRAGMA table_info(daily_stats) failed (likely locked): ' . $db->lastErrorMsg());
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
    $res = @$db->query("PRAGMA table_info(offers_cache)");
    $existing = [];
    if ($res instanceof SQLite3Result) {
        while ($row = $res->fetchArray(SQLITE3_ASSOC)) { $existing[$row['name']] = true; }
    } else {
        error_log('SQLite: PRAGMA table_info(offers_cache) failed (likely locked): ' . $db->lastErrorMsg());
    }
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
        $idxRes = @$db->query("PRAGMA index_list(daily_stats)");
        if ($idxRes instanceof SQLite3Result) {
            while ($idxRow = $idxRes->fetchArray(SQLITE3_ASSOC)) {
                if ((int)$idxRow['unique'] !== 1) continue;
                $uniqueIndexCols[$idxRow['name']] = [];
            }
            $idxRes->finalize();
        } else {
            error_log('SQLite: PRAGMA index_list failed (likely locked): ' . $db->lastErrorMsg());
        }
        foreach (array_keys($uniqueIndexCols) as $idxName) {
            $colRes = @$db->query("PRAGMA index_info(" . SQLite3::escapeString($idxName) . ")");
            if ($colRes instanceof SQLite3Result) {
                while ($c = $colRes->fetchArray(SQLITE3_ASSOC)) $uniqueIndexCols[$idxName][] = $c['name'];
                $colRes->finalize();
            }
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

    // Кэш результатов глубокого AI-анализа. Ключ — диапазон дат `from|to`,
    // чтобы при перезагрузке страницы пользователь видел тот же отчёт без
    // повторного дорогого запроса к провайдеру. Принудительный пересчёт —
    // через action=ai_analyze&force=1 (кнопка «Обновить» в UI).
    //
    // Дополнительно храним:
    //   payload_hash      — sha1 канонического payload-а; при изменении
    //                       исходных данных (после cron_update.php) старый
    //                       ответ помечается как stale (но всё ещё показывается).
    //   model_used / latency_ms / *_tokens / prompt_version — телеметрия
    //                       для блока ai_diag (стоимость, скорость, версия).
    $safeExec('CREATE TABLE IF NOT EXISTS ai_analysis_cache (
        period_key TEXT PRIMARY KEY,
        period_from TEXT NOT NULL,
        period_to TEXT NOT NULL,
        result_json TEXT NOT NULL,
        created_at INTEGER NOT NULL
    )');
    // Бэкфилл колонок (если таблица существовала до миграции).
    $aiCacheCols = [];
    $resCols = @$db->query("PRAGMA table_info(ai_analysis_cache)");
    if ($resCols) {
        while ($row = $resCols->fetchArray(SQLITE3_ASSOC)) {
            if (!empty($row['name'])) $aiCacheCols[] = $row['name'];
        }
    }
    foreach ([
        'payload_hash'      => 'TEXT',
        'prompt_version'    => 'TEXT',
        'model_used'        => 'TEXT',
        'latency_ms'        => 'INTEGER',
        'prompt_tokens'     => 'INTEGER',
        'completion_tokens' => 'INTEGER',
        'total_tokens'      => 'INTEGER',
        // Quality counters (точность/качество ответа).
        'refine_rounds'             => 'INTEGER',
        'hallu_offer_count'         => 'INTEGER',
        'numeric_check_failures'    => 'INTEGER',
        'empty_evidence_count'      => 'INTEGER',
        'low_quality_recs'          => 'INTEGER',
        'duplicate_recs_dropped'    => 'INTEGER',
        'filter_drops'              => 'INTEGER',
        'anomaly_coverage_missed'   => 'INTEGER',
    ] as $colName => $colType) {
        if (!in_array($colName, $aiCacheCols, true)) {
            @$db->exec("ALTER TABLE ai_analysis_cache ADD COLUMN {$colName} {$colType}");
        }
    }

    // Таблица backtest-аккуратности AI-прогнозов (точка роста №5c).
    // На каждую успешную AI-сессию мы знаем forecast.period_7d и period_30d.
    // Когда соответствующий период истёк, сравниваем прогноз с фактом из
    // daily_stats и считаем MAPE. Триггер — action=ai_backtest_run из cron.
    $safeExec('CREATE TABLE IF NOT EXISTS ai_forecast_accuracy (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        baseline_date TEXT NOT NULL,         -- дата, на которую был построен прогноз (period_to AI-сессии)
        horizon TEXT NOT NULL,               -- "7d" | "30d"
        target_from TEXT NOT NULL,           -- начало периода прогноза (baseline_date+1)
        target_to TEXT NOT NULL,             -- конец периода прогноза
        forecast_revenue REAL,
        forecast_clicks REAL,
        forecast_epc REAL,
        actual_revenue REAL,
        actual_clicks REAL,
        actual_epc REAL,
        mape_revenue REAL,                   -- |forecast - actual| / actual * 100
        mape_clicks REAL,
        mape_epc REAL,
        prompt_version TEXT,
        model_used TEXT,
        created_at INTEGER NOT NULL,
        UNIQUE(baseline_date, horizon)
    )');
    $safeExec('CREATE INDEX IF NOT EXISTS idx_ai_forecast_baseline ON ai_forecast_accuracy(baseline_date)');
    
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
        // Справочник офферов с extendedFields=1 на больших аккаунтах легко
        // отвечает дольше дефолтных 30 с — поднимаем до 60 с, иначе пагинация
        // обрывается на середине и в кэше остаются неполные данные
        // (см. лог "fetchOffersListAll … timed out after 30001 ms").
        $result = apiGet($url, 60);
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
        'task' => 'Глубокий бизнес-анализ статистики аффилиатной CPA-площадки в займовой/МФО-вертикали с фокусом на домонетизацию ОТКАЗНОГО трафика по каналам (витрина в ЛК / SMS / оффлайн / мобильное приложение).',
        'goals' => [
            'Дать конкретные, исполнимые рекомендации (а не общие фразы).',
            'Опираться строго на полученные цифры, явно ссылаться на них в выводах.',
            'Сделать прогноз ключевых показателей (выручка, EPC, клики, approve %, конверсия) на следующие 7 и 30 дней.',
            'Анализировать в разрезе площадок, дающих суммарно ~99% выручки (top-площадки, мелкие игнорировать).',
            'Помочь принять ключевые решения, влияющие на рост выручки.',
            'Структурировано отделить: пути конверсий и слабые точки; анализ офферов vs рынок (с прогнозом); кросс-сейл для займовой аудитории; диагностику резких просадок EPC ото дня ко дню.',
            'Сегментировать отказной трафик по каналам домонетизации и для каждого канала рекомендовать офферы из payload (showcase / sms / offline / mobile_app), исходя из специфики канала.',
            'Указать точки роста и проседающие сегменты с числовыми метриками и планом действий.',
        ],
        // Схема выходного JSON. Используем её одновременно как часть промта и для валидации.
        // ВАЖНО: reasoning_log намеренно вынесен в КОНЕЦ схемы. Если модель упрётся в
        // max_tokens (deepseek-v4-flash: ~8192), обязательные поля (summary,
        // reject_monetization_strategy, risks) уже будут сериализованы ДО обрыва.
        'output_schema' => [
            'reasoning' => 'string — краткое резюме CoT для аналитика (опционально).',
            'summary' => 'string — деловое резюме периода (2-4 предложения), без воды.',
            // ВАЖНО: каждая рекомендация ДОЛЖНА быть «адресной» и «измеримой».
            // Пустые/общие формулировки (без target и без числового expected_impact.delta_pct)
            // в UI помечаются как low-quality и сворачиваются — поэтому модели нет смысла их
            // выдавать.
            'recommendations' => 'array<{title:string, target:{type:"offer"|"platform"|"sub1"|"channel"|"global", name:string}, action_type:string (one of allowed_action_types), action:string, implementation_steps:array<string> (2-5 шагов «что нажать / кому позвонить»), expected_impact:{metric:"epc"|"revenue"|"cr"|"ar"|"approved"|"clicks", current:number, target:number, delta_abs:number, delta_pct:number, horizon_days:7|30, confidence:"low"|"medium"|"high"}, evidence:{source:"daily_recent"|"weekly_trend"|"monthly_trend"|"market_compare"|"sub1_anomalies"|"offers_top"|"offers_underperforming"|"offers_daily_epc"|"top_platforms"|"traffic_sources_breakdown"|"forecast_baseline"|"anomalies_detected"|"epc_drops_signals"|"reject_funnel"|"kpi", fields:array<string>, values:object, note:string}, time_window:{from:string, to:string}, data_points_used:number, revoke_if:string (короткое условие отмены, ≤140 chars), priority:"high"|"medium"|"low"}> — 3-7 конкретных рекомендаций. ОГРАНИЧЕНИЯ: target.name (если type≠"global"|"channel") ОБЯЗАН быть из allowed_entities соответствующего типа; action_type ОБЯЗАН быть из allowed_action_types; expected_impact.delta_pct — обязательно ненулевое число; evidence.fields должны существовать в evidence.source-блоке payload.',
            'forecast' => '{period_7d:{revenue:number, clicks:number, epc:number, approve_rate:number, confidence:"low"|"medium"|"high", basis:string}, period_30d:{revenue:number, clicks:number, epc:number, approve_rate:number, confidence:"low"|"medium"|"high", basis:string}}',
            'platforms_breakdown' => 'array<{name:string, revenue_share_pct:number, status:"grow"|"stable"|"watch"|"risk", insight:string, action:string}> — разбор только по площадкам из top_platforms (до 10), с долей в выручке',
            'key_decisions' => 'array<{decision:string, target:{type:"offer"|"platform"|"sub1"|"channel"|"global", name:string}, action_type:string (one of allowed_action_types), rationale:string, kpi_impact:{metric:string, delta_pct:number, horizon_days:7|30}}> — 2-5 ключевых управленческих решений, направленных на рост выручки. Те же ограничения по target/action_type, что и в recommendations.',
            'risks' => 'array<string> — риски и аномалии, требующие внимания (фрод, концентрация, просадки)',
            // Структурированные блоки.
            'conversion_paths' => '{funnel:{clicks:number, conversions:number, approved:number, revenue:number, cr_pct:number, approve_rate_pct:number, epc:number}, weak_points:array<{stage:"click_to_conversion"|"conversion_to_approve"|"approve_to_revenue", where:string, metric:string, value:number, benchmark:number, severity:"high"|"medium"|"low", root_cause:string, fix:string, expected_uplift:string}>, summary:string} — пути конверсии и слабые точки воронки. weak_points — 2-6 шт.',
            'offers_market_analysis' => '{offers:array<{name:string, your_epc:number, market_epc:number, delta_pct:number, verdict:"scale"|"hold"|"replace"|"test", recommendation:string, forecast_epc:number, forecast_revenue_uplift:string, confidence:"low"|"medium"|"high", evidence:string}>, watchlist:array<{name:string, reason:string}>, summary:string} — поофферный анализ vs рыночный EPC, прогноз и список офферов, на которые стоит обратить внимание для увеличения доходности трафика. offers — 5-12 шт.',
            'cross_sell' => '{audience:string, products:array<{product:string, why:string, fit_score:"low"|"medium"|"high", suggested_offer_types:array<string>, expected_epc_range:string, kpi_impact:string}>, summary:string} — закономерности и идеи кросс-сейла другим продуктам займовой аудитории (страхование, карты, рефинанс, БКИ, мед.услуги, телеком и т.п.). 3-6 продуктов.',
            'epc_drops' => '{detected:boolean, drops:array<{date:string, prev_date:string, prev_epc:number, curr_epc:number, drop_pct:number, affected_offers:array<string>, affected_platforms:array<string>, evidence_based_reasons:array<string>, recommended_replacement:{offer:string, basis:string, expected_epc:number, historical_period:string}, confidence:"low"|"medium"|"high"}>, summary:string} — диагностика резких просадок EPC ото дня ко дню. Если просадок >=control.thresholds.epc_drop_pct% не обнаружено — detected=false, drops=[]. Иначе для каждой просадки даётся аргументированное обоснование (на основании daily_recent / weekly_trend / sub1_anomalies / market_compare) и рекомендуется оффер для замены, опираясь на ретроданные прошлых периодов.',
            // НОВЫЕ блоки для модернизации под отказной трафик МФО.
            'reject_monetization_strategy' => '{showcase_optimization:array<{offer_name:string, action:"add"|"promote"|"remove", reasoning:string}>, sms_campaigns:array<{offer_name:string, trigger_message_idea:string, expected_epc_uplift:number}>, offline_routing:array<{offer_name:string, reasoning:string}>, mobile_app_retention:array<{offer_name:string, placement_strategy:string}>, summary:string} — стратегия домонетизации отказного трафика по 4 каналам. По 2-5 офферов в каждом подмассиве. Все offer_name ОБЯЗАНЫ быть из allowed_entities.offers. Подбор: showcase = AR > control.thresholds.ar_floor% И CR > control.thresholds.cr_floor% (горячий трафик после отказа); sms = высокий EPC + высокий CR + триггер 0%/одобрение всем; offline = высокая выплата за лид + низкий CR (сложные офферы для дожима оператором); mobile_app = долгосрочные/LTV-офферы (карты, рассрочка, длинные займы).',
            'growth_points' => 'array<{dimension:string, target:{type:"offer"|"platform"|"sub1"|"channel"|"global", name:string}, action_type:string (one of allowed_action_types), current_metric:string, target_metric:string, expected_impact:{metric:"epc"|"revenue"|"cr"|"ar"|"approved"|"clicks", current:number, target:number, delta_abs:number, delta_pct:number, horizon_days:7|30, confidence:"low"|"medium"|"high"}, action_plan:string}> — 3-6 точек роста с числовыми текущими и целевыми метриками. dimension — например «Увеличение CR в SMS», «Рост EPC на витрине ЛК», «Снижение CPL в оффлайне».',
            'underperforming_segments' => 'array<{segment:"showcase"|"sms"|"offline"|"app"|"web", issue:string, solution:string}> — 2-5 проседающих сегментов отказного трафика. issue должен содержать конкретные числа из traffic_sources_breakdown.',
            // reasoning_log вынесен в самый конец схемы (см. комментарий выше).
            // Описание укорочено: одна короткая строка на фазу, без раздувания.
            'reasoning_log' => 'array<string> — РОВНО 5 коротких пунктов (≤200 символов каждый), английский: "PHASE 1 DATA SANITY: ...", "PHASE 2 FRAUD DETECTION: ...", "PHASE 3 MARKET & DROPS: ...", "PHASE 4 ROUTING: ...", "PHASE 5 SELF-CHECK: ...". По одной ключевой цифре на пункт, без длинных рассуждений. PHASE 5 — самопроверка: для каждой recommendation подтвердить, что target.name есть в allowed_entities и evidence.values сходятся с payload.',
        ],
    ];
}

function aiBuildSystemPrompt($sig) {
    // Полная JSON-схема ответа — встраиваем как авторитетный OUTPUT SCHEMA.
    // Сохраняем все ранее введённые блоки (forecast, conversion_paths,
    // offers_market_analysis, cross_sell, epc_drops, platforms_breakdown,
    // key_decisions, recommendations, growth_points, underperforming_segments),
    // но добавляем reasoning_log и формализуем CoT-фазы.
    $schemaJson = json_encode($sig['output_schema'], JSON_UNESCAPED_UNICODE | JSON_PRETTY_PRINT);
    $actionTypes = AI_ACTION_TYPES_JSON;
    return <<<PROMPT
[ROLE & CONTEXT]
You are a Senior Affiliate Marketing Analyst and Data Scientist specializing in Payday Loans (MFO) and Reject Traffic monetization.
Your objective is to analyze the provided JSON payload containing daily performance metrics, detect anomalies, and strategically route underperforming or rejected traffic to the most profitable monetization channels.

[CONTROL CONTEXT — READ FIRST, OBEY STRICTLY]
The input payload contains a `control` object that defines HOW you must analyze and recommend. Treat it as the operator's directive. Specifically:
- control.profile ∈ {conservative, balanced, aggressive_growth, antifraud_focus, roi_only} — adjusts your bias:
  • conservative      → prefer verdict=hold over replace; min_confidence=high for any "replace"/"pause_offer"; growth_points cap = 3.
  • balanced          → defaults; growth_points cap = 5.
  • aggressive_growth → bias toward verdict=scale and action_type∈{scale_traffic, add_to_showcase, launch_sms_campaign}; allow medium confidence for big bets.
  • antifraud_focus   → put fraud_suspect anomalies first in risks; recommend block_sub1 / investigate_sub1 with high priority; cap recommendations at 5, all targeting sub1/platform.
  • roi_only          → only recommend changes whose expected_impact.delta_pct ≥ 5 AND expected_impact.metric ∈ {revenue, epc}; drop everything else.
- control.thresholds.{min_clicks, epc_drop_pct, ar_floor, cr_floor, fraud_clicks_threshold} — OVERRIDE the defaults below. Use THESE values everywhere instead of the constants in [REJECT TRAFFIC ROUTING LOGIC] and [ANALYSIS PHASES].
- control.filters.{include_platforms, exclude_platforms, include_sub1, exclude_sub1, include_offers, exclude_offers} — analysis scope filters. NEVER produce a recommendation, key_decision, growth_point, watchlist entry, epc_drops entry, platforms_breakdown row or reject_monetization_strategy entry whose target/name is in the corresponding exclude_* list. If include_* is non-empty, restrict to those values only. Always honor the filters even if a stronger signal exists outside.
- control.kpi_targets — operator's targets for the period: {epc_target, revenue_target, sms_capacity_per_day, offline_capacity_per_day, monthly_budget_rub}. Cap your recommendations to feasible levels: SMS recommendations may not propose volume > sms_capacity_per_day; offline may not exceed offline_capacity_per_day; expected_impact.target on revenue/epc should reference distance to *_target.
- control.detail_level ∈ {short, detailed} — short → recommendations=3, growth_points=3, evidence.note ≤ 80 chars; detailed → recommendations=5-7, growth_points=5-6, evidence.note ≤ 200 chars.
- control.previous_feedback — array of {recommendation_title, target, action_type, verdict:"accepted"|"rejected"|"irrelevant", reason:string} from prior runs. DEMOTE (priority=low) or DROP entirely any recommendation whose (target.name + action_type) was rejected/irrelevant in the last feedback. Avoid repeating the exact title text of rejected items.

If the `control` object is missing — apply defaults (profile=balanced, detail_level=detailed, no filters, thresholds = those listed in [REJECT TRAFFIC ROUTING LOGIC]).

[ALLOWED ENTITIES — STRICT WHITELIST]
The payload contains an `allowed_entities` object: {offers:[...], platforms:[...], sub1:[...]}. Every offer name, platform name and sub1 value you mention in any output field MUST appear (case-insensitive trim) in the corresponding list. If a list is empty, you MUST NOT mention that entity type at all. This whitelist is computed deterministically from the payload — there is no "smarter" alternative.

[ALLOWED ACTION TYPES]
Every recommendations[].action_type, key_decisions[].action_type and growth_points[].action_type MUST be one of:
{$actionTypes}
Do not invent new action_type values. If no listed type fits — use "monitor".

[CRITICAL CONSTRAINTS - STRICT COMPLIANCE REQUIRED]
1. NO HALLUCINATIONS: You MUST ONLY recommend offer names, platform names and sub1 values that exactly match strings present in allowed_entities (see above). Do NOT invent, guess, translate or paraphrase offer names. If a channel has no eligible offers in allowed_entities — return an empty array for that channel and explain why in reject_monetization_strategy.summary.
2. DATA-DRIVEN ONLY: Every recommendation MUST be backed by numbers from the payload. Always cite the exact metric value in evidence.values (e.g. {"AR":18.4, "CR":12.1, "EPC":2.7}) AND name the source block in evidence.source AND list which fields you used in evidence.fields. evidence.fields MUST be real field names that exist in the named source block of the payload.
3. SILENT OPERATION: Do not introduce yourself, do not apologize, do not mention you are an AI, do not mention the model or provider.
4. JSON FORMAT: Your entire response MUST be a single, valid, parseable JSON object matching the OUTPUT SCHEMA below. No markdown, no code fences, no commentary outside JSON.
5. LANGUAGE: User-facing text fields (summary, reasoning, recommendations.title/action/implementation_steps/revoke_if/evidence.note, platforms_breakdown.*, conversion_paths.*, offers_market_analysis.*, cross_sell.*, epc_drops.*, reject_monetization_strategy.*, growth_points.*, underperforming_segments.*, risks) — Russian, professional, no fluff. The reasoning_log array AND all enum values (action_type, target.type, evidence.source, expected_impact.metric, etc.) — English/snake_case. Numbers — JSON numbers, never strings. Currency = ₽. Percentages = numbers like 27.4 (meaning 27.4%).
6. ADDRESSABILITY: Every recommendation, key_decision and growth_point MUST have a non-empty `target` and a non-empty `action_type`. If you cannot identify a concrete target — DO NOT emit the item; better fewer items than vague ones. expected_impact.delta_pct MUST be a non-zero number (positive for improvements, negative for cuts).
7. EVIDENCE INTEGRITY: For each recommendation, set time_window to the actual date range you used (subset of period.from..period.to) and data_points_used to the number of daily records that informed the call (e.g. 7 for last week). If data_points_used < control.thresholds.min_clicks_window (default 7) — set expected_impact.confidence="low".

[MATHEMATICS & DEFINITIONS — TREAT AS AXIOMS]
- EPC (Earnings Per Click) = Revenue / Clicks
- CR (Conversion Rate, %) = (Leads / Clicks) * 100        [Leads = "conversions" in payload]
- AR (Approval Rate, %) = (Approved / Leads) * 100
- CPA (Cost / Payout Per Action) = Revenue / Approved      [revenue per approved lead]
- Market Delta (%) = (your_epc - market_epc) / market_epc * 100
- EPC variance for an offer = stdev(offers_daily_epc[i].series[].epc) / mean(...)

[REJECT TRAFFIC ROUTING LOGIC — APPLY STRICTLY]
Classify and route candidate offers from the payload into FOUR reject monetization channels using these strict criteria. Use offers_top, traffic_sources_breakdown.*.top_offers and offers_daily_epc as the candidate pool. THRESHOLDS BELOW are DEFAULTS — if control.thresholds overrides them, use the overrides.

1. SHOWCASE (Витрина в ЛК МФО — Hot Online Rejects)
   • Hard condition: AR > control.thresholds.ar_floor (default 15%) AND CR > control.thresholds.cr_floor (default 10%).
   • Goal: Maximize probability of instant approval for users who just got rejected on the primary offer.
   • Signals from payload: high approve_rate_pct, high cr_pct, approval_difficulty="easy" or is_zero_percent=true.
   • action: "add" (new on showcase), "promote" (raise in ranking), "remove" (kick out an underperformer that was already there).

2. SMS CAMPAIGNS (Cold/Warm Rejects)
   • Hard condition: Highest CR (Click → Lead) AND high EPC among candidates.
   • Goal: Maximize clickbait conversion on a cooled-down base. CPA matters less than high-volume CR.
   • Signals: high cr_pct, high epc, is_zero_percent=true or approval_difficulty="easy".
   • trigger_message_idea: short SMS copy (≤140 chars) with a concrete trigger ("0%", "одобрение всем", "до 30 000₽ за 5 мин").
   • expected_epc_uplift: numeric expected % uplift of the SMS-channel EPC. RESPECT control.kpi_targets.sms_capacity_per_day.

3. OFFLINE CALL-CENTER (Hard Rejects)
   • Hard condition: Highest CPA among candidates. Low overall CR is acceptable if CPA is massive — that's what funds the operator.
   • Goal: Cover the expensive cost of human brokers/operators with high margin per closed sale.
   • Signals: is_installment=true, approval_difficulty="hard", low cr_pct + high revenue/conversions. RESPECT control.kpi_targets.offline_capacity_per_day.

4. MOBILE APP (Retention Rejects)
   • Hard condition: Stable EPC across the offers_daily_epc[].series array (low variance).
   • Goal: Long-term LTV — credit cards, installment loans, long PDL for loyal returning users.
   • Signals: stable daily epc series, is_installment=true, sustained approve_rate_pct.
   • placement_strategy: where exactly inside the app (home screen / push / re-loan widget / post-repayment offer).

[ANALYSIS PHASES - CHAIN OF THOUGHT]
Mentally execute these phases BEFORE you start writing JSON. Then write the JSON in schema order, putting the reasoning_log array LAST (it is the final key of the JSON object). Keep each reasoning_log entry to ≤200 chars (one sentence with ONE key number). Phases:
- PHASE 1 — DATA SANITY: Check for logical impossibilities (e.g. conversions > clicks, approved > conversions, negative revenue). Note any payload defects.
- PHASE 2 — FRAUD DETECTION: Scan sub1_anomalies for clicks > control.thresholds.fraud_clicks_threshold (default 300) with AR < 2% or CR < 1%. Flag suspicious sub1 values with their numbers.
- PHASE 3 — MARKET & DROPS: Walk market_compare and epc_drops_signals. Compute lost-revenue potential = sum(clicks * |delta|) for negative deltas.
- PHASE 4 — ROUTING: Apply [REJECT TRAFFIC ROUTING LOGIC] to top offers. For each routed offer name explicitly cite AR/CR/CPA/EPC values from the payload and which channel it goes to and why.
- PHASE 5 — SELF-CHECK: Walk YOUR OWN recommendations[]. For each, verify: (a) target.name ∈ allowed_entities, (b) action_type ∈ allowed_action_types, (c) evidence.values match payload numbers within ±1%, (d) target NOT in control.filters.exclude_*, (e) expected_impact.delta_pct ≠ 0. If anything fails — DROP the item BEFORE emitting JSON.

[ADDITIONAL BLOCK GUIDANCE — keep all of these populated]
A. conversion_paths — full funnel (clicks → conversions → approved → revenue) from kpi + 2-6 weak_points (stage / where / metric / value / benchmark / severity / root_cause / fix / expected_uplift).
B. offers_market_analysis — for each offer in market_compare set verdict (scale/hold/replace/test) using Market Delta, give recommendation, forecast_epc, forecast_revenue_uplift; build watchlist of high-volume offers with positive delta.
C. cross_sell — pick audience (PDL / installment / microloans) and propose 3-6 adjacent products (insurance, debit/credit cards, refinance, BKI, legal debt help, telecom). Tie each to a pattern visible in the payload.
D. epc_drops — find days where EPC dropped ≥control.thresholds.epc_drop_pct (default 20%) vs previous day (use daily_recent + offers_daily_epc); for each drop give evidence_based_reasons and a recommended_replacement offer drawn from historical periods. If none — detected=false, drops=[]. PRIORITIZE drops already pre-detected in input field anomalies_detected (kind="epc_drop" / "platform_epc_drop" / "offer_epc_drop") — they are the deterministic ground truth, do NOT skip them.
E. forecast — period_7d and period_30d numeric forecasts.
   IF input contains forecast_baseline.period_7d / period_30d (deterministic EMA + DOW-seasonality projection): USE IT AS THE STARTING POINT. You may adjust UP or DOWN by at most ±20% per metric, and ONLY with an explicit numeric reason (e.g. "drop on platform X − 15% revenue", "new offer Y added"). Put your final adjusted numbers in forecast.period_*.{revenue,clicks,epc} and put the baseline values in forecast.period_*.baseline.{revenue,clicks,epc}. Set forecast.period_*.adjustment_reason (string).
   IF baseline is absent — derive forecast from weekly_trend / monthly_trend yourself; degrade confidence if data is sparse.
F. platforms_breakdown — only top_platforms (~99% of revenue, ≤10). Skip the long tail. Honor control.filters.exclude_platforms.
G. growth_points — 3-6 items with numeric current_metric and target_metric and a concrete action_plan (use traffic_sources_breakdown numbers). Each MUST have target + action_type + expected_impact.
H. underperforming_segments — 2-5 items with segment ∈ {showcase, sms, offline, app, web}, issue with payload numbers, solution.
I. anomalies_detected (input field, NOT output) — массив заранее посчитанных аномалий: fraud_suspect (sub1 с подозрительно низким AR/CR при больших кликах), epc_drop (день-к-дню), offer_epc_drop / platform_epc_drop (просадки конкретного оффера или площадки vs trailing-7d-median), quality_anomaly (offer×platform пары с аномально низким AR). Используй их как АВТОРИТЕТНЫЕ диагнозы — не пропускай и не пересчитывай. Включи их в risks (kind=fraud_suspect → severity=high) и в epc_drops (kind=*epc_drop*). Для quality_anomaly выводи в reject_monetization_strategy с предложением routing-а.

[OUTPUT SCHEMA]
Return ONLY a single JSON object exactly matching this schema (keys and types):
{$schemaJson}
PROMPT;
}

function aiBuildUserPrompt($payload, $sig) {
    // По новому ТЗ user prompt должен быть максимально коротким — вся логика
    // и схема уже вшиты в system prompt. Здесь просто доставляем payload.
    $data = json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_PRETTY_PRINT);
    return "Analyze the following performance snapshot according to your system instructions. Mentally walk the 4 phases, then output the final JSON. Place the reasoning_log array LAST in the JSON (it is the final key) with ≤200 chars per phase entry.\n\n### PAYLOAD JSON ###\n{$data}\n### END PAYLOAD ###";
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
        'anomalies_detected' => 30,   // pre-detected аномалии (fraud/epc_drop/quality)
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
    // traffic_sources_breakdown: гарантируем, что top_offers внутри каждого
    // канала не более 5 — этого достаточно AI, чтобы сформировать
    // reject_monetization_strategy без раздувания промпта.
    if (isset($p['traffic_sources_breakdown']) && is_array($p['traffic_sources_breakdown'])) {
        foreach ($p['traffic_sources_breakdown'] as &$ch) {
            if (is_array($ch) && isset($ch['top_offers']) && is_array($ch['top_offers']) && count($ch['top_offers']) > 5) {
                $ch['top_offers'] = array_slice($ch['top_offers'], 0, 5);
            }
        }
        unset($ch);
    }
    return $p;
}

function aiValidateOutput($obj, $sig) {
    if (!is_array($obj)) return 'Output is not a JSON object';
    // Жёстко валидируем ТОЛЬКО три ключа верхнего уровня (по новому ТЗ):
    //   summary, reject_monetization_strategy, risks.
    // Остальные блоки — мягкие: при отсутствии бэкфиллим безопасными
    // значениями в aiBackfillOutput, чтобы фронт не падал.
    foreach (['summary', 'reject_monetization_strategy', 'risks'] as $req) {
        if (!array_key_exists($req, $obj)) return "Missing required field: {$req}";
    }
    if (!is_array($obj['reject_monetization_strategy'])) {
        return 'reject_monetization_strategy must be an object';
    }
    if (!is_array($obj['risks'])) {
        return 'risks must be an array';
    }
    // Мягкая проверка типов прочих блоков (не роняем ответ, если просто отсутствуют).
    foreach (['recommendations', 'platforms_breakdown', 'key_decisions', 'growth_points',
             'underperforming_segments', 'reasoning_log'] as $opt) {
        if (array_key_exists($opt, $obj) && !is_array($obj[$opt])) {
            return "{$opt} must be an array";
        }
    }
    foreach (['forecast', 'conversion_paths', 'offers_market_analysis', 'cross_sell', 'epc_drops'] as $opt) {
        if (array_key_exists($opt, $obj) && !is_array($obj[$opt])) {
            return "{$opt} must be an object";
        }
    }
    return null;
}

// Бэкфилл недостающих опциональных полей. Гарантирует, что фронтенд получит
// предсказуемую структуру, даже если модель пропустила часть блоков (например,
// забыла mobile_app_retention внутри reject_monetization_strategy).
function aiBackfillOutput($obj) {
    if (!is_array($obj)) return $obj;
    // Опциональные массивы верхнего уровня.
    foreach (['recommendations', 'platforms_breakdown', 'key_decisions',
             'growth_points', 'underperforming_segments', 'reasoning_log'] as $k) {
        if (!array_key_exists($k, $obj) || !is_array($obj[$k])) $obj[$k] = [];
    }
    // Опциональные объекты верхнего уровня.
    foreach (['forecast', 'conversion_paths', 'offers_market_analysis', 'cross_sell', 'epc_drops'] as $k) {
        if (!array_key_exists($k, $obj) || !is_array($obj[$k])) $obj[$k] = new stdClass();
    }
    // reject_monetization_strategy: гарантируем все 4 подмассива.
    $rms = is_array($obj['reject_monetization_strategy'] ?? null) ? $obj['reject_monetization_strategy'] : [];
    foreach (['showcase_optimization', 'sms_campaigns', 'offline_routing', 'mobile_app_retention'] as $k) {
        if (!array_key_exists($k, $rms) || !is_array($rms[$k])) $rms[$k] = [];
    }
    if (!array_key_exists('summary', $rms) || !is_string($rms['summary'])) $rms['summary'] = '';
    $obj['reject_monetization_strategy'] = $rms;
    return $obj;
}

// =================== УПРАВЛЯЕМОСТЬ: control defaults ===================
// Фронт может прислать `payload.control` с профилем/порогами/фильтрами/KPI.
// Здесь мы заполняем недостающие поля безопасными дефолтами, чтобы и
// system-prompt, и пост-валидация работали с одной и той же конфигурацией.
function aiNormalizeControl($control) {
    if (!is_array($control)) $control = [];
    $allowedProfiles = ['conservative','balanced','aggressive_growth','antifraud_focus','roi_only'];
    $profile = isset($control['profile']) && in_array($control['profile'], $allowedProfiles, true)
        ? $control['profile'] : 'balanced';
    $detail = (isset($control['detail_level']) && $control['detail_level'] === 'short') ? 'short' : 'detailed';

    $th = is_array($control['thresholds'] ?? null) ? $control['thresholds'] : [];
    $thresholds = [
        'min_clicks'              => isset($th['min_clicks'])              ? max(0, (int)$th['min_clicks'])              : 100,
        'epc_drop_pct'            => isset($th['epc_drop_pct'])            ? max(1, (int)$th['epc_drop_pct'])            : 20,
        'ar_floor'                => isset($th['ar_floor'])                ? max(0, (float)$th['ar_floor'])              : 15.0,
        'cr_floor'                => isset($th['cr_floor'])                ? max(0, (float)$th['cr_floor'])              : 10.0,
        'fraud_clicks_threshold'  => isset($th['fraud_clicks_threshold'])  ? max(0, (int)$th['fraud_clicks_threshold'])  : 300,
        'min_clicks_window'       => isset($th['min_clicks_window'])       ? max(1, (int)$th['min_clicks_window'])       : 7,
    ];

    $f = is_array($control['filters'] ?? null) ? $control['filters'] : [];
    $listOf = function ($v) {
        if (!is_array($v)) return [];
        $out = [];
        foreach ($v as $x) { if (is_string($x) && trim($x) !== '') $out[] = trim($x); }
        return array_values(array_unique($out));
    };
    $filters = [
        'include_platforms' => $listOf($f['include_platforms'] ?? []),
        'exclude_platforms' => $listOf($f['exclude_platforms'] ?? []),
        'include_sub1'      => $listOf($f['include_sub1'] ?? []),
        'exclude_sub1'      => $listOf($f['exclude_sub1'] ?? []),
        'include_offers'    => $listOf($f['include_offers'] ?? []),
        'exclude_offers'    => $listOf($f['exclude_offers'] ?? []),
    ];

    $kpi = is_array($control['kpi_targets'] ?? null) ? $control['kpi_targets'] : [];
    $kpiTargets = [
        'epc_target'              => isset($kpi['epc_target'])              ? (float)$kpi['epc_target']              : 0,
        'revenue_target'          => isset($kpi['revenue_target'])          ? (float)$kpi['revenue_target']          : 0,
        'sms_capacity_per_day'    => isset($kpi['sms_capacity_per_day'])    ? (int)  $kpi['sms_capacity_per_day']    : 0,
        'offline_capacity_per_day'=> isset($kpi['offline_capacity_per_day'])? (int)  $kpi['offline_capacity_per_day']: 0,
        'monthly_budget_rub'      => isset($kpi['monthly_budget_rub'])      ? (float)$kpi['monthly_budget_rub']      : 0,
    ];

    $prevFb = [];
    if (isset($control['previous_feedback']) && is_array($control['previous_feedback'])) {
        foreach (array_slice($control['previous_feedback'], 0, 50) as $fb) {
            if (!is_array($fb)) continue;
            $prevFb[] = [
                'recommendation_title' => (string)($fb['recommendation_title'] ?? ''),
                'target'               => is_array($fb['target'] ?? null) ? $fb['target'] : null,
                'action_type'          => (string)($fb['action_type'] ?? ''),
                'verdict'              => in_array($fb['verdict'] ?? '', ['accepted','rejected','irrelevant'], true) ? $fb['verdict'] : '',
                'reason'               => (string)($fb['reason'] ?? ''),
            ];
        }
    }

    return [
        'profile'           => $profile,
        'detail_level'      => $detail,
        'thresholds'        => $thresholds,
        'filters'           => $filters,
        'kpi_targets'       => $kpiTargets,
        'previous_feedback' => $prevFb,
    ];
}

// =================== ТОЧНОСТЬ: post-обработка AI-ответа ===================
// Применяем дисциплинирующие правила К JSON-ответу модели уже после schema +
// no-hallu валидации. Возвращаем «улучшенный» объект и счётчики качества для
// телеметрии (сохраняются в ai_analysis_cache.*_count).
//
//   1. Калибровка confidence по data_points_used и delta_pct.
//   2. Применение exclude-фильтров (на случай, если модель проигнорировала).
//   3. Понижение priority для общих фраз (нет target / action_type / delta_pct)
//      + установка флага quality:"low" — UI сворачивает такие карточки.
//   4. Дедупликация по (target.type+target.name+action_type).
//   5. Подсчёт пропущенных anomalies_detected (epc_drop → epc_drops.drops,
//      fraud_suspect → risks).
function aiPostProcess($obj, $payload, $control) {
    $counters = [
        'empty_evidence_count'    => 0,
        'numeric_check_failures'  => 0,
        'low_quality_recs'        => 0,
        'duplicate_recs_dropped'  => 0,
        'filter_drops'            => 0,
        'anomaly_coverage_missed' => 0,
    ];
    if (!is_array($obj)) return ['data' => $obj, 'counters' => $counters];

    $excludeOffersLc    = array_map('mb_strtolower', $control['filters']['exclude_offers']    ?? []);
    $excludePlatformsLc = array_map('mb_strtolower', $control['filters']['exclude_platforms'] ?? []);
    $excludeSub1Lc      = array_map('mb_strtolower', $control['filters']['exclude_sub1']      ?? []);

    // --- 1+3. Recommendations: калибровка confidence, low-quality flag, exclude. ---
    $allowedActionTypes = json_decode(AI_ACTION_TYPES_JSON, true) ?: [];
    $processedRecs = [];
    $seenKeys = [];
    foreach ($obj['recommendations'] ?? [] as $r) {
        if (!is_array($r)) continue;

        // Exclude-фильтр.
        $tType = is_array($r['target'] ?? null) ? (string)($r['target']['type'] ?? '') : '';
        $tName = is_array($r['target'] ?? null) ? trim((string)($r['target']['name'] ?? '')) : '';
        $tNameLc = mb_strtolower($tName);
        if ($tType === 'offer'    && $tName !== '' && in_array($tNameLc, $excludeOffersLc, true))    { $counters['filter_drops']++; continue; }
        if ($tType === 'platform' && $tName !== '' && in_array($tNameLc, $excludePlatformsLc, true)) { $counters['filter_drops']++; continue; }
        if ($tType === 'sub1'     && $tName !== '' && in_array($tNameLc, $excludeSub1Lc, true))      { $counters['filter_drops']++; continue; }

        // Калибровка confidence.
        $ei = is_array($r['expected_impact'] ?? null) ? $r['expected_impact'] : [];
        $deltaPct = isset($ei['delta_pct']) ? (float)$ei['delta_pct'] : 0;
        $dataPoints = isset($r['data_points_used']) ? (int)$r['data_points_used'] : 0;
        $minWindow = (int)($control['thresholds']['min_clicks_window'] ?? 7);
        $cal = 'medium';
        if ($dataPoints >= 21 && abs($deltaPct) >= 5) $cal = 'high';
        elseif ($dataPoints < $minWindow || abs($deltaPct) < 5) $cal = 'low';
        // Не повышаем уверенность модели, только понижаем — модель может
        // переоценивать confidence, но не имеет права быть увереннее данных.
        $modelConf = (string)($ei['confidence'] ?? '');
        $rank = ['low' => 0, 'medium' => 1, 'high' => 2];
        if (!isset($rank[$modelConf]) || $rank[$cal] < $rank[$modelConf]) {
            $ei['confidence'] = $cal;
            $r['expected_impact'] = $ei;
        }

        // Low-quality detector: нет target / action_type / нулевая delta.
        $atype = (string)($r['action_type'] ?? '');
        $isLow = ($tType === '' || $tName === '' && $tType !== 'global')
              || (!in_array($atype, $allowedActionTypes, true))
              || ($deltaPct == 0);
        if (empty($r['evidence']) || (is_array($r['evidence']) && empty($r['evidence']['source'] ?? ''))) {
            $isLow = true;
            $counters['empty_evidence_count']++;
        }
        if ($isLow) {
            $r['quality'] = 'low';
            $r['priority'] = 'low';
            $counters['low_quality_recs']++;
        }

        // ROI-only профиль: режем то, что не дотягивает по delta_pct/metric.
        if (($control['profile'] ?? '') === 'roi_only') {
            $metric = (string)($ei['metric'] ?? '');
            if (abs($deltaPct) < 5 || !in_array($metric, ['revenue', 'epc'], true)) {
                $counters['filter_drops']++;
                continue;
            }
        }

        // Дедупликация по (target.type, target.name, action_type).
        $dupKey = $tType . '|' . $tNameLc . '|' . $atype;
        if ($atype !== '' && $tName !== '' && isset($seenKeys[$dupKey])) {
            $counters['duplicate_recs_dropped']++;
            continue;
        }
        $seenKeys[$dupKey] = true;

        $processedRecs[] = $r;
    }
    $obj['recommendations'] = $processedRecs;

    // --- 5. Покрытие anomalies_detected. ---
    $anoms = is_array($payload['anomalies_detected'] ?? null) ? $payload['anomalies_detected'] : [];
    if ($anoms) {
        $missedDrops = 0; $missedFraud = 0;
        // Собираем то, что модель упомянула.
        $epcDropsList = is_array($obj['epc_drops']['drops'] ?? null) ? $obj['epc_drops']['drops'] : [];
        $mentionedDropDates = [];
        foreach ($epcDropsList as $d) {
            if (!is_array($d)) continue;
            if (!empty($d['date'])) $mentionedDropDates[mb_strtolower(trim((string)$d['date']))] = true;
            if (!empty($d['affected_offers']) && is_array($d['affected_offers'])) {
                foreach ($d['affected_offers'] as $n) $mentionedDropDates[mb_strtolower(trim((string)$n))] = true;
            }
            if (!empty($d['affected_platforms']) && is_array($d['affected_platforms'])) {
                foreach ($d['affected_platforms'] as $n) $mentionedDropDates[mb_strtolower(trim((string)$n))] = true;
            }
        }
        $risksTextLc = mb_strtolower(json_encode($obj['risks'] ?? [], JSON_UNESCAPED_UNICODE) ?: '');
        foreach ($anoms as $a) {
            if (!is_array($a)) continue;
            $kind = (string)($a['kind'] ?? '');
            if (in_array($kind, ['epc_drop','offer_epc_drop','platform_epc_drop'], true)) {
                $key = mb_strtolower(trim((string)($a['offer'] ?? $a['platform'] ?? $a['date'] ?? '')));
                if ($key !== '' && empty($mentionedDropDates[$key])) {
                    $missedDrops++;
                }
            } elseif ($kind === 'fraud_suspect') {
                $sub = mb_strtolower(trim((string)($a['sub1'] ?? '')));
                if ($sub !== '' && strpos($risksTextLc, $sub) === false) $missedFraud++;
            }
        }
        $counters['anomaly_coverage_missed'] = $missedDrops + $missedFraud;
    }

    return ['data' => $obj, 'counters' => $counters];
}

/**
 * Собирает белый список имён офферов и площадок из payload-а.
 * Имена нормализуются (lowercase + trim) для устойчивого сравнения.
 * Используется в no-hallucination валидаторе ниже.
 */
function aiCollectAllowedNames($payload) {
    $offers    = [];
    $platforms = [];
    $sub1s     = [];
    $pushOffer = function ($name) use (&$offers) {
        $n = is_string($name) ? trim($name) : '';
        if ($n !== '') $offers[mb_strtolower($n)] = $n;
    };
    $pushPlat = function ($name) use (&$platforms) {
        $n = is_string($name) ? trim($name) : '';
        if ($n !== '') $platforms[mb_strtolower($n)] = $n;
    };
    $pushSub = function ($name) use (&$sub1s) {
        $n = is_string($name) ? trim($name) : '';
        if ($n !== '') $sub1s[mb_strtolower($n)] = $n;
    };
    foreach (['offers_top', 'offers_underperforming', 'market_compare', 'offers_daily_epc'] as $k) {
        if (!isset($payload[$k]) || !is_array($payload[$k])) continue;
        foreach ($payload[$k] as $row) {
            if (is_array($row) && isset($row['name'])) $pushOffer($row['name']);
        }
    }
    if (isset($payload['top_platforms']) && is_array($payload['top_platforms'])) {
        foreach ($payload['top_platforms'] as $row) {
            if (is_array($row) && isset($row['name'])) $pushPlat($row['name']);
        }
    }
    if (isset($payload['sub1_anomalies']) && is_array($payload['sub1_anomalies'])) {
        foreach ($payload['sub1_anomalies'] as $row) {
            if (is_array($row) && isset($row['name'])) $pushSub($row['name']);
        }
    }
    if (isset($payload['traffic_sources_breakdown']) && is_array($payload['traffic_sources_breakdown'])) {
        foreach ($payload['traffic_sources_breakdown'] as $ch) {
            if (is_array($ch) && isset($ch['top_offers']) && is_array($ch['top_offers'])) {
                foreach ($ch['top_offers'] as $o) {
                    if (is_array($o) && isset($o['name'])) $pushOffer($o['name']);
                }
            }
        }
    }
    // anomalies_detected (новый блок из buildAIPayload) тоже содержит имена.
    if (isset($payload['anomalies_detected']) && is_array($payload['anomalies_detected'])) {
        foreach ($payload['anomalies_detected'] as $a) {
            if (!is_array($a)) continue;
            if (isset($a['offer']))    $pushOffer($a['offer']);
            if (isset($a['platform'])) $pushPlat($a['platform']);
            if (isset($a['sub1']))     $pushSub($a['sub1']);
        }
    }
    return [
        'offers'    => array_values($offers),
        'platforms' => array_values($platforms),
        'sub1s'     => array_values($sub1s),
    ];
}

/**
 * Возвращает массив выдуманных имён, которые модель упомянула в ответе,
 * но которых нет в payload. Это самая частая причина «красивых, но
 * бесполезных» советов: AI любит подкидывать «Webbankir», «MoneyMan»
 * и т.п. из общего знания, даже если их нет в данных площадки.
 *
 * Возвращаемая структура: array<{path:string, kind:"offer"|"platform", name:string}>.
 */
function aiFindHallucinations($obj, $allowed) {
    if (!is_array($obj)) return [];
    $allowedOffersLc    = array_map('mb_strtolower', $allowed['offers']);
    $allowedPlatformsLc = array_map('mb_strtolower', $allowed['platforms']);
    $issues = [];

    $checkOffer = function ($name, $path) use (&$issues, $allowedOffersLc) {
        if (!is_string($name)) return;
        $trim = trim($name);
        if ($trim === '') return;
        $lc = mb_strtolower($trim);
        if (!in_array($lc, $allowedOffersLc, true)) {
            $issues[] = ['path' => $path, 'kind' => 'offer', 'name' => $trim];
        }
    };
    $checkPlat = function ($name, $path) use (&$issues, $allowedPlatformsLc) {
        if (!is_string($name)) return;
        $trim = trim($name);
        if ($trim === '') return;
        $lc = mb_strtolower($trim);
        if (!in_array($lc, $allowedPlatformsLc, true)) {
            $issues[] = ['path' => $path, 'kind' => 'platform', 'name' => $trim];
        }
    };

    // reject_monetization_strategy.*[].offer_name
    $rms = $obj['reject_monetization_strategy'] ?? null;
    if (is_array($rms)) {
        foreach (['showcase_optimization', 'sms_campaigns', 'offline_routing', 'mobile_app_retention'] as $sub) {
            if (!isset($rms[$sub]) || !is_array($rms[$sub])) continue;
            foreach ($rms[$sub] as $i => $row) {
                if (is_array($row) && isset($row['offer_name'])) {
                    $checkOffer($row['offer_name'], "reject_monetization_strategy.{$sub}[{$i}].offer_name");
                }
            }
        }
    }
    // offers_market_analysis.offers[].name + watchlist[].name
    $oma = $obj['offers_market_analysis'] ?? null;
    if (is_array($oma)) {
        if (isset($oma['offers']) && is_array($oma['offers'])) {
            foreach ($oma['offers'] as $i => $row) {
                if (is_array($row) && isset($row['name'])) {
                    $checkOffer($row['name'], "offers_market_analysis.offers[$i].name");
                }
            }
        }
        if (isset($oma['watchlist']) && is_array($oma['watchlist'])) {
            foreach ($oma['watchlist'] as $i => $row) {
                if (is_array($row) && isset($row['name'])) {
                    $checkOffer($row['name'], "offers_market_analysis.watchlist[$i].name");
                }
            }
        }
    }
    // epc_drops.drops[].affected_offers + recommended_replacement.offer + affected_platforms
    $drops = $obj['epc_drops']['drops'] ?? null;
    if (is_array($drops)) {
        foreach ($drops as $i => $d) {
            if (!is_array($d)) continue;
            if (isset($d['affected_offers']) && is_array($d['affected_offers'])) {
                foreach ($d['affected_offers'] as $j => $n) {
                    $checkOffer($n, "epc_drops.drops[$i].affected_offers[$j]");
                }
            }
            if (isset($d['affected_platforms']) && is_array($d['affected_platforms'])) {
                foreach ($d['affected_platforms'] as $j => $n) {
                    $checkPlat($n, "epc_drops.drops[$i].affected_platforms[$j]");
                }
            }
            if (isset($d['recommended_replacement']) && is_array($d['recommended_replacement'])
                && isset($d['recommended_replacement']['offer'])) {
                $checkOffer($d['recommended_replacement']['offer'],
                    "epc_drops.drops[$i].recommended_replacement.offer");
            }
        }
    }
    // platforms_breakdown[].name
    if (isset($obj['platforms_breakdown']) && is_array($obj['platforms_breakdown'])) {
        foreach ($obj['platforms_breakdown'] as $i => $p) {
            if (is_array($p) && isset($p['name'])) {
                $checkPlat($p['name'], "platforms_breakdown[$i].name");
            }
        }
    }
    return $issues;
}

function aiCallProviderStructured($sysPrompt, $userPrompt, $sig, $payload = null) {
    // Аккумулятор телеметрии по всем вызовам провайдера в рамках одной
    // структурированной сессии (primary → fallback → refine → no-hallu refine).
    // Возвращается наверх вместе с распарсенным/валидным JSON.
    $meta = [
        'latency_ms'        => 0,
        'prompt_tokens'     => 0,
        'completion_tokens' => 0,
        'total_tokens'      => 0,
        'model_used'        => '',
        'calls'             => 0,
        'stages'            => [], // human-readable: ['primary','refine','hallu_refine']
        // ---- Quality counters (для дашборда здоровья промпта). ----
        'refine_rounds'           => 0,
        'hallu_offer_count'       => 0,
        'numeric_check_failures'  => 0,
        'empty_evidence_count'    => 0,
        'low_quality_recs'        => 0,
        'duplicate_recs_dropped'  => 0,
        'filter_drops'            => 0,
        'anomaly_coverage_missed' => 0,
    ];
    $accMeta = function (&$meta, $resp, $stage) {
        $meta['calls']++;
        $meta['stages'][] = $stage;
        if ($stage === 'refine_schema' || $stage === 'refine_no_hallu') {
            $meta['refine_rounds']++;
        }
        if (isset($resp['latency_ms']))  $meta['latency_ms']        += (int)$resp['latency_ms'];
        if (isset($resp['model']))       $meta['model_used']         = (string)$resp['model'];
        if (isset($resp['usage']) && is_array($resp['usage'])) {
            $u = $resp['usage'];
            $meta['prompt_tokens']     += (int)($u['prompt_tokens']     ?? 0);
            $meta['completion_tokens'] += (int)($u['completion_tokens'] ?? 0);
            $meta['total_tokens']      += (int)($u['total_tokens']      ?? 0);
        }
    };
    // Helper: финализация — пост-обработка (фильтры, confidence, дедуп,
    // anomaly coverage), bake counters в meta, backfill опциональных полей.
    $finalize = function ($obj) use (&$meta, $payload) {
        $control = aiNormalizeControl($payload['control'] ?? null);
        // Считаем галлюцинации ПОСЛЕ no-hallu refine — это итоговое значение.
        if (is_array($payload)) {
            $allowedFinal = aiCollectAllowedNames($payload);
            $issuesFinal  = aiFindHallucinations($obj, $allowedFinal);
            $meta['hallu_offer_count'] = count($issuesFinal);
        }
        $pp = aiPostProcess($obj, is_array($payload) ? $payload : [], $control);
        foreach (['empty_evidence_count','numeric_check_failures','low_quality_recs',
                  'duplicate_recs_dropped','filter_drops','anomaly_coverage_missed'] as $k) {
            $meta[$k] = (int)($pp['counters'][$k] ?? 0);
        }
        return aiBackfillOutput($pp['data']);
    };

    // Шаг 1 — основной запрос на быстрой модели (deepseek-v4-flash).
    $resp = aiCallProvider($sysPrompt, $userPrompt, AI_MODEL);
    $accMeta($meta, $resp, 'primary');
    if (isset($resp['error'])) {
        // Один fallback на флагман V4-pro (он медленнее, но иногда переживает 5xx).
        aiLog('structured_fallback', ['primary_error' => $resp['error']]);
        $resp = aiCallProvider($sysPrompt, $userPrompt, AI_FALLBACK_MODEL);
        $accMeta($meta, $resp, 'fallback');
        if (isset($resp['error'])) { $resp['meta'] = $meta; return $resp; }
    }
    $obj = aiExtractJson($resp['content'] ?? '');
    $err = aiValidateOutput($obj, $sig);
    if ($err === null) {
        aiLog('structured_ok', ['stage' => 'primary']);
        // No-hallucination проверка ПОСЛЕ schema validation.
        $obj = aiNoHalluRefineIfNeeded($obj, $sysPrompt, $userPrompt, $sig, $payload, $meta);
        return ['data' => $finalize($obj), 'meta' => $meta];
    }
    aiLog('structured_validation_failed', [
        'stage'           => 'primary',
        'validation_err'  => $err,
        'content_preview' => substr((string)($resp['content'] ?? ''), 0, 300),
    ]);

    // Шаг 2 (Refine) — один retry с явной коррекцией.
    // ВАЖНО: refine всегда идёт по быстрой модели (deepseek-v4-flash), даже
    // если основной ответ пришёл от V4-pro. Иначе суммарное время
    // запроса (flash ~60c + v4-pro ~150c) легко уходит за proxy_read_timeout
    // вышестоящего nginx/Cloudflare и клиент получает HTML 504 вместо JSON.
    $refineUser = $userPrompt . "\n\nПредыдущий ответ был невалиден: {$err}. Верни ИСПРАВЛЕННЫЙ JSON строго по схеме, без markdown.";
    $resp2 = aiCallProvider($sysPrompt, $refineUser, AI_MODEL);
    $accMeta($meta, $resp2, 'refine_schema');
    if (isset($resp2['error'])) { $resp2['meta'] = $meta; return $resp2; }
    $obj2 = aiExtractJson($resp2['content'] ?? '');
    $err2 = aiValidateOutput($obj2, $sig);
    if ($err2 === null) {
        aiLog('structured_ok', ['stage' => 'refine']);
        $obj2 = aiNoHalluRefineIfNeeded($obj2, $sysPrompt, $userPrompt, $sig, $payload, $meta);
        return ['data' => $finalize($obj2), 'meta' => $meta];
    }
    aiLog('structured_validation_failed', [
        'stage'           => 'refine',
        'validation_err'  => $err2,
        'content_preview' => substr((string)($resp2['content'] ?? ''), 0, 300),
    ]);

    return ['error' => 'Validation failed: ' . $err2, 'meta' => $meta];
}

/**
 * Если в ответе модели обнаружены имена офферов/площадок, которых нет в
 * payload, делаем один точечный Refine с явным списком разрешённых имён и
 * списком запрещённых. Если и после этого галлюцинации остались — оставляем
 * лучший из двух ответов (с меньшим числом галлюцинаций) и логируем.
 *
 * Это самая дешёвая и эффективная защита от «красивых, но выдуманных»
 * рекомендаций (типа упоминания популярных МФО, которых нет в кабинете
 * пользователя).
 */
function aiNoHalluRefineIfNeeded($obj, $sysPrompt, $userPrompt, $sig, $payload, &$meta) {
    if (!is_array($payload)) return $obj;
    $allowed = aiCollectAllowedNames($payload);
    $issues  = aiFindHallucinations($obj, $allowed);
    if (empty($issues)) return $obj;
    aiLog('hallucination_detected', [
        'count'    => count($issues),
        'examples' => array_slice($issues, 0, 5),
    ]);

    // Готовим компактный allowed-list для модели. Жёстко ограничиваем размер,
    // чтобы не раздуть refine-промт.
    $offersList    = array_slice($allowed['offers'], 0, 60);
    $platformsList = array_slice($allowed['platforms'], 0, 30);
    $forbidden     = array_values(array_unique(array_map(fn($i) => $i['name'], $issues)));
    $forbiddenList = array_slice($forbidden, 0, 30);

    $instr  = "ВАЖНО: предыдущий ответ содержит имена, которых НЕТ в payload (галлюцинации). ";
    $instr .= "Используй ТОЛЬКО имена из allow-list. Запрещённые имена ниже — удали их и замени реальными из allow-list или оставь массивы пустыми.\n\n";
    $instr .= "ALLOWED OFFERS (используй ТОЛЬКО их):\n- " . implode("\n- ", $offersList) . "\n\n";
    $instr .= "ALLOWED PLATFORMS:\n- " . implode("\n- ", $platformsList) . "\n\n";
    $instr .= "FORBIDDEN NAMES (выдуманные, удалить):\n- " . implode("\n- ", $forbiddenList) . "\n\n";
    $instr .= "Верни ИСПРАВЛЕННЫЙ JSON строго по схеме, без markdown. Все offer_name / name в reject_monetization_strategy, offers_market_analysis, epc_drops, platforms_breakdown ОБЯЗАНЫ совпадать со строками из allow-list побайтно.";

    $refineUser = $userPrompt . "\n\n" . $instr;
    $resp = aiCallProvider($sysPrompt, $refineUser, AI_MODEL);
    if (isset($meta) && is_array($meta)) {
        $meta['calls']    = ($meta['calls']    ?? 0) + 1;
        $meta['stages'][] = 'refine_no_hallu';
        if (isset($resp['latency_ms']))  $meta['latency_ms']  = ($meta['latency_ms']  ?? 0) + (int)$resp['latency_ms'];
        if (isset($resp['model']))       $meta['model_used']  = (string)$resp['model'];
        if (isset($resp['usage']) && is_array($resp['usage'])) {
            $u = $resp['usage'];
            $meta['prompt_tokens']     = ($meta['prompt_tokens']     ?? 0) + (int)($u['prompt_tokens']     ?? 0);
            $meta['completion_tokens'] = ($meta['completion_tokens'] ?? 0) + (int)($u['completion_tokens'] ?? 0);
            $meta['total_tokens']      = ($meta['total_tokens']      ?? 0) + (int)($u['total_tokens']      ?? 0);
        }
    }
    if (isset($resp['error'])) {
        aiLog('hallucination_refine_failed', ['error' => $resp['error']]);
        return $obj; // оставляем исходный — он хотя бы прошёл schema validation
    }
    $obj2 = aiExtractJson($resp['content'] ?? '');
    $err  = aiValidateOutput($obj2, $sig);
    if ($err !== null) {
        aiLog('hallucination_refine_invalid', ['validation_err' => $err]);
        return $obj;
    }
    $issues2 = aiFindHallucinations($obj2, $allowed);
    if (count($issues2) <= count($issues)) {
        aiLog('hallucination_refine_ok', [
            'before' => count($issues),
            'after'  => count($issues2),
        ]);
        return $obj2;
    }
    // Стало хуже — откатываемся.
    aiLog('hallucination_refine_worse', [
        'before' => count($issues),
        'after'  => count($issues2),
    ]);
    return $obj;
}

function aiCallProvider($sysPrompt, $userPrompt, $model) {
    // Legacy `deepseek-reasoner` НЕ поддерживает temperature, top_p,
    // presence_penalty, frequency_penalty, response_format и tool/function
    // calling. Передача любого из этих полей даёт HTTP 400 →
    // "AI service unavailable". Новый `deepseek-v4-pro` (флагман V4) ЭТИ
    // поля поддерживает — гейтим JSON-mode/temperature только для legacy.
    $isLegacyReasoner = (stripos((string)$model, 'reasoner') !== false);
    // «Thinking» модели (legacy reasoner + V4-pro) кладут chain-of-thought
    // в reasoning_content, который тратит max_tokens и заметно увеличивает
    // время ответа → им нужен повышенный бюджет токенов и таймаут.
    $isThinkingModel  = $isLegacyReasoner
        || (stripos((string)$model, 'v4-pro') !== false);

    // ВАЖНО: ходим в провайдера ВСЕГДА в режиме SSE (stream=true).
    // Без стриминга deepseek шлёт тело ответа одним куском только после
    // полной генерации, а на длинных prompt-ах (~60К chars + 8K max_tokens)
    // это занимает >90с → curl возвращает "Operation timed out ... 1 bytes
    // received" даже при исправной сети. Со стримингом chunks приходят
    // непрерывно, соединение не считается idle, и мы можем дать большой
    // суммарный CURLOPT_TIMEOUT как страховку, опираясь на детектор стола
    // (CURLOPT_LOW_SPEED_*) для реального обрыва.
    $body = [
        'model' => $model,
        'messages' => [
            ['role' => 'system', 'content' => $sysPrompt],
            ['role' => 'user',   'content' => $userPrompt],
        ],
        // Для thinking-моделей (legacy reasoner + V4-pro) поднимаем лимит:
        // модель кладёт chain-of-thought в отдельное поле reasoning_content,
        // но этот текст ТОЖЕ учитывается в max_tokens. На 3000 итоговый JSON
        // часто обрывается → JSON-парс падает → fallback тоже валится. С
        // добавлением 4 структурированных блоков (conversion_paths,
        // offers_market_analysis, cross_sell, epc_drops) объём ответа вырос;
        // на 5000 deepseek-chat стабильно ловил finish_reason=length и отдавал
        // обрезанный JSON → "Output is not a JSON object". Поднято до 8000
        // (deepseek-v4-flash cap ~8192) и до 12000 для thinking-моделей с
        // учётом скрытого reasoning_content.
        'max_tokens' => $isThinkingModel ? 12000 : 8000,
        'stream' => true,
        // Просим usage в финальном чанке — нужен для логов.
        'stream_options' => ['include_usage' => true],
    ];
    if (!$isLegacyReasoner) {
        $body['temperature'] = 0.2;
        // У провайдера есть JSON-mode — попросим, если поддерживается моделью.
        // V4-flash и V4-pro поддерживают; legacy reasoner — нет.
        $body['response_format'] = ['type' => 'json_object'];
    }

    // Аккумуляторы для парсинга SSE.
    $contentBuf   = '';   // итоговый assistant content (без reasoning_content)
    $rawBody      = '';   // полное «сырое» тело ответа — нужно для error-логов
                          // в случае HTTP != 200 (там тело — обычный JSON, а не SSE)
    $sseBuf       = '';   // незавершённое SSE-сообщение между chunks
    $finishReason = null;
    $usage        = null;
    $sawAnyDelta  = false;

    $writeFn = function ($ch, $chunk) use (
        &$contentBuf, &$rawBody, &$sseBuf,
        &$finishReason, &$usage, &$sawAnyDelta
    ) {
        $rawBody .= $chunk;
        $sseBuf  .= $chunk;
        // Нормализуем CRLF → LF на ВСЁМ буфере (а не только на $chunk),
        // чтобы корректно склеить случай, когда "\r\n" разрезано на границе
        // двух chunk-ов (curl может отдавать SSE короткими байтовыми порциями).
        // Реальные стримы используют и "\r\n\r\n", и "\n\n" — нам нужно
        // унифицированно искать только "\n\n".
        if (strpos($sseBuf, "\r") !== false) {
            $sseBuf = str_replace("\r\n", "\n", $sseBuf);
        }
        // SSE-события разделяются пустой строкой ("\n\n"). Обрабатываем все
        // полностью пришедшие события, остаток оставляем в буфере до следующего
        // chunk-а.
        while (($pos = strpos($sseBuf, "\n\n")) !== false) {
            $event  = substr($sseBuf, 0, $pos);
            $sseBuf = substr($sseBuf, $pos + 2);
            // Внутри события может быть несколько строк: data:, event:, id:, ...
            // Конкатенируем все data:-строки (по спецификации SSE).
            $dataLines = [];
            foreach (explode("\n", $event) as $line) {
                if (strncmp($line, 'data:', 5) !== 0) continue;
                $dataLines[] = ltrim(substr($line, 5));
            }
            if (!$dataLines) continue;
            $data = implode("\n", $dataLines);
            if ($data === '' || $data === '[DONE]') continue;
            $j = json_decode($data, true);
            if (!is_array($j)) continue;
            // usage может прийти отдельным финальным чанком (choices=[]).
            if (isset($j['usage']) && is_array($j['usage'])) {
                $usage = $j['usage'];
            }
            $choice = $j['choices'][0] ?? null;
            if (!is_array($choice)) continue;
            $delta = $choice['delta'] ?? null;
            if (is_array($delta) && isset($delta['content']) && is_string($delta['content'])) {
                $contentBuf .= $delta['content'];
                $sawAnyDelta = true;
            }
            // reasoning_content (chain-of-thought thinking-моделей: legacy
            // reasoner / V4-pro) НЕ кладём в итоговый ответ — это служебный
            // текст.
            if (isset($choice['finish_reason']) && $choice['finish_reason'] !== null) {
                $finishReason = $choice['finish_reason'];
            }
        }
        return strlen($chunk);
    };

    // Общий тайм-аут — страховка на самый худший случай. С учётом стриминга
    // нормальный ответ deepseek-v4-flash укладывается в 30-60с; thinking-
    // модели (V4-pro / legacy reasoner) — до ~2-3 минут. Реальный обрыв
    // «висящего» соединения детектится через CURLOPT_LOW_SPEED_TIME (нет
    // байт N секунд → curl сам разорвёт).
    $totalTimeout = $isThinkingModel ? 240 : 180;
    $stallSeconds = 60;

    $ch = curl_init();
    curl_setopt_array($ch, [
        CURLOPT_URL => AI_API_URL,
        CURLOPT_POST => true,
        // RETURNTRANSFER не нужен — данные обрабатываем через WRITEFUNCTION.
        CURLOPT_RETURNTRANSFER => false,
        CURLOPT_TIMEOUT => $totalTimeout,
        CURLOPT_CONNECTTIMEOUT => 15,
        CURLOPT_SSL_VERIFYPEER => true,
        CURLOPT_SSL_VERIFYHOST => 2,
        CURLOPT_HTTPHEADER => [
            'Content-Type: application/json',
            'Accept: text/event-stream',
            'Authorization: Bearer ' . aiEffectiveApiKey(),
        ],
        CURLOPT_POSTFIELDS    => json_encode($body, JSON_UNESCAPED_UNICODE),
        CURLOPT_WRITEFUNCTION => $writeFn,
        // Стол-детект: если за $stallSeconds к нам не приходит даже 1 байт —
        // считаем что провайдер «завис», и обрываем (curl вернёт ошибку 28).
        CURLOPT_LOW_SPEED_TIME  => $stallSeconds,
        CURLOPT_LOW_SPEED_LIMIT => 1,
        // Полностью убиваем любую буферизацию на стороне curl/прокси.
        CURLOPT_TCP_NODELAY => true,
    ]);
    aiLog('provider_request', [
        'model'           => $model,
        'is_reasoner'     => $isLegacyReasoner,
        'is_thinking'     => $isThinkingModel,
        'sys_prompt_len'  => strlen((string)$sysPrompt),
        'user_prompt_len' => strlen((string)$userPrompt),
        'max_tokens'      => $body['max_tokens'],
        'timeout_s'       => $totalTimeout,
        'stream'          => true,
        'stall_s'         => $stallSeconds,
    ]);
    $tStart    = microtime(true);
    $execOk    = curl_exec($ch);
    $code      = curl_getinfo($ch, CURLINFO_HTTP_CODE);
    $cerr      = curl_error($ch);
    $cerrno    = curl_errno($ch);
    curl_close($ch);
    $elapsedMs = (int)round((microtime(true) - $tStart) * 1000);

    if ($cerr) {
        // Если успели набрать контент до обрыва — попытаемся отдать его наверх,
        // дальше aiExtractJson / aiRepairTruncatedJson вытянут валидный JSON
        // из обрезанного потока. Это лучше, чем уронить весь анализ.
        if ($sawAnyDelta && $contentBuf !== '') {
            aiLog('provider_stream_partial', [
                'model'         => $model,
                'curl_errno'    => $cerrno,
                'curl_error'    => $cerr,
                'elapsed_ms'    => $elapsedMs,
                'content_len'   => strlen((string)$contentBuf),
                'finish_reason' => $finishReason,
            ]);
            return [
                'content'    => $contentBuf,
                'model'      => $model,
                'latency_ms' => $elapsedMs,
                'usage'      => is_array($usage) ? $usage : null,
            ];
        }
        error_log('AI provider CURL error: ' . $cerr);
        aiLog('provider_curl_error', [
            'model'      => $model,
            'curl_errno' => $cerrno,
            'curl_error' => $cerr,
            'elapsed_ms' => $elapsedMs,
        ]);
        return ['error' => 'transport'];
    }
    if ($code < 200 || $code >= 300) {
        // Для не-2xx тело — обычный JSON с ошибкой провайдера, его собрали
        // в $rawBody (наш writer молча принимал байты). Логируем сниппет.
        $snippet = substr((string)$rawBody, 0, 500);
        error_log("AI provider HTTP {$code}: " . $snippet);
        aiLog('provider_http_error', [
            'model'        => $model,
            'http_code'    => (int)$code,
            'elapsed_ms'   => $elapsedMs,
            'body_snippet' => $snippet,
        ]);
        return ['error' => "http_{$code}"];
    }
    if ($contentBuf === '') {
        aiLog('provider_empty_content', [
            'model'         => $model,
            'http_code'     => (int)$code,
            'elapsed_ms'    => $elapsedMs,
            'finish_reason' => $finishReason,
            'usage'         => $usage,
            'raw_snippet'   => substr((string)$rawBody, 0, 300),
        ]);
        return ['error' => 'empty_content'];
    }
    aiLog('provider_ok', [
        'model'         => $model,
        'http_code'     => (int)$code,
        'elapsed_ms'    => $elapsedMs,
        'content_len'   => strlen((string)$contentBuf),
        'finish_reason' => $finishReason,
        'usage'         => $usage,
    ]);
    return [
        'content'    => $contentBuf,
        'model'      => $model,
        'latency_ms' => $elapsedMs,
        'usage'      => is_array($usage) ? $usage : null,
    ];
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
    // Попытка восстановить ОБРЕЗАННЫЙ JSON (finish_reason=length).
    // Идея: проходим строку посимвольно, отслеживая глубину {}/[] и состояние
    // строки/escape. На каждом моменте, когда мы не внутри строки и текущий
    // символ — допустимое окончание значения (}, ], число/литерал), запоминаем
    // позицию. По окончании прохода обрезаем по последней безопасной позиции и
    // дозакрываем оставшиеся открытые контейнеры. Лучше получить частичный, но
    // валидный JSON с обязательными полями, чем уронить весь ответ.
    $repaired = aiRepairTruncatedJson($t);
    if ($repaired !== null) {
        $obj = json_decode($repaired, true);
        if (is_array($obj)) return $obj;
    }
    return null;
}

/**
 * Пытается восстановить валидный JSON из обрезанного по max_tokens ответа.
 * Возвращает строку с восстановленным JSON или null, если восстановить нельзя.
 *
 * Алгоритм:
 *   1. Находим начало JSON-объекта ('{').
 *   2. Идём по символам, отслеживая стек открытых '{'/'[' и состояние строки.
 *   3. Запоминаем позицию последней безопасной "точки разреза" — индекс ПОСЛЕ
 *      символа, на котором текущая пара ключ:значение была корректно закрыта
 *      (запятая или закрывающая скобка вне строки).
 *   4. Обрезаем строку до этой точки, удаляем висящую запятую и дозакрываем
 *      оставшиеся открытые '{'/'[' в обратном порядке.
 */
function aiRepairTruncatedJson($text) {
    $start = strpos($text, '{');
    if ($start === false) return null;
    $s = substr($text, $start);
    $len = strlen($s);
    $stack = [];           // стек открытых контейнеров: '{' или '['
    $inString = false;
    $escape = false;
    $safeCut = -1;         // индекс ПОСЛЕ безопасного символа (запятая/закр. скобка верхнего уровня значения)
    $safeStack = [];       // снапшот стека на момент safeCut
    for ($i = 0; $i < $len; $i++) {
        $ch = $s[$i];
        if ($inString) {
            if ($escape) { $escape = false; continue; }
            if ($ch === '\\') { $escape = true; continue; }
            if ($ch === '"') { $inString = false; }
            continue;
        }
        if ($ch === '"') { $inString = true; continue; }
        if ($ch === '{' || $ch === '[') { $stack[] = $ch; continue; }
        if ($ch === '}' || $ch === ']') {
            if (empty($stack)) break;
            array_pop($stack);
            $safeCut   = $i + 1;
            $safeStack = $stack;
            continue;
        }
        if ($ch === ',') {
            $safeCut   = $i; // отрежем ДО запятой, потом сами дозакроем контейнеры
            $safeStack = $stack;
            continue;
        }
    }
    if ($safeCut === -1) return null;
    $head = substr($s, 0, $safeCut);
    // Уберём возможный висящий "ключ": часть после последней запятой/двоеточия,
    // если она не выглядит как завершённое значение (страховка от обрыва внутри
    // числа/литерала true/false/null).
    $head = rtrim($head);
    if ($head === '' || $head === '{') return null;
    // Снимем висящую запятую в конце.
    $head = rtrim($head, ", \t\n\r");
    // Закрываем оставшиеся контейнеры в обратном порядке.
    $tail = '';
    for ($i = count($safeStack) - 1; $i >= 0; $i--) {
        $tail .= ($safeStack[$i] === '{') ? '}' : ']';
    }
    return $head . $tail;
}

/** Достаёт кэшированный AI-результат за период [from..to] или null. */
function aiCacheGet($db, $from, $to) {
    $key = $from . '|' . $to;
    $stmt = $db->prepare('SELECT period_from, period_to, result_json, created_at,
        payload_hash, prompt_version, model_used, latency_ms,
        prompt_tokens, completion_tokens, total_tokens
        FROM ai_analysis_cache WHERE period_key = :k LIMIT 1');
    $stmt->bindValue(':k', $key, SQLITE3_TEXT);
    $res = $stmt->execute();
    $row = $res->fetchArray(SQLITE3_ASSOC);
    if (!$row) return null;
    $data = json_decode((string)$row['result_json'], true);
    if (!is_array($data)) return null;
    return [
        'period_from'       => (string)$row['period_from'],
        'period_to'         => (string)$row['period_to'],
        'created_at'        => (int)$row['created_at'],
        'data'              => $data,
        'payload_hash'      => isset($row['payload_hash']) ? (string)$row['payload_hash'] : '',
        'prompt_version'    => isset($row['prompt_version']) ? (string)$row['prompt_version'] : '',
        'model_used'        => isset($row['model_used']) ? (string)$row['model_used'] : '',
        'latency_ms'        => isset($row['latency_ms']) ? (int)$row['latency_ms'] : 0,
        'prompt_tokens'     => isset($row['prompt_tokens']) ? (int)$row['prompt_tokens'] : 0,
        'completion_tokens' => isset($row['completion_tokens']) ? (int)$row['completion_tokens'] : 0,
        'total_tokens'      => isset($row['total_tokens']) ? (int)$row['total_tokens'] : 0,
    ];
}

/** Сохраняет (UPSERT) результат AI-анализа за период с телеметрией. */
function aiCachePut($db, $from, $to, $data, $createdAt, $meta = []) {
    $key = $from . '|' . $to;
    $stmt = $db->prepare('INSERT INTO ai_analysis_cache
        (period_key, period_from, period_to, result_json, created_at,
         payload_hash, prompt_version, model_used, latency_ms,
         prompt_tokens, completion_tokens, total_tokens,
         refine_rounds, hallu_offer_count, numeric_check_failures,
         empty_evidence_count, low_quality_recs, duplicate_recs_dropped,
         filter_drops, anomaly_coverage_missed)
        VALUES (:k, :f, :t, :j, :c, :ph, :pv, :mu, :lm, :pt, :ct, :tt,
                :rr, :hc, :nc, :ec, :lq, :dd, :fd, :ac)
        ON CONFLICT(period_key) DO UPDATE SET
            period_from              = excluded.period_from,
            period_to                = excluded.period_to,
            result_json              = excluded.result_json,
            created_at               = excluded.created_at,
            payload_hash             = excluded.payload_hash,
            prompt_version           = excluded.prompt_version,
            model_used               = excluded.model_used,
            latency_ms               = excluded.latency_ms,
            prompt_tokens            = excluded.prompt_tokens,
            completion_tokens        = excluded.completion_tokens,
            total_tokens             = excluded.total_tokens,
            refine_rounds            = excluded.refine_rounds,
            hallu_offer_count        = excluded.hallu_offer_count,
            numeric_check_failures   = excluded.numeric_check_failures,
            empty_evidence_count     = excluded.empty_evidence_count,
            low_quality_recs         = excluded.low_quality_recs,
            duplicate_recs_dropped   = excluded.duplicate_recs_dropped,
            filter_drops             = excluded.filter_drops,
            anomaly_coverage_missed  = excluded.anomaly_coverage_missed');
    $stmt->bindValue(':k',  $key, SQLITE3_TEXT);
    $stmt->bindValue(':f',  $from, SQLITE3_TEXT);
    $stmt->bindValue(':t',  $to, SQLITE3_TEXT);
    $stmt->bindValue(':j',  json_encode($data, JSON_UNESCAPED_UNICODE), SQLITE3_TEXT);
    $stmt->bindValue(':c',  (int)$createdAt, SQLITE3_INTEGER);
    $stmt->bindValue(':ph', (string)($meta['payload_hash']      ?? ''), SQLITE3_TEXT);
    $stmt->bindValue(':pv', (string)($meta['prompt_version']    ?? ''), SQLITE3_TEXT);
    $stmt->bindValue(':mu', (string)($meta['model_used']        ?? ''), SQLITE3_TEXT);
    $stmt->bindValue(':lm', (int)   ($meta['latency_ms']        ?? 0),  SQLITE3_INTEGER);
    $stmt->bindValue(':pt', (int)   ($meta['prompt_tokens']     ?? 0),  SQLITE3_INTEGER);
    $stmt->bindValue(':ct', (int)   ($meta['completion_tokens'] ?? 0),  SQLITE3_INTEGER);
    $stmt->bindValue(':tt', (int)   ($meta['total_tokens']      ?? 0),  SQLITE3_INTEGER);
    $stmt->bindValue(':rr', (int)   ($meta['refine_rounds']            ?? 0), SQLITE3_INTEGER);
    $stmt->bindValue(':hc', (int)   ($meta['hallu_offer_count']        ?? 0), SQLITE3_INTEGER);
    $stmt->bindValue(':nc', (int)   ($meta['numeric_check_failures']   ?? 0), SQLITE3_INTEGER);
    $stmt->bindValue(':ec', (int)   ($meta['empty_evidence_count']     ?? 0), SQLITE3_INTEGER);
    $stmt->bindValue(':lq', (int)   ($meta['low_quality_recs']         ?? 0), SQLITE3_INTEGER);
    $stmt->bindValue(':dd', (int)   ($meta['duplicate_recs_dropped']   ?? 0), SQLITE3_INTEGER);
    $stmt->bindValue(':fd', (int)   ($meta['filter_drops']             ?? 0), SQLITE3_INTEGER);
    $stmt->bindValue(':ac', (int)   ($meta['anomaly_coverage_missed']  ?? 0), SQLITE3_INTEGER);
    $stmt->execute();
}

/**
 * Считает sha1 канонического (отсортированного) представления payload.
 * Включает AI_PROMPT_VERSION, чтобы смена схемы автоматически инвалидировала
 * совпадение хеша. Используется для детекции stale-кэша после обновления
 * исходных данных через cron_update.php.
 */
function aiPayloadHash($payload) {
    $canon = aiCanonicalize($payload);
    $json = json_encode($canon, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
    return sha1(AI_PROMPT_VERSION . '|' . $json);
}

/** Рекурсивно сортирует ключи объектов — нужно для воспроизводимого hash. */
function aiCanonicalize($v) {
    if (is_array($v)) {
        // Если массив ассоциативный — сортируем по ключам.
        $isAssoc = array_keys($v) !== range(0, count($v) - 1);
        if ($isAssoc) {
            ksort($v);
            foreach ($v as $k => $vv) $v[$k] = aiCanonicalize($vv);
        } else {
            foreach ($v as $i => $vv) $v[$i] = aiCanonicalize($vv);
        }
    }
    return $v;
}

/**
 * Агрегирует ai.log за последние N секунд.
 * Возвращает счётчики событий, success rate, среднюю latency успешных
 * вызовов провайдера, средние/суммарные токены, число фоллбеков, refine-ов
 * и срабатываний no-hallucination детектора.
 *
 * Файл читается ВЕСЬ построчно (он ограничен AI_LOG_MAX_BYTES = 2 МБ),
 * это десятки тысяч строк максимум — для diag-а допустимо.
 */
function aiAggregateLogStats($windowSec = 86400) {
    $stats = [
        'window_sec'          => (int)$windowSec,
        'events_total'        => 0,
        'provider_ok'         => 0,
        'provider_http_error' => 0,
        'provider_curl_error' => 0,
        'provider_empty'      => 0,
        'analyze_success'     => 0,
        'analyze_error'       => 0,
        'cache_hit'           => 0,
        'cache_stale'         => 0,
        'fallback'            => 0,
        'refine_schema'       => 0,
        'hallucination'       => 0,
        'hallu_refine_ok'     => 0,
        'avg_latency_ms'      => 0,
        'p95_latency_ms'      => 0,
        'sum_prompt_tokens'   => 0,
        'sum_completion_tokens'=> 0,
        'sum_total_tokens'    => 0,
    ];
    if (!is_file(AI_LOG_FILE)) return $stats;
    $fp = @fopen(AI_LOG_FILE, 'rb');
    if (!$fp) return $stats;
    $cutoff = time() - $windowSec;
    $latencies = [];
    while (($line = fgets($fp)) !== false) {
        $line = trim($line);
        if ($line === '') continue;
        $j = json_decode($line, true);
        if (!is_array($j)) continue;
        // Поле ts может быть ISO-строкой ("2026-01-02T03:04:05Z") или числом.
        $ts = 0;
        if (isset($j['ts'])) {
            if (is_numeric($j['ts'])) $ts = (int)$j['ts'];
            else { $t = strtotime((string)$j['ts']); if ($t) $ts = $t; }
        }
        if ($ts && $ts < $cutoff) continue;
        $stats['events_total']++;
        $event = (string)($j['event'] ?? '');
        $ctx   = is_array($j['context'] ?? null) ? $j['context'] : [];
        switch ($event) {
            case 'provider_ok':
                $stats['provider_ok']++;
                if (isset($ctx['elapsed_ms'])) $latencies[] = (int)$ctx['elapsed_ms'];
                if (isset($ctx['usage']) && is_array($ctx['usage'])) {
                    $stats['sum_prompt_tokens']     += (int)($ctx['usage']['prompt_tokens']     ?? 0);
                    $stats['sum_completion_tokens'] += (int)($ctx['usage']['completion_tokens'] ?? 0);
                    $stats['sum_total_tokens']      += (int)($ctx['usage']['total_tokens']      ?? 0);
                }
                break;
            case 'provider_http_error':    $stats['provider_http_error']++; break;
            case 'provider_curl_error':    $stats['provider_curl_error']++; break;
            case 'provider_empty_content': $stats['provider_empty']++;      break;
            case 'ai_analyze_success':     $stats['analyze_success']++;     break;
            case 'ai_analyze_error':       $stats['analyze_error']++;       break;
            case 'ai_analyze_cache_hit':   $stats['cache_hit']++;           break;
            case 'ai_analyze_cache_stale': $stats['cache_stale']++;         break;
            case 'structured_fallback':    $stats['fallback']++;            break;
            case 'structured_validation_failed': $stats['refine_schema']++; break;
            case 'hallucination_detected': $stats['hallucination']++;       break;
            case 'hallucination_refine_ok':$stats['hallu_refine_ok']++;     break;
        }
    }
    fclose($fp);
    if (!empty($latencies)) {
        sort($latencies);
        $sum = array_sum($latencies);
        $stats['avg_latency_ms'] = (int)round($sum / count($latencies));
        $idx = (int)floor(0.95 * (count($latencies) - 1));
        $stats['p95_latency_ms'] = (int)$latencies[$idx];
    }
    $okPlusErr = $stats['provider_ok'] + $stats['provider_http_error']
                + $stats['provider_curl_error'] + $stats['provider_empty'];
    $stats['provider_success_rate_pct'] = $okPlusErr > 0
        ? round($stats['provider_ok'] / $okPlusErr * 100, 1)
        : null;
    return $stats;
}

/**
 * Backtest точности AI-прогнозов (точка роста №5c).
 *
 * Идея: при каждом успешном ai_analyze в кэш записывается JSON с
 * forecast.period_7d / period_30d. Этот хелпер проходит по всем кэш-записям,
 * для тех, у кого 7-дневный/30-дневный горизонт уже истёк, считает фактические
 * revenue/clicks/EPC из daily_stats и сохраняет MAPE в ai_forecast_accuracy.
 *
 * Возвращает счётчики: сколько строк проверено, сколько добавлено новых,
 * сколько пропущено (нет факта / уже посчитано / нет прогноза).
 */
function aiBacktestRun($db) {
    $today = date('Y-m-d');
    $report = [
        'evaluated'      => 0,
        'inserted'       => 0,
        'skipped_no_fact'=> 0,
        'skipped_done'   => 0,
        'skipped_no_fc'  => 0,
        'errors'         => 0,
    ];
    $rows = [];
    $res = @$db->query('SELECT period_to, result_json, prompt_version, model_used
        FROM ai_analysis_cache
        WHERE result_json IS NOT NULL AND result_json != ""
        ORDER BY period_to DESC LIMIT 200');
    if ($res) {
        while ($r = $res->fetchArray(SQLITE3_ASSOC)) $rows[] = $r;
    }
    foreach ($rows as $row) {
        $report['evaluated']++;
        $baselineDate = (string)$row['period_to'];
        if ($baselineDate === '') { $report['errors']++; continue; }
        $data = json_decode((string)$row['result_json'], true);
        if (!is_array($data) || !isset($data['forecast']) || !is_array($data['forecast'])) {
            $report['skipped_no_fc']++;
            continue;
        }
        foreach (['7d' => 7, '30d' => 30] as $horizon => $days) {
            $key = 'period_' . $horizon;
            if (!isset($data['forecast'][$key]) || !is_array($data['forecast'][$key])) continue;
            $fc = $data['forecast'][$key];
            $targetFrom = date('Y-m-d', strtotime($baselineDate . ' +1 day'));
            $targetTo   = date('Y-m-d', strtotime($baselineDate . ' +' . $days . ' day'));
            // Если горизонт ещё не истёк — пропускаем.
            if ($targetTo >= $today) { $report['skipped_no_fact']++; continue; }
            // Уже посчитано? — prepared statement для согласованности с
            // остальным кодом (хотя оба значения берём из row базы, не от юзера).
            $stmtCheck = $db->prepare('SELECT 1 FROM ai_forecast_accuracy
                WHERE baseline_date = :bd AND horizon = :h');
            $stmtCheck->bindValue(':bd', $baselineDate, SQLITE3_TEXT);
            $stmtCheck->bindValue(':h',  $horizon,      SQLITE3_TEXT);
            $exists = $stmtCheck->execute()->fetchArray(SQLITE3_NUM);
            $stmtCheck->close();
            if ($exists) { $report['skipped_done']++; continue; }
            // Считаем факт за горизонт прогноза — тоже через prepared statement.
            $stmtFact = $db->prepare('SELECT
                    COALESCE(SUM(revenue),0)     AS rev,
                    COALESCE(SUM(raw_clicks),0)  AS clk
                FROM daily_stats
                WHERE date >= :tf AND date <= :tt');
            $stmtFact->bindValue(':tf', $targetFrom, SQLITE3_TEXT);
            $stmtFact->bindValue(':tt', $targetTo,   SQLITE3_TEXT);
            $factRes = $stmtFact->execute();
            $fact = $factRes ? $factRes->fetchArray(SQLITE3_ASSOC) : null;
            $stmtFact->close();
            if (!is_array($fact) || (float)$fact['rev'] <= 0) {
                $report['skipped_no_fact']++;
                continue;
            }
            $actRev = (float)$fact['rev'];
            $actClk = (float)$fact['clk'];
            $actEpc = $actClk > 0 ? $actRev / $actClk : 0;
            $fcRev  = isset($fc['revenue'])  ? (float)$fc['revenue']  : 0;
            $fcClk  = isset($fc['clicks'])   ? (float)$fc['clicks']   : 0;
            $fcEpc  = isset($fc['epc'])      ? (float)$fc['epc']      : 0;
            $mape = function ($f, $a) {
                if ($a == 0) return null;
                return round(abs($f - $a) / abs($a) * 100, 1);
            };
            $stmt = $db->prepare('INSERT OR IGNORE INTO ai_forecast_accuracy
                (baseline_date, horizon, target_from, target_to,
                 forecast_revenue, forecast_clicks, forecast_epc,
                 actual_revenue, actual_clicks, actual_epc,
                 mape_revenue, mape_clicks, mape_epc,
                 prompt_version, model_used, created_at)
                VALUES (:bd, :h, :tf, :tt,
                        :fr, :fc, :fe,
                        :ar, :ac, :ae,
                        :mr, :mc, :me,
                        :pv, :mu, :ts)');
            $stmt->bindValue(':bd', $baselineDate, SQLITE3_TEXT);
            $stmt->bindValue(':h',  $horizon,      SQLITE3_TEXT);
            $stmt->bindValue(':tf', $targetFrom,   SQLITE3_TEXT);
            $stmt->bindValue(':tt', $targetTo,     SQLITE3_TEXT);
            $stmt->bindValue(':fr', $fcRev,        SQLITE3_FLOAT);
            $stmt->bindValue(':fc', $fcClk,        SQLITE3_FLOAT);
            $stmt->bindValue(':fe', $fcEpc,        SQLITE3_FLOAT);
            $stmt->bindValue(':ar', $actRev,       SQLITE3_FLOAT);
            $stmt->bindValue(':ac', $actClk,       SQLITE3_FLOAT);
            $stmt->bindValue(':ae', $actEpc,       SQLITE3_FLOAT);
            $mr = $mape($fcRev, $actRev); $stmt->bindValue(':mr', $mr, $mr === null ? SQLITE3_NULL : SQLITE3_FLOAT);
            $mc = $mape($fcClk, $actClk); $stmt->bindValue(':mc', $mc, $mc === null ? SQLITE3_NULL : SQLITE3_FLOAT);
            $me = $mape($fcEpc, $actEpc); $stmt->bindValue(':me', $me, $me === null ? SQLITE3_NULL : SQLITE3_FLOAT);
            $stmt->bindValue(':pv', (string)($row['prompt_version'] ?? ''), SQLITE3_TEXT);
            $stmt->bindValue(':mu', (string)($row['model_used'] ?? ''),     SQLITE3_TEXT);
            $stmt->bindValue(':ts', time(),       SQLITE3_INTEGER);
            if ($stmt->execute()) $report['inserted']++;
            $stmt->reset();
        }
    }
    return $report;
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
    $res = @$db->query('SELECT offer_id, name, market_epc, market_cr, market_ar, your_epc, your_cr, updated_at FROM offers_cache ORDER BY market_epc DESC');
    $rows = [];
    if ($res instanceof SQLite3Result) {
        while ($row = $res->fetchArray(SQLITE3_ASSOC)) $rows[] = $row;
    } else {
        error_log('get_offers_market: query failed: ' . $db->lastErrorMsg());
    }
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
    $res = @$db->query('SELECT key, value FROM app_settings');
    $out = [];
    if ($res instanceof SQLite3Result) {
        while ($row = $res->fetchArray(SQLITE3_ASSOC)) $out[$row['key']] = $row['value'];
    } else {
        error_log('get_settings: query failed: ' . $db->lastErrorMsg());
    }
    // Маскируем чувствительные значения, оставляя признак "задано".
    $masked = $out;
    if (!empty($masked['tg_bot_token'])) $masked['tg_bot_token'] = '***SET***';
    if (!empty($masked['ai_api_key']))   $masked['ai_api_key']   = '***SET***';
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
        // Не перезаписываем секреты маркером "***SET***".
        if ($v === '***SET***') continue;
        // API-ключи часто вставляют с пробелами/переводом строки и невидимыми
        // символами (U+00A0, U+200B) — чистим, иначе DeepSeek вернёт 401 из-за
        // лишних символов в Authorization. Ключ DeepSeek — печатаемый ASCII,
        // поэтому полностью выбрасываем непечатаемые символы. Telegram-токен
        // тоже печатаемый ASCII, но достаточно trim() (двоеточие в нём значимо).
        if ($k === 'ai_api_key' && is_string($v)) {
            $v = aiSanitizeApiKey($v);
        } elseif ($k === 'tg_bot_token' && is_string($v)) {
            $v = trim($v);
        }
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
//
// Кэширование: успешные результаты сохраняются в `ai_analysis_cache` по ключу
// `period_from|period_to`. По умолчанию `ai_analyze` сначала отдаёт кэш, если
// тот существует. Для принудительного пересчёта (кнопка «Обновить») передаём
// query-параметр `force=1`.
if ($action === 'ai_get_cached') {
    // Лёгкий GET-эндпоинт, чтобы фронт мог восстановить ранее сохранённый
    // отчёт после перезагрузки страницы без затрат на провайдера.
    // Опциональный параметр payload_hash — если передан, мы сравним его с
    // хешем, который был в момент кэширования. При несовпадении вернём
    // флаг stale=true, но сам отчёт всё равно отдадим — пользователь увидит
    // последний доступный анализ + плашку «исходные данные могли обновиться».
    $from = (string)($_GET['from'] ?? '');
    $to   = (string)($_GET['to'] ?? '');
    $reqHash = (string)($_GET['payload_hash'] ?? '');
    if ($from === '' || $to === '') {
        echo json_encode(['status' => 'error', 'error' => 'from and to required']);
        exit;
    }
    $row = aiCacheGet($db, $from, $to);
    if (!$row) {
        echo json_encode(['status' => 'success', 'cached' => false]);
        exit;
    }
    $stale = ($reqHash !== '' && $row['payload_hash'] !== '' && $reqHash !== $row['payload_hash']);
    echo json_encode([
        'status'         => 'success',
        'cached'         => true,
        'data'           => $row['data'],
        'period'         => ['from' => $row['period_from'], 'to' => $row['period_to']],
        'created_at'     => $row['created_at'],
        'payload_hash'   => $row['payload_hash'],
        'prompt_version' => $row['prompt_version'],
        'stale'          => $stale,
        'meta' => [
            'model_used'        => $row['model_used'],
            'latency_ms'        => $row['latency_ms'],
            'prompt_tokens'     => $row['prompt_tokens'],
            'completion_tokens' => $row['completion_tokens'],
            'total_tokens'      => $row['total_tokens'],
        ],
    ]);
    exit;
}
if ($action === 'ai_backtest_run') {
    // Запуск бэктеста точности AI-прогнозов. Идемпотентен: для каждой пары
    // (baseline_date, horizon) считается ровно один раз. Безопасно дёргать
    // по cron (вызывается из cron_update.php).
    @set_time_limit(120);
    $report = aiBacktestRun($db);
    aiLog('ai_backtest_run', $report);
    echo json_encode(['status' => 'success', 'report' => $report]);
    exit;
}
if ($action === 'ai_forecast_accuracy') {
    // Возвращает агрегированные метрики MAPE и последние 30 строк сравнения
    // forecast vs actual — для админ-блока в UI.
    $rows = [];
    $res = @$db->query('SELECT baseline_date, horizon, target_from, target_to,
            forecast_revenue, forecast_clicks, forecast_epc,
            actual_revenue, actual_clicks, actual_epc,
            mape_revenue, mape_clicks, mape_epc,
            prompt_version, model_used, created_at
        FROM ai_forecast_accuracy
        ORDER BY created_at DESC LIMIT 30');
    if ($res) {
        while ($r = $res->fetchArray(SQLITE3_ASSOC)) $rows[] = $r;
    }
    $aggR = @$db->querySingle('SELECT COUNT(*) AS cnt,
        AVG(mape_revenue) AS mr, AVG(mape_clicks) AS mc, AVG(mape_epc) AS me
        FROM ai_forecast_accuracy', true);
    $agg = is_array($aggR) ? [
        'evaluated'    => (int)($aggR['cnt'] ?? 0),
        'mape_revenue' => $aggR['mr'] !== null ? round((float)$aggR['mr'], 1) : null,
        'mape_clicks'  => $aggR['mc'] !== null ? round((float)$aggR['mc'], 1) : null,
        'mape_epc'     => $aggR['me'] !== null ? round((float)$aggR['me'], 1) : null,
    ] : ['evaluated' => 0];
    // Отдельно — по горизонтам.
    $byHorizon = [];
    $resH = @$db->query('SELECT horizon, COUNT(*) AS cnt,
        AVG(mape_revenue) AS mr, AVG(mape_clicks) AS mc, AVG(mape_epc) AS me
        FROM ai_forecast_accuracy GROUP BY horizon');
    if ($resH) {
        while ($r = $resH->fetchArray(SQLITE3_ASSOC)) {
            $byHorizon[(string)$r['horizon']] = [
                'evaluated'    => (int)$r['cnt'],
                'mape_revenue' => $r['mr'] !== null ? round((float)$r['mr'], 1) : null,
                'mape_clicks'  => $r['mc'] !== null ? round((float)$r['mc'], 1) : null,
                'mape_epc'     => $r['me'] !== null ? round((float)$r['me'], 1) : null,
            ];
        }
    }
    echo json_encode([
        'status'     => 'success',
        'aggregate'  => $agg,
        'by_horizon' => $byHorizon,
        'recent'     => $rows,
    ], JSON_UNESCAPED_UNICODE);
    exit;
}
if ($action === 'ai_log') {
    // Безопасный просмотр последних N строк AI-лога. Файловый лог содержит
    // только метаданные (HTTP-коды, длительности, превью), без API-ключа и
    // полных промптов — поэтому отдавать его наружу безопасно. Лимит
    // нужен, чтобы один запрос не вернул весь многомегабайтный файл.
    $lines = (int)($_GET['lines'] ?? 200);
    if ($lines < 1) $lines = 1;
    if ($lines > 2000) $lines = 2000;

    if (!is_file(AI_LOG_FILE)) {
        echo json_encode([
            'status'    => 'success',
            'path'      => AI_LOG_FILE,
            'exists'    => false,
            'size_bytes'=> 0,
            'lines'     => [],
            'hint'      => 'Лог пуст. Сделайте запрос ai_diag или ai_analyze, чтобы появились записи.',
        ]);
        exit;
    }
    // Читаем построчно с конца, чтобы не грузить весь файл в память.
    $tail = [];
    $fp = @fopen(AI_LOG_FILE, 'rb');
    if ($fp) {
        $bufSize = 8192;
        $stat = fstat($fp);
        $size = $stat['size'] ?? 0;
        $pos = $size;
        $leftover = '';
        while ($pos > 0 && count($tail) < $lines) {
            $read = ($pos >= $bufSize) ? $bufSize : $pos;
            $pos -= $read;
            fseek($fp, $pos);
            $chunk = fread($fp, $read) . $leftover;
            $parts = explode("\n", $chunk);
            $leftover = array_shift($parts); // может быть неполной строкой
            // Добавляем с конца, в обратном порядке.
            for ($i = count($parts) - 1; $i >= 0; $i--) {
                if ($parts[$i] === '') continue;
                $tail[] = $parts[$i];
                if (count($tail) >= $lines) break;
            }
        }
        if ($pos === 0 && $leftover !== '' && count($tail) < $lines) {
            $tail[] = $leftover;
        }
        fclose($fp);
        $tail = array_reverse($tail);
    }
    echo json_encode([
        'status'     => 'success',
        'path'       => AI_LOG_FILE,
        'exists'     => true,
        'size_bytes' => (int)@filesize(AI_LOG_FILE),
        'lines'      => $tail,
    ], JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
    exit;
}
if ($action === 'ai_log_clear') {
    // Сброс файлового AI-лога. Не трогаем php_errors.log.
    $existed = is_file(AI_LOG_FILE);
    if ($existed) @unlink(AI_LOG_FILE);
    aiLog('ai_log_cleared', ['existed' => $existed]);
    echo json_encode(['status' => 'success', 'cleared' => $existed]);
    exit;
}
if ($action === 'ai_diag') {
    // Безопасная диагностика AI-интеграции. НЕ раскрывает имя провайдера,
    // полный ключ и URL клиенту — только факт наличия и санитарные данные.
    // Полезно, когда фронт получает обезличенное "AI service is temporarily
    // unavailable" и непонятно, что именно сломалось: ключ, конфиг, сеть,
    // баланс или провайдер.
    @set_time_limit(30);
    aiLog('ai_diag_start', []);
    global $AI_CONFIG_LOAD_STATUS;
    $effKey = aiEffectiveApiKey();
    $diag = [
        'status' => 'success',
        'config_file_present' => is_file(__DIR__ . '/ai_config.php'),
        'config_file_readable' => is_readable(__DIR__ . '/ai_config.php'),
        'config_load_status' => $AI_CONFIG_LOAD_STATUS ?? 'unknown', // absent|loaded|unreadable
        'api_key_present' => ($effKey !== ''),
        'api_key_source' => aiApiKeySource(), // config|db|none
        'api_key_length_bucket' => $effKey === '' ? 'empty' : (strlen($effKey) < 20 ? 'short' : 'ok'),
        'api_key_prefix' => $effKey === '' ? '' : substr($effKey, 0, 4) . '…',
        'env_key_set' => (getenv('AI_API_KEY') !== false && getenv('AI_API_KEY') !== ''),
        'php_version' => PHP_VERSION,
        'curl_loaded' => function_exists('curl_init'),
        'openssl_loaded' => extension_loaded('openssl'),
        'sqlite_loaded' => extension_loaded('sqlite3'),
        'error_log_path' => __DIR__ . '/php_errors.log',
        'error_log_writable' => is_writable(__DIR__) || is_writable(__DIR__ . '/php_errors.log'),
        'expected_config_path' => __DIR__ . '/ai_config.php',
        // Информация про файловый AI-лог (см. action=ai_log).
        'ai_log_path'      => AI_LOG_FILE,
        'ai_log_exists'    => is_file(AI_LOG_FILE),
        'ai_log_writable'  => is_writable(AI_LOG_FILE) || is_writable(__DIR__),
        'ai_log_size_bytes'=> is_file(AI_LOG_FILE) ? (int)@filesize(AI_LOG_FILE) : 0,
        'ai_log_view_url'  => '?action=ai_log&lines=200',
    ];

    if (!$diag['api_key_present']) {
        // Подскажем точную причину пустого ключа:
        if ($diag['config_load_status'] === 'absent') {
            $diag['hint'] = 'Ключ DeepSeek не задан. Самый простой способ — вписать его в интерфейсе '
                . '(вкладка «События» → блок «🤖 DeepSeek API-ключ» → Сохранить). '
                . 'Либо положите ai_config.php рядом с leads-proxy.php, либо задайте переменную окружения AI_API_KEY.';
        } elseif ($diag['config_load_status'] === 'unreadable') {
            $diag['hint'] = 'Файл ai_config.php найден, но веб-сервер не может его прочитать. Сделайте chmod 644 или впишите ключ через интерфейс.';
        } else {
            $diag['hint'] = 'Ключ пуст. Впишите его через интерфейс (блок «🤖 DeepSeek API-ключ») или проверьте строку define(\'AI_API_KEY\', \'sk-...\'); в ai_config.php.';
        }
        aiLog('ai_diag_no_key', [
            'config_load_status' => $diag['config_load_status'],
            'hint'               => $diag['hint'],
        ]);
        echo json_encode($diag);
        exit;
    }

    // Минимальный пробный запрос к провайдеру: 1 токен ответа, дешёвая модель.
    // Возвращаем HTTP-код и КОРОТКИЙ фрагмент ответа без чувствительных данных.
    $probeBody = [
        'model' => AI_MODEL,
        'messages' => [
            ['role' => 'user', 'content' => 'ping'],
        ],
        'max_tokens' => 1,
        'stream' => false,
    ];
    $ch = curl_init();
    curl_setopt_array($ch, [
        CURLOPT_URL => AI_API_URL,
        CURLOPT_POST => true,
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_TIMEOUT => 20,
        CURLOPT_CONNECTTIMEOUT => 10,
        CURLOPT_SSL_VERIFYPEER => true,
        CURLOPT_SSL_VERIFYHOST => 2,
        CURLOPT_HTTPHEADER => [
            'Content-Type: application/json',
            'Accept: application/json',
            'Authorization: Bearer ' . aiEffectiveApiKey(),
        ],
        CURLOPT_POSTFIELDS => json_encode($probeBody, JSON_UNESCAPED_UNICODE),
    ]);
    $t0 = microtime(true);
    $raw = curl_exec($ch);
    $diag['probe'] = [
        'http_code'    => (int)curl_getinfo($ch, CURLINFO_HTTP_CODE),
        'curl_error'   => curl_error($ch) ?: null,
        'curl_errno'   => curl_errno($ch),
        'elapsed_ms'   => (int)round((microtime(true) - $t0) * 1000),
        // Короткий безопасный фрагмент тела (provider может вернуть текст
        // ошибки вида "Insufficient Balance" / "Authentication Fails").
        'body_snippet' => is_string($raw) ? substr($raw, 0, 300) : null,
    ];
    curl_close($ch);

    // Подсказки по типичным кодам — чтобы пользователю не нужно было лезть в логи.
    $code = $diag['probe']['http_code'];
    if ($diag['probe']['curl_error']) {
        $diag['hint'] = 'CURL ошибка — вероятно, хостинг блокирует исходящие соединения к API провайдера или нет DNS/SSL. Проверьте, открыт ли исходящий 443 порт.';
    } elseif ($code === 401) {
        $diag['hint'] = 'HTTP 401 — ключ невалиден/отозван/опечатка. Проверьте AI_API_KEY (без пробелов, переводов строк и невидимых символов; пересохраните ключ через интерфейс).';
    } elseif ($code === 402) {
        $diag['hint'] = 'HTTP 402 — недостаточный баланс у провайдера. Пополните счёт.';
    } elseif ($code === 429) {
        $diag['hint'] = 'HTTP 429 — rate limit. Подождите и повторите.';
    } elseif ($code === 400) {
        $diag['hint'] = 'HTTP 400 — провайдер отклонил запрос (часто неверная модель). Проверьте AI_MODEL в ai_config.php.';
    } elseif ($code >= 500) {
        $diag['hint'] = 'HTTP ' . $code . ' — временный сбой провайдера, попробуйте позже.';
    } elseif ($code >= 200 && $code < 300) {
        $diag['hint'] = 'OK — провайдер отвечает. Если генерация всё равно падает, причина в валидации JSON-схемы ответа: смотрите php_errors.log.';
    } else {
        $diag['hint'] = 'Неожиданный HTTP-код ' . $code . '. Смотрите body_snippet и php_errors.log.';
    }

    aiLog('ai_diag_result', [
        'http_code'   => $diag['probe']['http_code'],
        'curl_errno'  => $diag['probe']['curl_errno'],
        'curl_error'  => $diag['probe']['curl_error'],
        'elapsed_ms'  => $diag['probe']['elapsed_ms'],
        'hint'        => $diag['hint'],
    ]);

    // ---- Агрегаты по ai.log за последние 24ч (точка роста №5b). ----
    // Проходимся хвостом по ai.log (JSON-line), считаем счётчики событий,
    // среднюю latency успешных вызовов и среднее число токенов. Без этих
    // метрик невозможно понять, окупается ли AI и не деградирует ли он.
    $diag['usage_24h'] = aiAggregateLogStats(86400);
    $diag['prompt_version'] = AI_PROMPT_VERSION;
    // ---- Агрегаты по ai_analysis_cache (текущий размер кэша). ----
    try {
        $cacheRow = @$db->querySingle(
            'SELECT COUNT(*) AS cnt,
                    COALESCE(SUM(total_tokens),0) AS total_tokens,
                    COALESCE(AVG(latency_ms),0)   AS avg_latency_ms
             FROM ai_analysis_cache', true);
        if (is_array($cacheRow)) {
            $diag['cache_stats'] = [
                'entries'        => (int)($cacheRow['cnt'] ?? 0),
                'total_tokens'   => (int)($cacheRow['total_tokens'] ?? 0),
                'avg_latency_ms' => (int)round((float)($cacheRow['avg_latency_ms'] ?? 0)),
            ];
        }
    } catch (Throwable $e) {
        $diag['cache_stats'] = ['error' => $e->getMessage()];
    }
    // ---- Агрегаты точности прогнозов (точка роста №5c). ----
    try {
        $accRow = @$db->querySingle(
            'SELECT COUNT(*) AS cnt,
                    AVG(mape_revenue) AS mape_rev,
                    AVG(mape_clicks)  AS mape_clk,
                    AVG(mape_epc)     AS mape_epc
             FROM ai_forecast_accuracy', true);
        if (is_array($accRow)) {
            $diag['forecast_accuracy'] = [
                'evaluated'    => (int)($accRow['cnt'] ?? 0),
                'mape_revenue' => $accRow['mape_rev'] !== null ? round((float)$accRow['mape_rev'], 1) : null,
                'mape_clicks'  => $accRow['mape_clk'] !== null ? round((float)$accRow['mape_clk'], 1) : null,
                'mape_epc'     => $accRow['mape_epc'] !== null ? round((float)$accRow['mape_epc'], 1) : null,
            ];
        }
    } catch (Throwable $e) {
        $diag['forecast_accuracy'] = ['error' => $e->getMessage()];
    }

    echo json_encode($diag);
    exit;
}
if ($action === 'ai_analyze') {
    // Поднимаем PHP-лимиты: AI-запрос может занять несколько минут,
    // дефолтные max_execution_time/memory_limit на shared-хостинге часто
    // ниже реальной длительности и приводят к фаталу → пустой/HTML ответ.
    // Со стримингом v4-flash ≤180с, fallback v4-pro ≤240с, refine v4-flash ≤180с —
    // в худшем случае суммарно до ~10 минут, поэтому даём 720с с запасом.
    @set_time_limit(720);
    @ini_set('memory_limit', '256M');
    // Просим nginx/Cloudflare НЕ буферизовать ответ — иначе reverse-proxy
    // может закрыть соединение по своему `proxy_read_timeout` раньше,
    // чем PHP успеет дописать тело, и клиент получит HTML 504.
    header('X-Accel-Buffering: no');
    header('Cache-Control: no-cache');
    // Сбрасываем уже накопленные хедеры в сокет до начала тяжёлого вызова.
    @ob_flush();
    @flush();

    $raw = file_get_contents('php://input');
    $payload = json_decode($raw, true);
    if (!is_array($payload)) {
        aiLog('ai_analyze_bad_payload', ['raw_len' => is_string($raw) ? strlen($raw) : 0]);
        echo json_encode(['status' => 'error', 'error' => 'Invalid payload']);
        exit;
    }

    // Достаём период из payload — по нему ключуется кэш.
    $periodFrom = '';
    $periodTo   = '';
    if (isset($payload['period']) && is_array($payload['period'])) {
        $periodFrom = (string)($payload['period']['from'] ?? '');
        $periodTo   = (string)($payload['period']['to']   ?? '');
    }
    $force = !empty($_GET['force']);
    aiLog('ai_analyze_start', [
        'period_from' => $periodFrom,
        'period_to'   => $periodTo,
        'force'       => $force,
        'payload_keys'=> array_keys($payload),
    ]);

    // Считаем хеш payload-а ДО тримминга — чтобы он был детерминирован
    // относительно того, что пришло от фронта (фронт шлёт тот же hash для
    // ai_get_cached). Хеш включает AI_PROMPT_VERSION, поэтому смена схемы
    // автоматически инвалидирует совпадение.
    $payloadHash = aiPayloadHash($payload);

    // Если не запрошен принудительный пересчёт и есть кэш с таким же hash —
    // отдаём его как fresh. Если hash отличается (данные обновились через
    // cron_update.php) — НЕ отдаём кэш молча, а идём к провайдеру за новым
    // ответом, чтобы пользователь не видел устаревший.
    if (!$force && $periodFrom !== '' && $periodTo !== '') {
        $cached = aiCacheGet($db, $periodFrom, $periodTo);
        if ($cached && $cached['payload_hash'] === $payloadHash) {
            aiLog('ai_analyze_cache_hit', [
                'period_from' => $cached['period_from'],
                'period_to'   => $cached['period_to'],
                'created_at'  => $cached['created_at'],
                'hash_match'  => true,
            ]);
            echo json_encode([
                'status'         => 'success',
                'data'           => $cached['data'],
                'cached'         => true,
                'stale'          => false,
                'period'         => ['from' => $cached['period_from'], 'to' => $cached['period_to']],
                'created_at'     => $cached['created_at'],
                'payload_hash'   => $cached['payload_hash'],
                'prompt_version' => $cached['prompt_version'],
                'meta' => [
                    'model_used'        => $cached['model_used'],
                    'latency_ms'        => $cached['latency_ms'],
                    'prompt_tokens'     => $cached['prompt_tokens'],
                    'completion_tokens' => $cached['completion_tokens'],
                    'total_tokens'      => $cached['total_tokens'],
                ],
            ]);
            exit;
        }
        if ($cached) {
            aiLog('ai_analyze_cache_stale', [
                'cached_hash'  => $cached['payload_hash'],
                'request_hash' => $payloadHash,
            ]);
        }
    }

    if (aiEffectiveApiKey() === '') {
        // Ключ не сконфигурирован на сервере — отдаём JSON, а не падаем в HTML.
        error_log('AI: API key is empty; refusing to call provider');
        aiLog('ai_analyze_no_key', []);
        echo json_encode(['status' => 'error', 'error' => 'Ключ DeepSeek не задан. Откройте вкладку «События» → «🤖 DeepSeek API-ключ» и сохраните ключ.']);
        exit;
    }
    // Жёстко ограничиваем размер payload, чтобы не раздувать prompt и стоимость.
    $payload = aiTrimPayload($payload);

    // Обогащение payload-а перед отправкой модели:
    //   • allowed_entities  — белый список офферов/площадок/sub1, считается из
    //     других блоков payload-а; system-prompt требует выбирать ТОЛЬКО оттуда.
    //   • allowed_action_types — закрытый список action_type (см. AI_ACTION_TYPES_JSON).
    //   • control            — нормализованные настройки (профиль, пороги,
    //     фильтры, KPI, detail_level, previous_feedback). Если фронт ничего не
    //     прислал — будут безопасные дефолты (profile=balanced и т.п.).
    // ВАЖНО: эти три поля НЕ влияют на $payloadHash — он считался раньше по
    // payload-у в том виде, в каком его прислал фронт. Это сделано осознанно:
    //   1) фронт не знает реализацию aiCollectAllowedNames, ему сложнее
    //      повторить байт-в-байт ту же канонизацию;
    //   2) `control` фронт уже шлёт сам, поэтому смена профиля/порогов
    //      автоматически меняет hash и инвалидирует кэш — что и требовалось.
    // Иначе говоря, control — это часть ВХОДА (от фронта), а allowed_entities
    // и allowed_action_types — это ДЕТЕРМИНИРОВАННЫЕ ПРОИЗВОДНЫЕ от входа,
    // их можно не включать в hash.
    $allowedEntities = aiCollectAllowedNames($payload);
    $payload['allowed_entities']    = $allowedEntities;
    $payload['allowed_action_types'] = json_decode(AI_ACTION_TYPES_JSON, true);
    $payload['control']             = aiNormalizeControl($payload['control'] ?? null);

    $signature = aiBuildSignature();
    $sysPrompt = aiBuildSystemPrompt($signature);
    $userPrompt = aiBuildUserPrompt($payload, $signature);

    $result = aiCallProviderStructured($sysPrompt, $userPrompt, $signature, $payload);
    if (isset($result['error'])) {
        // Никогда не возвращаем сырое имя провайдера/модели в текстах ошибок,
        // но отдаём короткий машинный код (`transport`, `http_401`, `http_402`,
        // `http_429`, `empty_content`, `invalid_json`, `Validation failed: ...`)
        // — это позволяет UI показать предметную причину вместо общей фразы и
        // экономит время на хождение в php_errors.log.
        $code = (string)$result['error'];
        // Грубая категоризация для человекочитаемой подсказки в UI.
        $hint = 'AI service is temporarily unavailable. Try again later.';
        if ($code === 'transport') {
            $hint = 'Не удаётся достучаться до AI-сервиса (исходящие соединения от хостинга?).';
        } elseif (strpos($code, 'http_401') === 0) {
            $hint = 'AI ключ отклонён (401). Проверьте AI_API_KEY (без пробелов и невидимых символов) и пересохраните ключ.';
        } elseif (strpos($code, 'http_402') === 0) {
            $hint = 'У AI-провайдера недостаточный баланс (402).';
        } elseif (strpos($code, 'http_429') === 0) {
            $hint = 'AI-провайдер отвечает 429 (rate limit). Повторите позже.';
        } elseif (strpos($code, 'http_5') === 0) {
            $hint = 'Временный сбой AI-провайдера (' . $code . '). Повторите позже.';
        } elseif ($code === 'empty_content' || $code === 'invalid_json') {
            $hint = 'AI вернул пустой/некорректный ответ. Повторите запрос.';
        } elseif (strpos($code, 'Validation failed') === 0) {
            $hint = 'AI вернул JSON не по схеме. Нажмите «Обновить» ещё раз.';
        }
        aiLog('ai_analyze_error', ['error_code' => $code, 'hint' => $hint]);
        echo json_encode([
            'status'     => 'error',
            'error'      => $hint,
            'error_code' => $code,
        ]);
        exit;
    }

    // Успех. Структура от aiCallProviderStructured: ['data' => ..., 'meta' => ...].
    $data = $result['data'] ?? $result;
    $meta = is_array($result['meta'] ?? null) ? $result['meta'] : [];

    // Успех — пишем в кэш (если знаем период) с телеметрией и hash.
    $createdAt = time();
    $cacheMeta = [
        'payload_hash'      => $payloadHash,
        'prompt_version'    => AI_PROMPT_VERSION,
        'model_used'        => (string)($meta['model_used']        ?? ''),
        'latency_ms'        => (int)   ($meta['latency_ms']        ?? 0),
        'prompt_tokens'     => (int)   ($meta['prompt_tokens']     ?? 0),
        'completion_tokens' => (int)   ($meta['completion_tokens'] ?? 0),
        'total_tokens'      => (int)   ($meta['total_tokens']      ?? 0),
        'refine_rounds'           => (int)($meta['refine_rounds']           ?? 0),
        'hallu_offer_count'       => (int)($meta['hallu_offer_count']       ?? 0),
        'numeric_check_failures'  => (int)($meta['numeric_check_failures']  ?? 0),
        'empty_evidence_count'    => (int)($meta['empty_evidence_count']    ?? 0),
        'low_quality_recs'        => (int)($meta['low_quality_recs']        ?? 0),
        'duplicate_recs_dropped'  => (int)($meta['duplicate_recs_dropped']  ?? 0),
        'filter_drops'            => (int)($meta['filter_drops']            ?? 0),
        'anomaly_coverage_missed' => (int)($meta['anomaly_coverage_missed'] ?? 0),
    ];
    if ($periodFrom !== '' && $periodTo !== '') {
        aiCachePut($db, $periodFrom, $periodTo, $data, $createdAt, $cacheMeta);
    }
    aiLog('ai_analyze_success', [
        'period_from'    => $periodFrom,
        'period_to'      => $periodTo,
        'created_at'     => $createdAt,
        'payload_hash'   => $payloadHash,
        'prompt_version' => AI_PROMPT_VERSION,
        'meta'           => $meta,
    ]);
    echo json_encode([
        'status'         => 'success',
        'data'           => $data,
        'cached'         => false,
        'stale'          => false,
        'period'         => ['from' => $periodFrom, 'to' => $periodTo],
        'created_at'     => $createdAt,
        'payload_hash'   => $payloadHash,
        'prompt_version' => AI_PROMPT_VERSION,
        'meta'           => [
            'model_used'        => $cacheMeta['model_used'],
            'latency_ms'        => $cacheMeta['latency_ms'],
            'prompt_tokens'     => $cacheMeta['prompt_tokens'],
            'completion_tokens' => $cacheMeta['completion_tokens'],
            'total_tokens'      => $cacheMeta['total_tokens'],
        ],
    ]);
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