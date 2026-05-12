<?php
/**
 * ai_config.example.php
 *
 * Скопируйте этот файл в `ai_config.php` (он добавлен в .gitignore и не
 * попадёт в репозиторий) и впишите ваш ключ DeepSeek.
 *
 * Используется, если на хостинге нельзя задать переменные окружения
 * AI_API_KEY / AI_API_URL / AI_MODEL. Файл подхватывается автоматически
 * из leads-proxy.php — никаких других правок не требуется.
 *
 * Запросы к DeepSeek идут со стороны сервера (PHP cURL), а не из браузера,
 * поэтому VPN на компьютере пользователя не нужен — нужен лишь свободный
 * исходящий доступ от хостинга к api.deepseek.com.
 */

// Ваш API-ключ DeepSeek (https://platform.deepseek.com/api_keys).
// ВАЖНО: не коммитьте реальный ключ в git.
define('AI_API_KEY', 'sk-XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX');

// Эндпоинт провайдера. Менять обычно не нужно.
define('AI_API_URL', 'https://api.deepseek.com/chat/completions');

// Основная модель — `deepseek-v4-flash` (последнее поколение DeepSeek V4,
// быстрая, поддерживает response_format=json_object и temperature).
// Старые алиасы `deepseek-chat` / `deepseek-reasoner` ещё работают как
// прокси на V4, но будут удалены 24 июля 2026 — закладываемся на новые имена.
define('AI_MODEL', 'deepseek-v4-flash');

// Резервная модель — флагман `deepseek-v4-pro` (1.6T параметров, режим
// «thinking»). Используется только если основная вернула ошибку.
define('AI_FALLBACK_MODEL', 'deepseek-v4-pro');
