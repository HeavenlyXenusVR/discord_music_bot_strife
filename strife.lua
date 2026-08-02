--[[
  STRIFE -- "Cloud" -- GWS Music Bot
  LuaJIT port of strife.py, built on the shared swarmlua/ foundation.
  Last bot of the 13-bot swarm to go through a finish-up/validation pass.

  Node name: strife | Command namespace: /strife_main_*
  Named after Cloud Strife: ex-SOLDIER, mercenary, reluctant deliveryman.
  He carries the Buster Sword; this bot carries the queue. Both take the job
  seriously even when nobody's watching.

  Feature parity target: strife.py (discord.py + wavelink + aiomysql), which
  is the same underlying codebase as lockhart.py/tunestream.py/rhythm.py/etc
  (byte-for-byte identical aside from bot-name/table-prefix substitution: both
  declare exactly 59 `/xxx_main_*` slash commands). This bot's Postgres
  database (discord_music_strife) started completely empty -- unlike 10 of
  the other 12 bots in the swarm, there was no MariaDB data to preserve, so
  the schema below (SCHEMA_TABLES/SCHEMA_INDEXES/bootstrap_schema()) is
  authored fresh at its desired Postgres-native end-state instead of mirrored
  from existing rows -- the same situation discord_music_lockhart hit, and
  this file's own pattern was reused there (see lockhart.lua's header).

  Full command-surface parity with strife.py: all 59 of its
  `/strife_main_*` slash commands are ported (verified name-for-name against
  strife.py's @bot.tree.command declarations). strife_error_events IS wired
  (persist_error_event/send_error_webhook/report_error, plus swarmlua.bot's
  additive on_error hook) -- command handler failures, process_queue errors,
  panel-button failures, and heartbeat-loop errors all land there and, if
  STRIFE_ERROR_WEBHOOK_URL is configured, post to a dedicated error webhook.
  STRIFE_WEBHOOK_URL (operational "node online" logging) is also wired.

  Fixed during this pass (previously incomplete):
    - strife.lua never actually called bootstrap_schema(), never started the
      heartbeat/metrics background loop, and had no bot:run() at all -- the
      file, as handed off, registered commands but never actually booted.
    - Two direct table.unpack() calls in persist_playback_state() (LuaJIT 5.1
      has no global table.unpack) -- shimmed at the top of this file, the
      same gotcha that has bitten 2+ sibling bots even after pg.lua's own
      copy was fixed.
    - lib/swarmlua/pg.lua and lib/swarmlua/bot.lua were stale vendored
      copies, missing pg.lua's three fixes (table.unpack shim, nil-parameter
      NULL escaping, and the bigint/snowflake int8->string type deserializer
      that stops Discord IDs above 2^53 from silently losing precision) and
      bot.lua's on_error hook -- both overwritten wholesale from lua-shared/.
    - strife_main_247 (24/7 stay-in-vc toggle) and strife_main_panel
      (interactive button control panel) were referenced in /strife_main_help
      but never implemented -- both ported now, matching lockhart.lua's/
      tunestream.lua's shape (panel buttons + autocomplete route through a
      second bot.gateway:on("INTERACTION_CREATE") listener alongside
      swarmlua.bot's own, since Gateway:on supports multiple handlers per
      event without needing to touch swarmlua/bot.lua itself).
    - enqueue_track() never incremented strife_track_intelligence /
      strife_user_track_affinity's queued_count (strife.py does, as a Auto-DJ
      learning signal) -- added, correctly zero-filling every other NOT NULL
      DEFAULT 0 counter column on first insert.
    - strife_voice_state and strife_metrics were declared in the schema but
      never written to anywhere -- mark_voice_connected()/
      mark_voice_disconnected() (wired into connect_voice()/disconnect_voice())
      and heartbeat_tick()'s strife_metrics upsert now populate them, matching
      the shared cross-bot dashboard's expectations.
    - /strife_main_fade persisted transition_mode/fade_seconds/fade_curve
      but process_queue never actually applied them -- a track start under
      "fade"/"smart"/"mix" now starts at volume 0 and ramps to the configured
      volume over fade_seconds via a cancellable copas thread (fade_tokens),
      matching lockhart.lua's/tunestream.lua's shape. "mix" still falls back
      to the same smooth ramp as "fade" -- no BPM-matched crossfade, see
      simplifications below.
    - Added restore_queue_from_backup(): process_queue_impl now falls back to
      repopulating the live queue from strife_queue_backup when the live
      queue is empty and loop_mode is "queue", before trying Auto-DJ --
      mirrors strife.py's/lockhart.py's queue-parity-repair intent (a
      simplified version of it; see below).

  Deliberately NOT ported (same precedent as lockhart.lua/tunestream.lua/
  rhythm.lua for this shared Python codebase):
    - Redis cross-process locks, local ffmpeg/aubio audio cache + BPM/loudness
      analysis, YouTube Data API playlist fallback -- asyncio/aiomysql-races
      era plumbing that doesn't apply to this single-process LuaJIT stack.
    - Gemini track embeddings/co-occurrence (strife_track_embeddings,
      strife_track_cooccurrence) -- Auto-DJ is a simplified scoring model
      driven by strife_track_intelligence/strife_user_track_affinity
      counters instead of semantic embedding similarity.
    - The Aria/SwarmPanel remote-control bridge (strife_swarm_direct_orders,
      strife_swarm_overrides) -- this bot is controlled directly via its own
      Discord slash commands/panel only.
    - "Live playlist sync" (strife_active_playlist_tracks, the background
      task that re-scans a queued playlist URL for newly-added videos) --
      /strife_main_play expands a playlist once, in full, at queue time via
      Lavalink's loadType=playlist response. strife_active_playlists (the
      per-guild "what playlist is active" row, not the per-track sync table)
      IS still used the same way strife.py uses it.
    - strife_status_messages (in-place status-message-edit on a persistent
      "now playing" post) -- dropped for the same reason as lockhart.lua:
      /strife_main_panel and /strife_main_nowplaying already cover the same
      operational need without a message-ID tracking table.
    - "Beat-matched mixing" (/strife_main_fade mix) falls back to the same
      volume fade-in as /strife_main_fade fade -- no BPM analysis/matching.
    - restore_queue_from_backup()'s "queue parity repair" is a simple
      "restore everything from the backup mirror when the live queue goes
      empty in loop_mode=queue" -- not strife.py's fuller repair heuristic
      (which also reconciles the other direction, live -> backup, and
      handles partial/duplicate drift).
    - Full crash-recovery-on-restart (resuming every guild's playback
      position immediately after a process restart) is not wired up as a
      background task; strife_voice_state/strife_playback_state are kept
      current enough for an external dashboard/future pass to use, but this
      file doesn't itself replay them into a live rejoin at boot.
]]

package.path = "./lib/?.lua;./lib/?/init.lua;" .. package.path

local copas = require("copas")
local cjson = require("cjson")
local pgmoon = require("pgmoon")
local https = require("ssl.https")
local ltn12 = require("ltn12")
local Bot = require("swarmlua.bot")

local PG_NULL = pgmoon.Postgres.NULL

-- LuaJIT 5.1 doesn't have a global table.unpack (that's Lua 5.2+); pg.lua's
-- own copy of this shim only covers pg.lua's internals, not this file's own
-- direct table.unpack() calls in persist_playback_state() below -- this has
-- bitten 2+ sibling bots even after pg.lua was fixed, so shim it here too.
local unpack = table.unpack or unpack

-- =====================================================================
-- CONFIG (mirrors strife.py's env-var precedence exactly, so the shared
-- Music/.env keeps working unchanged)
-- =====================================================================

local BOT_ENV_PREFIX = "STRIFE"

local function env(name, default)
  local v = os.getenv(name)
  if v == nil or v == "" then return default end
  return v
end

-- File-based logging was entirely missing from this bot (stdout-only, so
-- anything not caught live via `docker logs` was gone on the next log
-- rotation/restart) -- unlike alucard.py's RotatingFileHandler, which every
-- Python-era bot had. Rather than hand-migrate every existing print() call
-- site (large, error-prone for one pass), print itself is wrapped here so
-- every call -- old and new -- is transparently timestamped, tagged, and
-- persisted to a rotating file (same 10MB x 5 backups policy as the other
-- bots' logline()/LOG_FILE and Lavalink's logback config), with zero change
-- to any call site's behavior or arguments.
local LOG_DIR = env("STRIFE_LOG_DIR", env("MUSIC_BOT_LOG_DIR", "/app/logs"))
pcall(function() os.execute("mkdir -p '" .. LOG_DIR .. "'") end)
local LOG_FILE = LOG_DIR .. "/strife.log"
local LOG_MAX_BYTES = tonumber(env("MUSIC_BOT_LOG_MAX_BYTES", "10485760")) or 10485760
local LOG_BACKUP_COUNT = tonumber(env("MUSIC_BOT_LOG_BACKUP_COUNT", "5")) or 5

local function rotate_log_if_needed()
  local f = io.open(LOG_FILE, "r")
  if not f then return end
  local size = f:seek("end")
  f:close()
  if not size or size < LOG_MAX_BYTES then return end
  for i = LOG_BACKUP_COUNT - 1, 1, -1 do
    os.rename(LOG_FILE .. "." .. i, LOG_FILE .. "." .. (i + 1))
  end
  os.rename(LOG_FILE, LOG_FILE .. ".1")
end

local raw_print = print
local function logline(level, fmt, ...)
  local ok, msg = pcall(string.format, fmt, ...)
  if not ok then msg = fmt end
  local line = string.format("%s %-5s strife %s", os.date("%Y-%m-%d %H:%M:%S"), level, msg)
  raw_print(line)
  rotate_log_if_needed()
  local f = io.open(LOG_FILE, "a")
  if f then f:write(line .. "\n"); f:close() end
end
local function log_info(fmt, ...) logline("INFO", fmt, ...) end
local function log_warn(fmt, ...) logline("WARN", fmt, ...) end
local function log_error(fmt, ...) logline("ERROR", fmt, ...) end

-- Existing print("[bot] message") calls throughout this file already embed
-- their own tag/timestamp-free format; route them through the same
-- rotating file (tagged INFO, since plain print() carries no level) instead
-- of rewriting every call site.
print = function(...)
  local n = select("#", ...)
  local parts = {{}}
  for i = 1, n do parts[i] = tostring((select(i, ...))) end
  logline("INFO", "%s", table.concat(parts, "\t"))
end

local function envf(name, default)
  local v = os.getenv(name)
  if v == nil or v == "" then return default end
  return tonumber(v) or default
end

local TOKEN = env("STRIFE_DISCORD_TOKEN", "")
if TOKEN == "" then
  error("STRIFE_DISCORD_TOKEN is not set; cannot start strife.")
end

local DB_CONFIG = {
  host = env("STRIFE_DB_HOST", env("DB_HOST", "127.0.0.1")),
  port = envf("STRIFE_DB_PORT", envf("DB_PORT", 5432)),
  user = env("STRIFE_DB_USER", env("DB_USER", "botuser")),
  password = env("STRIFE_DB_PASSWORD", env("DB_PASSWORD", "")),
  database = env("STRIFE_DB_NAME", "discord_music_strife"),
}

local LAVALINK_URI = env("STRIFE_LAVALINK_URI", env("LAVALINK_URI", "http://127.0.0.1:2333"))
local LAVALINK_PASSWORD = env("STRIFE_LAVALINK_PASSWORD", env("LAVALINK_PASSWORD", ""))
if LAVALINK_PASSWORD == "" then
  error("Set STRIFE_LAVALINK_PASSWORD or LAVALINK_PASSWORD before starting strife.")
end

-- Operational + error webhooks (mirrors strife.py's WEBHOOK_URL / ERROR_WEBHOOK_URL).
local WEBHOOK_URL = env("STRIFE_WEBHOOK_URL", "")
local ERROR_WEBHOOK_URL = env("STRIFE_ERROR_WEBHOOK_URL", env("SWARM_ERROR_WEBHOOK_URL", env("ERROR_WEBHOOK_URL", "")))

-- Parse host/port out of a URI like "http://127.0.0.1:2333"
local function parse_lavalink_uri(uri)
  local host, port = uri:match("^https?://([^:/]+):?(%d*)")
  host = host or "127.0.0.1"
  port = tonumber(port) or 2333
  return host, port
end
local LAVALINK_HOST, LAVALINK_PORT = parse_lavalink_uri(LAVALINK_URI)

-- =====================================================================
-- BOT CONSTRUCTION
-- =====================================================================
-- GUILDS | GUILD_VOICE_STATES -- application commands need no intent at
-- all, and voice-state tracking (who's in what channel) is the only thing
-- this bot actually listens for. Narrower than swarmlua's broad default.
local INTENTS = 1 + 128

local bot = Bot.new({
  token = TOKEN,
  intents = INTENTS,
  db = DB_CONFIG,
  lavalink = { host = LAVALINK_HOST, port = LAVALINK_PORT, password = LAVALINK_PASSWORD },
})

bot.start_time = os.time()

-- =====================================================================
-- SCHEMA BOOTSTRAP (Postgres-native; this DB started empty, so this is
-- authored directly at the desired end-state rather than mirrored from
-- strife.py's incremental MariaDB ALTER-TABLE migration history)
-- =====================================================================

local SCHEMA_TABLES = {
[[CREATE TABLE IF NOT EXISTS strife_playback_state (
  guild_id BIGINT NOT NULL,
  bot_name VARCHAR(50) NOT NULL DEFAULT 'strife',
  channel_id BIGINT,
  video_url TEXT,
  position_seconds INTEGER DEFAULT 0,
  is_playing BOOLEAN DEFAULT FALSE,
  is_paused BOOLEAN DEFAULT FALSE,
  title TEXT,
  last_checkpoint_at TIMESTAMPTZ DEFAULT now(),
  play_session_key VARCHAR(64),
  track_uid CHAR(32),
  PRIMARY KEY (guild_id, bot_name)
)]],
[[CREATE TABLE IF NOT EXISTS strife_guild_settings (
  guild_id BIGINT PRIMARY KEY,
  home_vc_id BIGINT,
  volume INTEGER DEFAULT 100,
  loop_mode VARCHAR(10) DEFAULT 'queue',
  filter_mode VARCHAR(20) DEFAULT 'none',
  filter_stack VARCHAR(20),
  dj_role_id BIGINT,
  feedback_channel_id BIGINT,
  transition_mode VARCHAR(10) DEFAULT 'off',
  fade_seconds DOUBLE PRECISION DEFAULT 3.0,
  fade_curve VARCHAR(20) DEFAULT 'linear',
  custom_speed DOUBLE PRECISION DEFAULT 1.0,
  custom_pitch DOUBLE PRECISION DEFAULT 1.0,
  custom_modifiers_left INTEGER DEFAULT 0,
  dj_only_mode BOOLEAN DEFAULT FALSE,
  stay_in_vc BOOLEAN DEFAULT FALSE
)]],
[[CREATE TABLE IF NOT EXISTS strife_queue (
  id BIGSERIAL PRIMARY KEY,
  guild_id BIGINT NOT NULL,
  bot_name VARCHAR(50) NOT NULL DEFAULT 'strife',
  video_url TEXT,
  title TEXT,
  requester_id BIGINT,
  track_uid CHAR(32)
)]],
[[CREATE TABLE IF NOT EXISTS strife_queue_backup (
  id BIGSERIAL PRIMARY KEY,
  guild_id BIGINT NOT NULL,
  bot_name VARCHAR(50) NOT NULL DEFAULT 'strife',
  video_url TEXT,
  title TEXT,
  requester_id BIGINT,
  track_uid CHAR(32)
)]],
[[CREATE TABLE IF NOT EXISTS strife_history (
  id BIGSERIAL PRIMARY KEY,
  guild_id BIGINT NOT NULL,
  video_url TEXT,
  title TEXT,
  played_at TIMESTAMPTZ DEFAULT now(),
  requester_id BIGINT
)]],
[[CREATE TABLE IF NOT EXISTS strife_user_playlists (
  id BIGSERIAL PRIMARY KEY,
  user_id BIGINT NOT NULL,
  playlist_name VARCHAR(255) NOT NULL,
  video_url TEXT,
  title TEXT
)]],
[[CREATE TABLE IF NOT EXISTS strife_track_intelligence (
  guild_id BIGINT NOT NULL,
  url_key VARCHAR(64) NOT NULL,
  video_url TEXT,
  title TEXT,
  queued_count INTEGER NOT NULL DEFAULT 0,
  play_count INTEGER NOT NULL DEFAULT 0,
  finish_count INTEGER NOT NULL DEFAULT 0,
  skip_count INTEGER NOT NULL DEFAULT 0,
  like_count INTEGER NOT NULL DEFAULT 0,
  dislike_count INTEGER NOT NULL DEFAULT 0,
  total_listen_seconds INTEGER NOT NULL DEFAULT 0,
  last_requester_id BIGINT,
  source VARCHAR(40) DEFAULT 'unknown',
  first_seen TIMESTAMPTZ DEFAULT now(),
  last_queued TIMESTAMPTZ,
  last_played TIMESTAMPTZ,
  updated_at TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (guild_id, url_key)
)]],
[[CREATE TABLE IF NOT EXISTS strife_user_track_affinity (
  guild_id BIGINT NOT NULL,
  user_id BIGINT NOT NULL,
  url_key VARCHAR(64) NOT NULL,
  video_url TEXT,
  title TEXT,
  queued_count INTEGER NOT NULL DEFAULT 0,
  play_count INTEGER NOT NULL DEFAULT 0,
  finish_count INTEGER NOT NULL DEFAULT 0,
  skip_count INTEGER NOT NULL DEFAULT 0,
  like_count INTEGER NOT NULL DEFAULT 0,
  dislike_count INTEGER NOT NULL DEFAULT 0,
  score DOUBLE PRECISION NOT NULL DEFAULT 0,
  last_requested TIMESTAMPTZ,
  updated_at TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (guild_id, user_id, url_key)
)]],
[[CREATE TABLE IF NOT EXISTS strife_smart_recommendations (
  id BIGSERIAL PRIMARY KEY,
  guild_id BIGINT NOT NULL,
  requester_id BIGINT,
  seed_title TEXT,
  seed_url TEXT,
  query_text TEXT,
  chosen_url TEXT,
  chosen_title TEXT,
  reason VARCHAR(80),
  accepted BOOLEAN DEFAULT TRUE,
  created_at TIMESTAMPTZ DEFAULT now()
)]],
[[CREATE TABLE IF NOT EXISTS strife_track_embeddings (
  url_key VARCHAR(64) PRIMARY KEY,
  title TEXT,
  video_url TEXT,
  embedding TEXT,
  updated_at TIMESTAMPTZ DEFAULT now()
)]],
[[CREATE TABLE IF NOT EXISTS strife_track_cooccurrence (
  guild_id BIGINT NOT NULL,
  url_key_a VARCHAR(64) NOT NULL,
  url_key_b VARCHAR(64) NOT NULL,
  weight DOUBLE PRECISION NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (guild_id, url_key_a, url_key_b)
)]],
[[CREATE TABLE IF NOT EXISTS strife_bot_home_channels (
  guild_id BIGINT NOT NULL,
  bot_name VARCHAR(50) NOT NULL DEFAULT 'strife',
  home_vc_id BIGINT,
  PRIMARY KEY (guild_id, bot_name)
)]],
[[CREATE TABLE IF NOT EXISTS strife_voice_state (
  guild_id BIGINT NOT NULL,
  bot_name VARCHAR(50) NOT NULL DEFAULT 'strife',
  last_channel_id BIGINT,
  connected_channel_id BIGINT,
  text_channel_id BIGINT,
  disconnected_at TIMESTAMPTZ,
  last_seen_at TIMESTAMPTZ DEFAULT now(),
  desired_connected BOOLEAN DEFAULT FALSE,
  reconnect_attempts INTEGER NOT NULL DEFAULT 0,
  last_error TEXT,
  PRIMARY KEY (guild_id, bot_name)
)]],
[[CREATE TABLE IF NOT EXISTS strife_metrics (
  guild_id BIGINT NOT NULL,
  bot_name VARCHAR(50) NOT NULL DEFAULT 'strife',
  voice_connected BOOLEAN DEFAULT FALSE,
  connected_channel_id BIGINT,
  player_connected BOOLEAN DEFAULT FALSE,
  player_playing BOOLEAN DEFAULT FALSE,
  player_paused BOOLEAN DEFAULT FALSE,
  queue_count INTEGER DEFAULT 0,
  backup_queue_count INTEGER DEFAULT 0,
  is_playing_db BOOLEAN DEFAULT FALSE,
  is_paused_db BOOLEAN DEFAULT FALSE,
  position_seconds INTEGER DEFAULT 0,
  duration_seconds INTEGER DEFAULT 0,
  recovery_pending BOOLEAN DEFAULT FALSE,
  heartbeat_age_seconds INTEGER DEFAULT 0,
  lavalink_ready BOOLEAN DEFAULT FALSE,
  last_error TEXT,
  updated_at TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (guild_id, bot_name)
)]],
[[CREATE TABLE IF NOT EXISTS strife_active_playlists (
  guild_id BIGINT NOT NULL,
  bot_name VARCHAR(50) NOT NULL DEFAULT 'strife',
  playlist_url TEXT,
  known_track_count INTEGER DEFAULT 0,
  requester_id BIGINT,
  channel_id BIGINT,
  PRIMARY KEY (guild_id, bot_name)
)]],
[[CREATE TABLE IF NOT EXISTS strife_active_playlist_tracks (
  guild_id BIGINT NOT NULL,
  bot_name VARCHAR(50) NOT NULL DEFAULT 'strife',
  playlist_url TEXT,
  position_idx INTEGER DEFAULT 0,
  track_key CHAR(40),
  track_uid CHAR(32),
  video_url TEXT,
  title TEXT,
  requester_id BIGINT,
  updated_at TIMESTAMPTZ DEFAULT now()
)]],
[[CREATE TABLE IF NOT EXISTS strife_swarm_toggles (
  guild_id BIGINT PRIMARY KEY,
  auto_dj BOOLEAN DEFAULT FALSE,
  audio_filter VARCHAR(20) DEFAULT 'normal'
)]],
[[CREATE TABLE IF NOT EXISTS strife_swarm_overrides (
  guild_id BIGINT NOT NULL,
  bot_name VARCHAR(50) NOT NULL DEFAULT 'strife',
  command VARCHAR(20),
  PRIMARY KEY (guild_id, bot_name)
)]],
[[CREATE TABLE IF NOT EXISTS strife_swarm_direct_orders (
  id BIGSERIAL PRIMARY KEY,
  bot_name VARCHAR(50),
  guild_id BIGINT,
  vc_id BIGINT,
  text_channel_id BIGINT,
  command VARCHAR(50),
  data TEXT,
  attempts INTEGER NOT NULL DEFAULT 0,
  last_error TEXT,
  claimed_at TIMESTAMPTZ,
  claim_token VARCHAR(128),
  created_at TIMESTAMPTZ DEFAULT now()
)]],
[[CREATE TABLE IF NOT EXISTS swarm_health (
  bot_name VARCHAR(50) PRIMARY KEY,
  last_pulse TIMESTAMPTZ DEFAULT now(),
  status VARCHAR(20)
)]],
[[CREATE TABLE IF NOT EXISTS strife_error_events (
  id BIGSERIAL PRIMARY KEY,
  bot_name VARCHAR(50) NOT NULL,
  guild_id BIGINT,
  error_level VARCHAR(20) NOT NULL DEFAULT 'error',
  error_type VARCHAR(50) NOT NULL DEFAULT 'runtime',
  title VARCHAR(255) NOT NULL,
  description TEXT,
  traceback_text TEXT,
  created_at TIMESTAMPTZ DEFAULT now()
)]],
[[CREATE TABLE IF NOT EXISTS strife_status_messages (
  guild_id BIGINT NOT NULL,
  bot_name VARCHAR(50) NOT NULL DEFAULT 'strife',
  feedback_channel_id BIGINT,
  message_id BIGINT,
  updated_at TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (guild_id, bot_name)
)]],
}

local SCHEMA_INDEXES = {
  "CREATE INDEX IF NOT EXISTS strife_track_intelligence_recent_idx ON strife_track_intelligence (guild_id, last_played)",
  "CREATE INDEX IF NOT EXISTS strife_track_intelligence_requester_idx ON strife_track_intelligence (guild_id, last_requester_id, last_played)",
  "CREATE INDEX IF NOT EXISTS strife_user_affinity_recent_idx ON strife_user_track_affinity (guild_id, user_id, last_requested)",
  "CREATE INDEX IF NOT EXISTS strife_smart_recommendations_recent_idx ON strife_smart_recommendations (guild_id, created_at)",
  "CREATE INDEX IF NOT EXISTS strife_track_intelligence_global_idx ON strife_track_intelligence (url_key, last_played)",
  "CREATE INDEX IF NOT EXISTS strife_track_cooccurrence_weight_idx ON strife_track_cooccurrence (guild_id, url_key_a, weight)",
  "CREATE INDEX IF NOT EXISTS strife_voice_state_rejoin_idx ON strife_voice_state (bot_name, desired_connected, last_seen_at)",
  "CREATE INDEX IF NOT EXISTS strife_metrics_status_idx ON strife_metrics (bot_name, updated_at)",
  "CREATE INDEX IF NOT EXISTS strife_queue_lookup_idx ON strife_queue (guild_id, bot_name, id)",
  "CREATE INDEX IF NOT EXISTS strife_queue_backup_lookup_idx ON strife_queue_backup (guild_id, bot_name, id)",
  "CREATE UNIQUE INDEX IF NOT EXISTS strife_queue_track_uid_unique_idx ON strife_queue (guild_id, bot_name, track_uid)",
  "CREATE UNIQUE INDEX IF NOT EXISTS strife_queue_backup_track_uid_unique_idx ON strife_queue_backup (guild_id, bot_name, track_uid)",
  "CREATE INDEX IF NOT EXISTS strife_history_guild_played_idx ON strife_history (guild_id, played_at)",
  "CREATE INDEX IF NOT EXISTS strife_history_requester_idx ON strife_history (guild_id, requester_id, played_at)",
  "CREATE INDEX IF NOT EXISTS strife_playback_resume_idx ON strife_playback_state (bot_name, is_playing, guild_id)",
  "CREATE INDEX IF NOT EXISTS strife_playback_track_uid_idx ON strife_playback_state (bot_name, track_uid)",
  "CREATE INDEX IF NOT EXISTS strife_playlist_bot_idx ON strife_active_playlists (bot_name, guild_id)",
  "CREATE INDEX IF NOT EXISTS strife_active_playlists_lookup_idx ON strife_active_playlists (guild_id, bot_name)",
  "CREATE INDEX IF NOT EXISTS strife_playlist_track_uid_idx ON strife_active_playlist_tracks (guild_id, bot_name, track_uid)",
  "CREATE INDEX IF NOT EXISTS strife_active_playlist_tracks_guild_bot_idx ON strife_active_playlist_tracks (guild_id, bot_name, position_idx)",
  "CREATE INDEX IF NOT EXISTS strife_active_playlist_tracks_del_idx ON strife_active_playlist_tracks (guild_id, bot_name, playlist_url)",
  "CREATE INDEX IF NOT EXISTS strife_user_playlist_lookup_idx ON strife_user_playlists (user_id, playlist_name)",
  "CREATE INDEX IF NOT EXISTS strife_error_events_guild_time_idx ON strife_error_events (guild_id, created_at)",
  "CREATE INDEX IF NOT EXISTS strife_direct_unclaimed_idx ON strife_swarm_direct_orders (bot_name, guild_id, claimed_at, id)",
  "CREATE INDEX IF NOT EXISTS strife_direct_claim_token_idx ON strife_swarm_direct_orders (claim_token)",
}

local function bootstrap_schema()
  for _, sql in ipairs(SCHEMA_TABLES) do
    local ok, err = bot.db:query(sql)
    if not ok then
      error("strife schema bootstrap failed on CREATE TABLE: " .. tostring(err) .. "\nSQL: " .. sql)
    end
  end
  for _, sql in ipairs(SCHEMA_INDEXES) do
    local ok, err = bot.db:query(sql)
    if not ok then
      print("[strife] schema index warning (non-fatal): " .. tostring(err) .. " -- " .. sql)
    end
  end
  print("[strife] Postgres schema verified/created (discord_music_strife).")
end

-- =====================================================================
-- SMALL UTILITIES
-- =====================================================================

local COLOR = {
  green = 0x2ecc71, red = 0xe74c3c, blue = 0x3498db, blurple = 0x5865F2,
  gold = 0xf1c40f, orange = 0xe67e22, purple = 0x9b59b6, dark_purple = 0x71368a,
  grey = 0x2b2d31,
}

local function embed(opts)
  local e = {}
  if opts.title then e.title = opts.title end
  if opts.description then e.description = opts.description end
  e.color = opts.color or COLOR.blurple
  if opts.fields then e.fields = opts.fields end
  if opts.footer then e.footer = { text = opts.footer } end
  if opts.author then e.author = { name = opts.author } end
  return e
end

local function field(name, value, inline)
  return { name = name, value = tostring(value), inline = inline ~= false }
end

local function now_iso() return os.date("!%Y-%m-%dT%H:%M:%SZ") end

local function json_encode(t) return cjson.encode(t) end

-- Snowflakes travel through cjson as Lua strings (Discord sends them as
-- JSON strings specifically to avoid float precision loss) -- keep them
-- as strings everywhere; Postgres coerces the quoted literal into BIGINT
-- columns on insert/compare automatically.
local function sid(v)
  if v == nil then return nil end
  return tostring(v)
end

-- Read a named option's value out of an interaction's option list.
local function opt(interaction, name, default)
  local options = interaction.data and interaction.data.options
  if not options then return default end
  for _, o in ipairs(options) do
    if o.name == name then
      if o.value == nil then return default end
      return o.value
    end
  end
  return default
end

-- Does this member have `flag` set in their permission bitfield?
-- (Discord permissions arrive as a decimal string; stay in double-space
-- arithmetic instead of LuaJIT's 32-bit bit.* ops, since bit 3 (ADMIN)
-- still resolves correctly via floor/mod even though the full field can
-- exceed 32 bits.)
local function has_perm(perm_str, flag)
  local n = tonumber(perm_str) or 0
  return math.floor(n / flag) % 2 == 1
end
local PERM_ADMINISTRATOR = 0x8

local guild_owner_cache = {} -- guild_id -> owner user_id

local function get_guild_owner_id(guild_id)
  if not guild_id then return nil end
  local cached = guild_owner_cache[guild_id]
  if cached then return cached end
  local g = bot.rest:get("/guilds/" .. tostring(guild_id))
  local owner_id = g and g.owner_id
  if owner_id then guild_owner_cache[guild_id] = owner_id end
  return owner_id
end

local function is_admin(interaction)
  local member = interaction.member
  if member and member.permissions and has_perm(member.permissions, PERM_ADMINISTRATOR) then
    return true
  end
  -- The guild owner always effectively has admin, but Discord's resolved
  -- member.permissions bitfield on the interaction has been observed to not
  -- reliably reflect that -- fall back to an explicit ownership check
  -- rather than ever locking the actual owner out of admin-gated commands.
  local uid = member and member.user and member.user.id
  local owner_id = uid and get_guild_owner_id(interaction.guild_id)
  return owner_id ~= nil and tostring(owner_id) == tostring(uid)
end

-- =====================================================================
-- DISCORD INTERACTION RESPONSE HELPERS
-- (bot.lua's Bot:reply only covers plain-text type-4 replies; commands
-- here need embeds, deferrals, followups and component updates too, so
-- these build directly on self.rest -- same REST client, just fuller use
-- of the interaction-response surface.)
-- =====================================================================

local function callback(interaction, response_type, data)
  return bot.rest:post(
    ("/interactions/%s/%s/callback"):format(interaction.id, interaction.token),
    { type = response_type, data = data }
  )
end

local function reply_embed(interaction, emb, ephemeral)
  return callback(interaction, 4, { embeds = { emb }, flags = ephemeral and 64 or nil })
end

local function reply_text(interaction, content, ephemeral)
  return callback(interaction, 4, { content = content, flags = ephemeral and 64 or nil })
end

local function defer(interaction, ephemeral)
  return callback(interaction, 5, { flags = ephemeral and 64 or nil })
end

local function defer_update(interaction)
  return callback(interaction, 6, {})
end

local function update_message(interaction, data)
  return callback(interaction, 7, data)
end

local function followup_embed(interaction, emb, ephemeral)
  return bot.rest:post(
    ("/webhooks/%s/%s"):format(bot.application_id, interaction.token),
    { embeds = { emb }, flags = ephemeral and 64 or nil }
  )
end

local function followup_text(interaction, content, ephemeral)
  return bot.rest:post(
    ("/webhooks/%s/%s"):format(bot.application_id, interaction.token),
    { content = content, flags = ephemeral and 64 or nil }
  )
end

-- =====================================================================
-- RUNTIME STATE (in-memory, per-process -- mirrors strife.py's module
-- level dicts like playback_tracking / guild_states)
-- =====================================================================

local playback = {}        -- guild_id -> { url, title, duration, position, requester_id, channel_id, current_filter, paused, track_uid, updated_at }
local voice_channel = {}   -- guild_id -> channel_id the bot itself is connected to
local voice_states = {}    -- guild_id -> { [user_id] = { channel_id=, is_bot= } }
local vote_skip = {}       -- guild_id -> { [user_id]=true }
local queue_votes = {}     -- "guild:queue_id" -> { up = {}, down = {} }
local sleep_timers = {}    -- guild_id -> generation counter (bump to cancel)
local requester_name_cache = {} -- user_id -> { name=, at= }
local fade_tokens = {}     -- guild_id -> incrementing token; a running fade-in checks it's still current

-- =====================================================================
-- DB HELPERS
-- =====================================================================

local function db(sql, ...)
  local rows, err = bot.db:query(sql, ...)
  if not rows then
    print("[strife] db error: " .. tostring(err) .. " | sql=" .. sql)
  end
  return rows, err
end

local function db1(sql, ...)
  local rows = db(sql, ...)
  return rows and rows[1] or nil
end

local function ensure_guild_settings(guild_id)
  db("INSERT INTO strife_guild_settings (guild_id) VALUES (%s) ON CONFLICT (guild_id) DO NOTHING", guild_id)
end

local function get_guild_settings(guild_id)
  ensure_guild_settings(guild_id)
  return db1("SELECT * FROM strife_guild_settings WHERE guild_id = %s", guild_id)
end

local function get_autodj_enabled(guild_id)
  local row = db1("SELECT auto_dj FROM strife_swarm_toggles WHERE guild_id = %s", guild_id)
  return row ~= nil and row.auto_dj == true
end

local function set_autodj_enabled(guild_id, enabled)
  db([[INSERT INTO strife_swarm_toggles (guild_id, auto_dj) VALUES (%s, %s)
       ON CONFLICT (guild_id) DO UPDATE SET auto_dj = EXCLUDED.auto_dj]], guild_id, enabled)
end

-- =====================================================================
-- OPERATIONAL WEBHOOK + ERROR EVENTS (mirrors strife.py's send_webhook_log()/
-- _persist_error_event()/send_error_webhook_log(): writes into the shared
-- strife_error_events table (same table Aria/SwarmPanel reads from for the
-- cross-bot error dashboard) and, if configured, posts to a dedicated error
-- webhook. Wired into swarmlua.bot's additive/optional on_error hook below
-- so slash-command handler failures land here too, not just process_queue/
-- the heartbeat loop.)
-- =====================================================================

local function truncate(value, limit)
  local text = tostring(value or "")
  if #text <= limit then return text end
  return text:sub(1, limit - 3) .. "..."
end

local function http_post_json(url, payload_table)
  local ok, err = pcall(function()
    local body = cjson.encode(payload_table)
    local response_body = {}
    https.request({
      url = url, method = "POST",
      headers = { ["Content-Type"] = "application/json", ["Content-Length"] = tostring(#body) },
      source = ltn12.source.string(body), sink = ltn12.sink.table(response_body),
    })
  end)
  if not ok then print("[strife] webhook POST failed: " .. tostring(err)) end
end

local function send_webhook_log(title, description, color)
  if WEBHOOK_URL == "" or WEBHOOK_URL == "PASTE_YOUR_NEW_WEBHOOK_URL_HERE" then return end
  http_post_json(WEBHOOK_URL, {
    username = "Node: Strife",
    embeds = { { title = title, description = description, color = color or COLOR.blurple, footer = { text = "Swarm Network Matrix" } } },
  })
end

local function persist_error_event(guild_id, level, error_type, title, description)
  db([[INSERT INTO strife_error_events (bot_name, guild_id, error_level, error_type, title, description)
       VALUES ('strife', %s, %s, %s, %s, %s)]],
    guild_id or PG_NULL, level, error_type, truncate(title, 255), truncate(description, 5000))
end

local function send_error_webhook(title, description)
  if ERROR_WEBHOOK_URL == "" then return end
  http_post_json(ERROR_WEBHOOK_URL, {
    embeds = { { title = "\240\159\148\180 Strife Error: " .. truncate(title, 200), description = truncate(description, 4000),
                 color = COLOR.red, footer = { text = "Strife Music Bot" } } },
  })
end

-- guild_id may be nil (process-wide failures, e.g. the heartbeat loop).
local function report_error(guild_id, error_type, title, description)
  local ok, err = pcall(persist_error_event, guild_id, "error", error_type, title, description)
  if not ok then print("[strife] failed to persist error event: " .. tostring(err)) end
  send_error_webhook(title, description)
end

-- Wired into swarmlua.bot's additive/optional on_error hook (see
-- lib/swarmlua/bot.lua) so slash-command handler failures also land in
-- strife_error_events / the error webhook. That hook only covers type=2
-- (APPLICATION_COMMAND) interaction failures -- the autocomplete/panel-button
-- MESSAGE_COMPONENT routing further down wraps its own pcall and calls
-- report_error directly, since those never reach bot:run()'s dispatcher.
bot.on_error = function(err, context)
  report_error(nil, "command_error", tostring(context and context.command or "unknown"), tostring(err))
end

-- =====================================================================
-- MISC HELPERS: track uid, requester names, track-key, progress bar
-- =====================================================================

local function random_hex(n)
  local chars = {}
  for i = 1, n do chars[i] = ("%x"):format(math.random(0, 15)) end
  return table.concat(chars)
end

local function new_track_uid()
  return random_hex(32)
end

local bit = require("bit")
local function track_key(video_url, title)
  local base = (tostring(video_url or "") .. "|" .. tostring(title or "")):lower()
  -- Cheap, dependency-free hash (FNV-1a), truncated/padded to 64 chars like the
  -- Python SHA-based url_key -- only used as a grouping key, not for security.
  -- Uses bit.bxor (LuaJIT has no Lua-5.3-style `~` bitwise operator) -- this
  -- was a syntax error that never ran until this bot was first run in a real
  -- container instead of a dry-run smoke test.
  local hash = 2166136261
  for i = 1, #base do
    hash = bit.bxor(hash, base:byte(i)) % 4294967296
    hash = (hash * 16777619) % 4294967296
  end
  return ("%08x"):format(hash) .. ("%08x"):format(#base)
end

local function resolve_requester_name(guild_id, requester_id)
  if not requester_id then return "Unknown" end
  local cached = requester_name_cache[requester_id]
  if cached and (os.time() - cached.at) < 900 then return cached.name end
  local user, err = bot.rest:get("/users/" .. requester_id)
  local name = (user and (user.global_name or user.username)) or ("User " .. tostring(requester_id))
  requester_name_cache[requester_id] = { name = name, at = os.time() }
  return name
end

local function make_progress_bar(current, total, length)
  length = length or 15
  current = math.max(0, math.floor(current or 0))
  if not total or total <= 0 then
    return ("[%s] %d:%02d / Live"):format(("\226\150\172"):rep(length), math.floor(current / 60), current % 60)
  end
  total = math.floor(total)
  local progress = math.max(0, math.min(length, math.floor((current / total) * length)))
  local bar = ("\226\150\172"):rep(progress) .. "\240\159\148\152" .. ("\226\150\172"):rep(math.max(0, length - progress - 1))
  return ("[%s] %d:%02d / %d:%02d"):format(bar, math.floor(current / 60), current % 60, math.floor(total / 60), total % 60)
end

local function compact_title(title, limit)
  title = tostring(title or "Unknown title")
  limit = limit or 72
  if #title <= limit then return title end
  return title:sub(1, limit - 1) .. "\226\128\166"
end

-- =====================================================================
-- DJ CHECK
-- =====================================================================

-- Returns true/false. When silent is false (default) and access is
-- denied, this also sends the ephemeral "strict DJ mode" response itself
-- (matching strife.py's is_dj, which doubles as the responder).
local function is_dj(interaction, silent)
  if is_admin(interaction) then return true end
  local guild_id = interaction.guild_id
  local settings = db1("SELECT dj_role_id, dj_only_mode FROM strife_guild_settings WHERE guild_id = %s", guild_id)
  if settings and settings.dj_only_mode then
    local dj_role = settings.dj_role_id
    if dj_role then
      local roles = (interaction.member and interaction.member.roles) or {}
      for _, r in ipairs(roles) do
        if tostring(r) == tostring(dj_role) then return true end
      end
    end
    if not silent then
      reply_embed(interaction, embed({
        description = "\226\157\140 **Strict DJ Mode is Active.** You need the DJ Role.",
        color = COLOR.red,
      }), true)
    end
    return false
  end
  return true
end

-- =====================================================================
-- QUEUE HELPERS
-- =====================================================================

local function queue_count(guild_id)
  local row = db1("SELECT COUNT(*) AS n FROM strife_queue WHERE guild_id = %s AND bot_name = 'strife'", guild_id)
  return row and tonumber(row.n) or 0
end

local function snapshot_queue_backup(guild_id)
  db("DELETE FROM strife_queue_backup WHERE guild_id = %s AND bot_name = 'strife'", guild_id)
  db([[INSERT INTO strife_queue_backup (guild_id, bot_name, video_url, title, requester_id, track_uid)
       SELECT guild_id, bot_name, video_url, title, requester_id, track_uid
       FROM strife_queue WHERE guild_id = %s AND bot_name = 'strife']], guild_id)
end

local function delete_backup_track(guild_id, track_uid, video_url, title)
  if track_uid then
    db("DELETE FROM strife_queue_backup WHERE guild_id = %s AND bot_name = 'strife' AND track_uid = %s", guild_id, track_uid)
  elseif video_url and title then
    db([[DELETE FROM strife_queue_backup WHERE id IN (
           SELECT id FROM strife_queue_backup
           WHERE guild_id = %s AND bot_name = 'strife' AND video_url = %s AND title = %s
           LIMIT 1)]], guild_id, video_url, title)
  end
end

local function insert_queue_front(guild_id, video_url, title, requester_id, track_uid)
  -- strife_queue has no explicit ordinal column -- ordering is by `id ASC`
  -- (insertion order), so "front" means: shift every existing id up by
  -- renumbering via a temp negative id, then normalize. Simpler and just
  -- as correct: give the new row an id lower than every existing row by
  -- inserting it, then re-sequencing is unnecessary because Postgres
  -- BIGSERIAL ids are monotonic -- so instead we track a synthetic
  -- "priority" via negative ids is fragile. Cheapest correct approach:
  -- move every other row's insertion order down by deleting+reinserting
  -- is expensive; instead we simply give bump/front operations their own
  -- ordering column-free trick: insert then swap by re-numbering `id` is
  -- not possible on BIGSERIAL, so we materialize full reorder here.
  local rows = db("SELECT video_url, title, requester_id, track_uid FROM strife_queue WHERE guild_id = %s AND bot_name = 'strife' ORDER BY id ASC", guild_id) or {}
  db("DELETE FROM strife_queue WHERE guild_id = %s AND bot_name = 'strife'", guild_id)
  db("INSERT INTO strife_queue (guild_id, bot_name, video_url, title, requester_id, track_uid) VALUES (%s, 'strife', %s, %s, %s, %s)",
    guild_id, video_url, title, requester_id or PG_NULL, track_uid or new_track_uid())
  for _, r in ipairs(rows) do
    db("INSERT INTO strife_queue (guild_id, bot_name, video_url, title, requester_id, track_uid) VALUES (%s, 'strife', %s, %s, %s, %s)",
      guild_id, r.video_url, r.title, r.requester_id or PG_NULL, r.track_uid or new_track_uid())
  end
end

local function enqueue_track(guild_id, video_url, title, requester_id, track_uid)
  -- Resolve the uid once so the live queue row and its backup mirror share
  -- the exact same track_uid (calling new_track_uid() twice independently,
  -- once per INSERT, would desync them since every caller in this file
  -- passes an explicit track_uid anyway -- but harden it here regardless).
  local uid = track_uid or new_track_uid()
  db("INSERT INTO strife_queue (guild_id, bot_name, video_url, title, requester_id, track_uid) VALUES (%s, 'strife', %s, %s, %s, %s)",
    guild_id, video_url, title, requester_id or PG_NULL, uid)
  db("INSERT INTO strife_queue_backup (guild_id, bot_name, video_url, title, requester_id, track_uid) VALUES (%s, 'strife', %s, %s, %s, %s)",
    guild_id, video_url, title, requester_id or PG_NULL, uid)

  -- Mirrors strife.py's queued_count increment on strife_track_intelligence /
  -- strife_user_track_affinity at queue time (both counter columns are NOT
  -- NULL DEFAULT 0 -- every counter this INSERT doesn't name explicitly
  -- still gets zero-filled by Postgres via that DEFAULT, same shape as
  -- track_intel_touch()/affinity_touch() further down).
  local key = track_key(video_url, title)
  db([[INSERT INTO strife_track_intelligence (guild_id, url_key, video_url, title, queued_count, last_requester_id, last_queued, source)
       VALUES (%s, %s, %s, %s, 1, %s, now(), 'queue')
       ON CONFLICT (guild_id, url_key) DO UPDATE SET queued_count = strife_track_intelligence.queued_count + 1,
         video_url = EXCLUDED.video_url, title = EXCLUDED.title, last_requester_id = EXCLUDED.last_requester_id,
         last_queued = now(), updated_at = now()]],
    guild_id, key, video_url or "", title or "", requester_id or PG_NULL)
  if requester_id then
    db([[INSERT INTO strife_user_track_affinity (guild_id, user_id, url_key, video_url, title, queued_count, last_requested, score)
         VALUES (%s, %s, %s, %s, %s, 1, now(), 0.15)
         ON CONFLICT (guild_id, user_id, url_key) DO UPDATE SET queued_count = strife_user_track_affinity.queued_count + 1,
           video_url = EXCLUDED.video_url, title = EXCLUDED.title, last_requested = now(),
           score = strife_user_track_affinity.score + 0.15, updated_at = now()]],
      guild_id, requester_id, key, video_url or "", title or "")
  end
end

-- Fisher-Yates shuffle that keeps the first row pinned in place (used
-- after /play adds a batch, so the track that's about to play next stays
-- put while the rest of the freshly-added batch gets mixed in).
local function shuffle_queue_rows(guild_id, preserve_first)
  local rows = db("SELECT video_url, title, requester_id, track_uid FROM strife_queue WHERE guild_id = %s AND bot_name = 'strife' ORDER BY id ASC", guild_id) or {}
  if #rows == 0 then return 0 end
  local head = nil
  local rest = rows
  if preserve_first then
    head = rows[1]
    rest = {}
    for i = 2, #rows do rest[#rest + 1] = rows[i] end
  end
  for i = #rest, 2, -1 do
    local j = math.random(1, i)
    rest[i], rest[j] = rest[j], rest[i]
  end
  db("DELETE FROM strife_queue WHERE guild_id = %s AND bot_name = 'strife'", guild_id)
  local ordered = {}
  if head then ordered[#ordered + 1] = head end
  for _, r in ipairs(rest) do ordered[#ordered + 1] = r end
  for _, r in ipairs(ordered) do
    db("INSERT INTO strife_queue (guild_id, bot_name, video_url, title, requester_id, track_uid) VALUES (%s, 'strife', %s, %s, %s, %s)",
      guild_id, r.video_url, r.title, r.requester_id or PG_NULL, r.track_uid or new_track_uid())
  end
  return #ordered
end

-- Mirrors strife.py's restore_queue_from_backup(): repopulates the live
-- queue from strife_queue_backup. process_queue_impl calls this as a
-- fallback when the live queue is empty and loop_mode is "queue", so a
-- backup snapshot surviving a corrupted/emptied live table (or a restart
-- mid-session) still resumes the loop rather than falling straight to
-- Auto-DJ or disconnecting.
local function restore_queue_from_backup(guild_id)
  local rows = db("SELECT video_url, title, requester_id, track_uid FROM strife_queue_backup WHERE guild_id = %s AND bot_name = 'strife' ORDER BY id ASC", guild_id) or {}
  for _, r in ipairs(rows) do
    db("INSERT INTO strife_queue (guild_id, bot_name, video_url, title, requester_id, track_uid) VALUES (%s, 'strife', %s, %s, %s, %s)",
      guild_id, r.video_url, r.title, r.requester_id or PG_NULL, r.track_uid or new_track_uid())
  end
  return #rows
end

-- ===========================================================================
-- Live playlist sync: auto-add newly appeared tracks / auto-remove tracks
-- gone from a queued playlist's source. Ported from alucard.py's
-- playlist_sync_loop -- this was cut fleet-wide during the Lua rewrite
-- (strife_active_playlists existed only as inert per-guild metadata,
-- written to by nothing but the DELETE calls scattered through /stop,
-- /clear, sleep-timer-elapsed, and queue-drained; is_playlist_source() was
-- defined but never called). Simplified vs. the Python original: no
-- YouTube Data API path (this stack only has Lavalink), so every
-- re-extraction is treated as equally trustworthy and a removal always
-- requires 2 consecutive missing cycles rather than distinguishing an
-- "API-trusted" cycle from a truncating-fallback one.
-- ===========================================================================

local PLAYLIST_SYNC_INTERVAL = tonumber(env("PLAYLIST_SYNC_INTERVAL", "30")) or 30
local PLAYLIST_SYNC_MAX_TRACKED = tonumber(env("PLAYLIST_SYNC_MAX_TRACKED", "500")) or 500
local PLAYLIST_QUEUE_MIN_TRACKS = tonumber(env("PLAYLIST_QUEUE_MIN_TRACKS", "20")) or 20
local playlist_missing_streak = {} -- guild_id -> {track_key -> consecutive cycles missing}

-- This file's only other trim() isn't defined until much later (an
-- autocomplete handler near the bottom) -- too late to be in scope here.
-- Local copy, scoped to this block only.
local function trim(s) return tostring(s or ""):match("^%s*(.-)%s*$") end

local function clear_active_playlist(guild_id)
  db("DELETE FROM strife_active_playlists WHERE guild_id = %s AND bot_name = 'strife'", guild_id)
  db("DELETE FROM strife_active_playlist_tracks WHERE guild_id = %s AND bot_name = 'strife'", guild_id)
  playlist_missing_streak[guild_id] = nil
end

-- track_key() itself caps at 64 chars (matches strife_track_intelligence's
-- url_key VARCHAR(64)), but strife_active_playlist_tracks.track_key is a
-- pre-existing CHAR(40) column -- overflowing it fails the INSERT, and
-- since that happened on every row of every playlist sync, it also tripped
-- the pg.lua reconnect-storm bug fixed above (same failure, same query, in
-- a loop). CHAR (not VARCHAR) also blank-pads on read, so values coming
-- back out of this column need trimming before comparing them against a
-- freshly computed key.
local function playlist_track_key(url, title)
  local k = track_key(url, title)
  if #k > 40 then k = k:sub(1, 40) end
  return k
end

-- Called right after a /play (or equivalent) call resolves to a real
-- Lavalink loadType=playlist response -- registers it so playlist_sync_tick
-- starts watching it for adds/removals.
local function set_active_playlist(guild_id, playlist_url, entries, requester_id, channel_id)
  if not playlist_url or not entries or #entries == 0 then return end
  db([[INSERT INTO strife_active_playlists (guild_id, bot_name, playlist_url, known_track_count, requester_id, channel_id)
      VALUES (%s, 'strife', %s, %s, %s, %s)
      ON CONFLICT (guild_id, bot_name) DO UPDATE SET playlist_url = EXCLUDED.playlist_url,
        known_track_count = EXCLUDED.known_track_count, requester_id = EXCLUDED.requester_id, channel_id = EXCLUDED.channel_id]],
    guild_id, playlist_url, math.min(#entries, PLAYLIST_SYNC_MAX_TRACKED), requester_id, channel_id)
  db("DELETE FROM strife_active_playlist_tracks WHERE guild_id = %s AND bot_name = 'strife'", guild_id)
  for idx, t in ipairs(entries) do
    if idx > PLAYLIST_SYNC_MAX_TRACKED then break end
    db([[INSERT INTO strife_active_playlist_tracks (guild_id, bot_name, playlist_url, position_idx, track_key, track_uid, video_url, title, requester_id)
        VALUES (%s, 'strife', %s, %s, %s, %s, %s, %s, %s)]],
      guild_id, playlist_url, idx - 1, playlist_track_key(t.uri, t.title), new_track_uid(), t.uri, t.title, requester_id)
  end
  playlist_missing_streak[guild_id] = nil
end

-- One sync cycle across every guild with a registered active playlist:
-- re-extracts each playlist via Lavalink, multiset-diffs it against the
-- last-known snapshot (strife_active_playlist_tracks), enqueues additions,
-- and removes tracks that have been missing for 2 consecutive cycles
-- (from both the live queue and its backup mirror) -- plus a loop-mode
-- duplicate trim and a queue-health refill, matching the Python original.
local function playlist_sync_tick()
  local playlists = db("SELECT guild_id, playlist_url, known_track_count, requester_id, channel_id FROM strife_active_playlists WHERE bot_name = 'strife'") or {}
  for _, prow in ipairs(playlists) do
    local gid = prow.guild_id
    local ok, err = pcall(function()
      local result, lerr = bot.lavalink:load_tracks(prow.playlist_url)
      if not result or result.loadType == "error" or result.loadType == "empty" then
        log_warn("[%s] playlist sync: re-extract failed: %s", gid, tostring((result and result.data and result.data.message) or lerr or "no response"))
        return
      end
      local raw_entries = (result.loadType == "playlist") and ((result.data and result.data.tracks) or {}) or {}
      local entries = {}
      for _, t in ipairs(raw_entries) do
        if t.info and t.info.uri then entries[#entries + 1] = { uri = t.info.uri, title = t.info.title } end
      end
      if #entries == 0 then
        log_warn("[%s] playlist sync: re-extract returned no tracks", gid)
        return
      end
      if #entries > PLAYLIST_SYNC_MAX_TRACKED then
        local trimmed = {}
        for i = 1, PLAYLIST_SYNC_MAX_TRACKED do trimmed[i] = entries[i] end
        entries = trimmed
      end

      local current_rows = {}
      local current_counts = {}
      for _, t in ipairs(entries) do
        local k = playlist_track_key(t.uri, t.title)
        current_rows[#current_rows + 1] = { url = t.uri, title = t.title, key = k }
        current_counts[k] = (current_counts[k] or 0) + 1
      end

      local previous_rows = db("SELECT track_key, track_uid, video_url, title, requester_id FROM strife_active_playlist_tracks WHERE guild_id = %s AND bot_name = 'strife' ORDER BY position_idx ASC", gid) or {}
      local previous_by_key = {}
      local remaining_previous = {}
      for _, p in ipairs(previous_rows) do
        local k = trim(p.track_key or "")
        previous_by_key[k] = previous_by_key[k] or {}
        table.insert(previous_by_key[k], p)
        remaining_previous[k] = (remaining_previous[k] or 0) + 1
      end

      local added_rows = {}
      for _, r in ipairs(current_rows) do
        if remaining_previous[r.key] and remaining_previous[r.key] > 0 then
          remaining_previous[r.key] = remaining_previous[r.key] - 1
        else
          added_rows[#added_rows + 1] = r
        end
      end

      local removed_counts = {}
      local carry_forward = {}
      local streaks = playlist_missing_streak[gid] or {}
      for k, missing_n in pairs(remaining_previous) do
        if missing_n > 0 then
          local streak = (streaks[k] or 0) + 1
          streaks[k] = streak
          if streak >= 2 then
            removed_counts[k] = missing_n
          else
            for i = 1, missing_n do
              local p = previous_by_key[k] and previous_by_key[k][i]
              if p then carry_forward[#carry_forward + 1] = p end
            end
          end
        end
      end
      for k in pairs(current_counts) do streaks[k] = nil end
      playlist_missing_streak[gid] = next(streaks) and streaks or nil

      local purged_live, purged_backup = 0, 0
      if next(removed_counts) ~= nil then
        local budget = {}
        for k, c in pairs(removed_counts) do budget[k] = c end
        local live_rows = db("SELECT id, video_url, title FROM strife_queue WHERE guild_id = %s AND bot_name = 'strife' ORDER BY id ASC", gid) or {}
        for _, lr in ipairs(live_rows) do
          local k = playlist_track_key(lr.video_url, lr.title)
          if budget[k] and budget[k] > 0 then
            budget[k] = budget[k] - 1
            db("DELETE FROM strife_queue WHERE id = %s AND guild_id = %s AND bot_name = 'strife'", lr.id, gid)
            purged_live = purged_live + 1
          end
        end
        budget = {}
        for k, c in pairs(removed_counts) do budget[k] = c end
        local backup_rows = db("SELECT id, video_url, title FROM strife_queue_backup WHERE guild_id = %s AND bot_name = 'strife' ORDER BY id ASC", gid) or {}
        for _, br in ipairs(backup_rows) do
          local k = playlist_track_key(br.video_url, br.title)
          if budget[k] and budget[k] > 0 then
            budget[k] = budget[k] - 1
            db("DELETE FROM strife_queue_backup WHERE id = %s AND guild_id = %s AND bot_name = 'strife'", br.id, gid)
            purged_backup = purged_backup + 1
          end
        end
      end

      if #added_rows > 0 then
        for _, r in ipairs(added_rows) do enqueue_track(gid, r.url, r.title, prow.requester_id) end
        snapshot_queue_backup(gid)
        if #added_rows > 1 then shuffle_queue_rows(gid, true) end
      end

      -- Trim loop-mode duplicate live-queue copies down to at most 1 per track_key still in the playlist.
      local trim_count = 0
      local current_keys = {}
      for _, r in ipairs(current_rows) do current_keys[r.key] = true end
      local all_rows = db("SELECT id, video_url, title FROM strife_queue WHERE guild_id = %s AND bot_name = 'strife' ORDER BY id ASC", gid) or {}
      local seen = {}
      for _, qr in ipairs(all_rows) do
        local k = playlist_track_key(qr.video_url, qr.title)
        if current_keys[k] then
          if seen[k] then
            db("DELETE FROM strife_queue WHERE id = %s AND guild_id = %s AND bot_name = 'strife'", qr.id, gid)
            trim_count = trim_count + 1
          else
            seen[k] = true
          end
        end
      end

      -- Queue-health refill: independent of the diff above, top the live queue back up
      -- if it's thinner than expected relative to the tracked playlist (drained by
      -- playback/a bug/a restart without the source playlist itself changing).
      local refilled = 0
      local queued_rows = db("SELECT video_url, title FROM strife_queue WHERE guild_id = %s AND bot_name = 'strife'", gid) or {}
      local queued_keys = {}
      local queued_n = 0
      for _, qr in ipairs(queued_rows) do
        local k = playlist_track_key(qr.video_url, qr.title)
        if not queued_keys[k] then queued_keys[k] = true; queued_n = queued_n + 1 end
      end
      local target = math.min(#current_rows, PLAYLIST_QUEUE_MIN_TRACKS)
      if queued_n < target then
        local refill_rows = {}
        for _, r in ipairs(current_rows) do
          if queued_n + #refill_rows >= target then break end
          if not queued_keys[r.key] then refill_rows[#refill_rows + 1] = r end
        end
        for _, r in ipairs(refill_rows) do enqueue_track(gid, r.url, r.title, prow.requester_id) end
        if #refill_rows > 0 then
          refilled = #refill_rows
          snapshot_queue_backup(gid)
          shuffle_queue_rows(gid, true)
        end
      end

      if #added_rows > 0 or purged_live > 0 or purged_backup > 0 or trim_count > 0 or refilled > 0 then
        log_info("[%s] playlist sync: +%d -%d(live) -%d(backup) trimmed=%d refilled=%d", gid, #added_rows, purged_live, purged_backup, trim_count, refilled)
      end

      -- Persist this cycle's confirmed tracks plus anything still in its missing-streak
      -- grace window as the baseline the next cycle diffs against.
      db("DELETE FROM strife_active_playlist_tracks WHERE guild_id = %s AND bot_name = 'strife'", gid)
      local idx = 0
      for _, r in ipairs(current_rows) do
        db([[INSERT INTO strife_active_playlist_tracks (guild_id, bot_name, playlist_url, position_idx, track_key, track_uid, video_url, title, requester_id)
            VALUES (%s, 'strife', %s, %s, %s, %s, %s, %s, %s)]],
          gid, prow.playlist_url, idx, r.key, new_track_uid(), r.url, r.title, prow.requester_id)
        idx = idx + 1
      end
      for _, p in ipairs(carry_forward) do
        db([[INSERT INTO strife_active_playlist_tracks (guild_id, bot_name, playlist_url, position_idx, track_key, track_uid, video_url, title, requester_id)
            VALUES (%s, 'strife', %s, %s, %s, %s, %s, %s, %s)]],
          gid, prow.playlist_url, idx, trim(p.track_key or ""), trim(p.track_uid or "") ~= "" and trim(p.track_uid) or new_track_uid(), p.video_url, p.title, p.requester_id)
        idx = idx + 1
      end
      db("UPDATE strife_active_playlists SET known_track_count = %s WHERE guild_id = %s AND bot_name = 'strife'", idx, gid)
    end)
    if not ok then
      log_warn("[%s] playlist sync tick error: %s", gid, tostring(err))
      report_error(gid, "runtime", "playlist sync error", tostring(err))
    end
  end
end


-- =====================================================================
-- LAVALINK TRACK RESOLUTION
-- =====================================================================

local function is_explicit_query(value)
  value = tostring(value or ""):lower()
  if value:match("^https?://") then return true end
  return value:match("^[a-z0-9_]+search:") ~= nil
end

-- Ported alongside alucard.lua/lockhart.lua/etc's copy for playlist_sync_tick below.
local function is_playlist_source(v)
  v = tostring(v or "")
  if not v:match("^https?://") then return false end
  if v:match("[?&]list=") then return true end
  local lowered = v:lower()
  return lowered:find("/playlist", 1, true) ~= nil or lowered:find("/sets/", 1, true) ~= nil
end

-- Resolves a search string / URL into one or more Lavalink track objects
-- ({ encoded=, info={title=,uri=,length=,...} }). Mirrors strife.py's
-- search_playables()/unwrap_search_results(): a plain-text search embraces
-- the whole ranked result batch (not just the top hit) so /play on loose
-- text queues a small, shuffled "result bundle" -- a deliberate quirk of
-- the original bot worth keeping, capped here to avoid pathological batch
-- sizes. Direct URLs/playlists behave as you'd expect.
local function resolve_playables(query)
  query = tostring(query or ""):match("^%s*(.-)%s*$")
  if query == "" then return nil, "empty query" end
  if not is_explicit_query(query) then
    query = "ytmsearch:" .. query
  end
  local result, err = bot.lavalink:load_tracks(query)
  if not result then return nil, err or "lavalink request failed" end
  local load_type = result.loadType
  if load_type == "error" then
    local msg = result.data and result.data.message or "track load error"
    return nil, msg
  elseif load_type == "empty" then
    return nil, "nothing playable came back for that search"
  elseif load_type == "track" then
    return { result.data }, false
  elseif load_type == "search" then
    local entries = {}
    for i, t in ipairs(result.data or {}) do
      if i > 10 then break end
      entries[#entries + 1] = t
    end
    return entries, false
  elseif load_type == "playlist" then
    local entries = {}
    for _, t in ipairs((result.data and result.data.tracks) or {}) do
      entries[#entries + 1] = t
    end
    local name = result.data and result.data.info and result.data.info.name
    return entries, name or true
  end
  return nil, "unrecognized load type: " .. tostring(load_type)
end

-- =====================================================================
-- AUDIO FILTER PRESETS (ported 1:1 from strife.py's apply_filter_preset /
-- _blend_loudnorm -- same band values, same speed/pitch numbers)
-- =====================================================================

local FILTER_CHOICES = {
  { name = "None (Standard high quality audio)", value = "none" },
  { name = "Bassboost", value = "bassboost" },
  { name = "Nightcore", value = "nightcore" },
  { name = "Vaporwave", value = "vaporwave" },
  { name = "8D Rotation", value = "8d" },
  { name = "Karaoke", value = "karaoke" },
  { name = "Tremolo", value = "tremolo" },
  { name = "Vibrato", value = "vibrato" },
  { name = "Low Pass", value = "lowpass" },
  { name = "Lo-fi", value = "lofi" },
  { name = "Electronic", value = "electronic" },
  { name = "Party", value = "party" },
  { name = "Radio", value = "radio" },
  { name = "Cinema", value = "cinema" },
  { name = "Soft", value = "soft" },
  { name = "Pop", value = "pop" },
  { name = "Rock", value = "rock" },
  { name = "Classical", value = "classical" },
  { name = "Ear Rape", value = "earrape" },
  { name = "Double Time (2x)", value = "doubletime" },
  { name = "Slow Mo (0.5x)", value = "slowmo" },
  { name = "Pitch Up", value = "pitch_up" },
  { name = "Pitch Down", value = "pitch_down" },
  { name = "Deep", value = "deep" },
  { name = "Anime", value = "anime" },
}
local FILTER_VALUES = {}
for _, c in ipairs(FILTER_CHOICES) do FILTER_VALUES[c.value] = c.name end

local LOUDNORM_EQ = {
  {0,-0.05},{1,-0.03},{2,0.0},{3,0.0},{4,0.0},{5,0.0},{6,0.0},{7,-0.01},
  {8,-0.02},{9,-0.02},{10,-0.03},{11,-0.02},{12,0.01},{13,0.01},{14,0.0},
}

local function blend_loudnorm(preset_bands)
  local base = {}
  for _, b in ipairs(LOUDNORM_EQ) do base[b[1]] = b[2] end
  for _, b in ipairs(preset_bands or {}) do
    base[b[1]] = (base[b[1]] or 0.0) + b[2]
  end
  local out = {}
  for band = 0, 14 do
    out[#out + 1] = { band = band, gain = math.floor((base[band] or 0.0) * 10000 + 0.5) / 10000 }
  end
  return out
end

-- Builds a Lavalink v4 `filters` REST payload for one preset name.
-- Returns (filters_table, resulting_speed).
local function build_filter_payload(mode, current_speed)
  mode = tostring(mode or "none"):lower():gsub("%s", "")
  local speed = current_speed or 1.0
  local f = {}
  local preset_uses_eq = false

  if mode == "nightcore" then
    f.timescale = { speed = 1.25, pitch = 1.3 }; speed = 1.25
  elseif mode == "vaporwave" then
    f.timescale = { speed = 0.8, pitch = 0.8 }; speed = 0.8
  elseif mode == "bassboost" then
    preset_uses_eq = true
    f.equalizer = blend_loudnorm({ {0,0.32}, {1,0.24}, {2,0.12} })
  elseif mode == "8d" then
    f.rotation = { rotationHz = 0.18 }
    f.channelMix = { leftToLeft = 0.8, leftToRight = 0.2, rightToLeft = 0.2, rightToRight = 0.8 }
  elseif mode == "karaoke" then
    f.karaoke = { level = 1.0, monoLevel = 1.0, filterBand = 220.0, filterWidth = 100.0 }
  elseif mode == "tremolo" then
    f.tremolo = { frequency = 4.0, depth = 0.45 }
  elseif mode == "vibrato" then
    f.vibrato = { frequency = 4.5, depth = 0.35 }
  elseif mode == "lowpass" or mode == "lofi" then
    f.lowPass = { smoothing = mode == "lowpass" and 20.0 or 35.0 }
    if mode == "lofi" then f.timescale = { speed = 0.94, pitch = 0.96 }; speed = 0.94 end
  elseif mode == "electronic" then
    preset_uses_eq = true
    f.equalizer = blend_loudnorm({ {0,0.12}, {1,0.10}, {4,-0.05}, {8,0.08}, {10,0.14} })
  elseif mode == "party" then
    preset_uses_eq = true
    f.equalizer = blend_loudnorm({ {0,0.25}, {1,0.18}, {2,0.08}, {9,0.10} })
  elseif mode == "radio" then
    preset_uses_eq = true
    f.equalizer = blend_loudnorm({ {0,-0.18}, {1,-0.10}, {4,0.12}, {5,0.12}, {10,-0.12} })
    f.lowPass = { smoothing = 18.0 }
  elseif mode == "cinema" then
    preset_uses_eq = true
    f.equalizer = blend_loudnorm({ {0,0.18}, {1,0.12}, {8,0.08}, {9,0.10} })
  elseif mode == "soft" then
    f.lowPass = { smoothing = 25.0 }
  elseif mode == "pop" then
    preset_uses_eq = true
    f.equalizer = blend_loudnorm({ {0,-0.1},{1,0.1},{2,0.2},{3,0.25},{4,0.2},{5,0.1},{6,0.0},{7,-0.05},{8,-0.1},{9,-0.1},{10,-0.05},{11,0.0},{12,0.1},{13,0.2},{14,0.1} })
  elseif mode == "rock" then
    preset_uses_eq = true
    f.equalizer = blend_loudnorm({ {0,0.3},{1,0.25},{2,0.1},{3,-0.1},{4,-0.15},{5,-0.1},{6,0.0},{7,0.1},{8,0.3},{9,0.35},{10,0.3},{11,0.2},{12,0.1},{13,0.05},{14,0.0} })
  elseif mode == "classical" then
    preset_uses_eq = true
    f.equalizer = blend_loudnorm({ {0,0.1},{1,0.1},{2,0.05},{3,0.0},{4,-0.05},{5,-0.05},{6,-0.05},{7,-0.05},{8,-0.05},{9,-0.05},{10,0.0},{11,0.05},{12,0.1},{13,0.15},{14,0.2} })
  elseif mode == "earrape" then
    f.distortion = { sinOffset=0.0, sinScale=1.5, cosOffset=0.0, cosScale=1.5, tanOffset=0.0, tanScale=1.0, offset=0.0, scale=1.5 }
  elseif mode == "doubletime" then
    f.timescale = { speed = 2.0, pitch = 1.0 }; speed = 2.0
  elseif mode == "slowmo" then
    f.timescale = { speed = 0.5, pitch = 1.0 }; speed = 0.5
  elseif mode == "pitch_up" then
    f.timescale = { speed = 1.0, pitch = 1.3 }
  elseif mode == "pitch_down" then
    f.timescale = { speed = 1.0, pitch = 0.7 }
  elseif mode == "deep" then
    preset_uses_eq = true
    f.equalizer = blend_loudnorm({ {0,0.5}, {1,0.4}, {2,0.3} })
    f.timescale = { speed = 1.0, pitch = 0.8 }
  elseif mode == "anime" then
    f.timescale = { speed = 1.4, pitch = 1.5 }; speed = 1.4
  end

  if not preset_uses_eq then
    local eq = {}
    for _, b in ipairs(LOUDNORM_EQ) do eq[#eq + 1] = { band = b[1], gain = b[2] } end
    f.equalizer = eq
  end
  return f, speed
end

-- =====================================================================
-- VOICE CONNECTION
-- =====================================================================

-- strife_voice_state bookkeeping so an external dashboard (or a future
-- reconnect-on-restart pass) can see where the bot is/should be connected.
-- reconnect_attempts has an explicit NOT NULL DEFAULT 0 in the schema above,
-- so Postgres zero-fills it here without this INSERT needing to name it.
local function mark_voice_connected(guild_id, channel_id)
  db([[INSERT INTO strife_voice_state (guild_id, bot_name, connected_channel_id, last_channel_id, desired_connected, last_seen_at)
       VALUES (%s, 'strife', %s, %s, true, now())
       ON CONFLICT (guild_id, bot_name) DO UPDATE SET connected_channel_id = EXCLUDED.connected_channel_id,
         last_channel_id = EXCLUDED.last_channel_id, desired_connected = true, last_seen_at = now()]],
    guild_id, channel_id, channel_id)
end

local function mark_voice_disconnected(guild_id)
  db([[UPDATE strife_voice_state SET desired_connected = false, connected_channel_id = %s, disconnected_at = now()
       WHERE guild_id = %s AND bot_name = 'strife']], PG_NULL, guild_id)
end

local function connect_voice(guild_id, channel_id)
  if voice_channel[guild_id] == channel_id then return end
  bot.gateway:join_voice(guild_id, channel_id, false, true) -- self-deafened, matches strife.py's ensure_self_deaf
  voice_channel[guild_id] = channel_id
  mark_voice_connected(guild_id, channel_id)
end

local function disconnect_voice(guild_id)
  bot.gateway:leave_voice(guild_id)
  voice_channel[guild_id] = nil
  mark_voice_disconnected(guild_id)
end

local function members_in_channel(guild_id, channel_id)
  local out = {}
  local states = voice_states[guild_id]
  if not states then return out end
  for user_id, st in pairs(states) do
    if st.channel_id == channel_id and not st.is_bot then
      out[#out + 1] = user_id
    end
  end
  return out
end

-- =====================================================================
-- PLAYBACK ENGINE
-- =====================================================================

local function persist_playback_state(guild_id, patch)
  local existing = db1("SELECT guild_id FROM strife_playback_state WHERE guild_id = %s AND bot_name = 'strife'", guild_id)
  local cols, vals = {}, {}
  for k, v in pairs(patch) do cols[#cols + 1] = k; vals[#vals + 1] = v end
  if not existing then
    local names, placeholders, args = { "guild_id", "bot_name" }, { "%s", "'strife'" }, { guild_id }
    for _, k in ipairs(cols) do
      names[#names + 1] = k
      placeholders[#placeholders + 1] = "%s"
    end
    local sql = ("INSERT INTO strife_playback_state (%s) VALUES (%s)"):format(table.concat(names, ", "), table.concat(placeholders, ", "))
    for _, v in ipairs(vals) do args[#args + 1] = (v == nil and PG_NULL or v) end
    db(sql, unpack(args))
  else
    local sets = {}
    local args = {}
    for _, k in ipairs(cols) do sets[#sets + 1] = k .. " = %s" end
    local sql = ("UPDATE strife_playback_state SET %s WHERE guild_id = %%s AND bot_name = 'strife'"):format(table.concat(sets, ", "))
    for _, v in ipairs(vals) do args[#args + 1] = (v == nil and PG_NULL or v) end
    args[#args + 1] = guild_id
    db(sql, unpack(args))
  end
end

local function record_history(guild_id, video_url, title, requester_id)
  db("INSERT INTO strife_history (guild_id, video_url, title, requester_id) VALUES (%s, %s, %s, %s)",
    guild_id, video_url, title, requester_id or PG_NULL)
end

-- Cheap engagement-scoring writes used by /like /dislike /taste /recommend.
local function track_intel_touch(guild_id, video_url, title, requester_id, field_name)
  local key = track_key(video_url, title)
  local exists = db1("SELECT url_key FROM strife_track_intelligence WHERE guild_id = %s AND url_key = %s", guild_id, key)
  if not exists then
    db([[INSERT INTO strife_track_intelligence (guild_id, url_key, video_url, title, last_requester_id)
         VALUES (%s, %s, %s, %s, %s) ON CONFLICT (guild_id, url_key) DO NOTHING]],
      guild_id, key, video_url or "", title or "", requester_id or PG_NULL)
  end
  if field_name then
    db(("UPDATE strife_track_intelligence SET %s = %s + 1, updated_at = now() WHERE guild_id = %%s AND url_key = %%s"):format(field_name, field_name),
      guild_id, key)
  end
  return key
end

local function affinity_touch(guild_id, user_id, video_url, title, field_name, score_delta)
  local key = track_key(video_url, title)
  local exists = db1("SELECT url_key FROM strife_user_track_affinity WHERE guild_id = %s AND user_id = %s AND url_key = %s", guild_id, user_id, key)
  if not exists then
    db([[INSERT INTO strife_user_track_affinity (guild_id, user_id, url_key, video_url, title)
         VALUES (%s, %s, %s, %s, %s) ON CONFLICT (guild_id, user_id, url_key) DO NOTHING]],
      guild_id, user_id, key, video_url or "", title or "")
  end
  local sets = { "updated_at = now()", "last_requested = now()" }
  if field_name then sets[#sets + 1] = field_name .. " = " .. field_name .. " + 1" end
  if score_delta then sets[#sets + 1] = ("score = score + (%s)"):format(score_delta) end
  db(("UPDATE strife_user_track_affinity SET %s WHERE guild_id = %%s AND user_id = %%s AND url_key = %%s"):format(table.concat(sets, ", ")),
    guild_id, user_id, key)
end

local function requester_bot_id() return bot.user_id end

-- Forward declarations resolved later in the file (process_queue and the
-- Auto-DJ picker are mutually referential: an empty queue tries Auto-DJ,
-- and a successful Auto-DJ pick calls back into process_queue to play it).
local autodj_enqueue

-- Pops the next row off the live queue, resolves it against Lavalink, and
-- starts playback. Bad rows (dead links, load failures) are dropped and
-- the next one is tried, bounded so a run of dead tracks can't recurse
-- forever. Returns true if something started playing.
-- Port of alucard.py's update_stage_topic/clear_voice_channel_status: without
-- this, a stage channel just sits showing "waiting" in Discord's UI and the
-- member list never shows what's playing, even though audio is flowing fine
-- -- these are purely cosmetic Discord-side signals, not required for audio,
-- but users expect them. Channel type is memoized since it never changes.
local channel_kind_cache = {} -- channel_id -> "stage" | "voice"
local last_stage_topic = {}   -- guild_id -> last topic string sent (dedup)

local function get_channel_kind(channel_id)
  if not channel_id then return "voice" end
  local cached = channel_kind_cache[channel_id]
  if cached then return cached end
  local ch = bot.rest:get("/channels/" .. tostring(channel_id))
  local kind = (ch and ch.type == 13) and "stage" or "voice"
  channel_kind_cache[channel_id] = kind
  return kind
end

local function update_stage_topic(guild_id, channel_id, title)
  if not channel_id then return end
  local ok, err = pcall(function()
    local safe_title = tostring(title or "Unknown Track"):gsub("\n", " "):sub(1, 60)
    local topic = "\xF0\x9F\x8E\xB5 " .. safe_title
    if last_stage_topic[guild_id] == topic then return end
    if get_channel_kind(channel_id) == "stage" then
      local patched = bot.rest:patch("/stage-instances/" .. tostring(channel_id), { topic = topic })
      if not patched then
        bot.rest:post("/stage-instances", { channel_id = tostring(channel_id), topic = topic, privacy_level = 2 })
      end
    else
      bot.rest:patch("/channels/" .. tostring(channel_id), { status = topic })
    end
    last_stage_topic[guild_id] = topic
  end)
  if not ok then print(("[strife] stage/voice topic update failed for %s: %s"):format(tostring(guild_id), tostring(err))) end
end

local function clear_stage_topic(guild_id, channel_id)
  last_stage_topic[guild_id] = nil
  if not channel_id then return end
  pcall(function()
    if get_channel_kind(channel_id) == "stage" then
      bot.rest:delete("/stage-instances/" .. tostring(channel_id))
    else
      bot.rest:patch("/channels/" .. tostring(channel_id), { status = cjson.null })
    end
  end)
end

-- guild_id..":"..track_uid -> consecutive resolve-failure count. A Lavalink/
-- network blip mid-drain used to permanently delete every remaining queued
-- track (each dequeued row was gone before resolve_playables even ran, and
-- a failed resolve just deleted it -- backup copy included -- and moved to
-- the next row), so a short outage could wipe an entire queue in seconds.
local resolve_attempts = {}
local MAX_RESOLVE_ATTEMPTS = 3

local function process_queue_impl(guild_id, channel_id, depth)
  depth = depth or 0
  if depth > 5 then return false end
  if not (bot.lavalink and bot.lavalink.session_id) then
    -- Lavalink isn't connected right now -- leave the queue untouched
    -- rather than dequeuing tracks we can't resolve; the watchdog thread
    -- and the next natural trigger will retry once it's back.
    return false
  end
  connect_voice(guild_id, channel_id)

  local row = db1("SELECT id, video_url, title, requester_id, track_uid FROM strife_queue WHERE guild_id = %s AND bot_name = 'strife' ORDER BY id ASC LIMIT 1", guild_id)
  if not row then
    -- Queue is empty: if loop_mode is "queue", try restoring from the
    -- backup mirror first (recovers a session whose live queue got
    -- emptied/corrupted out from under it), then fall back to Auto-DJ.
    local settings_peek = db1("SELECT loop_mode FROM strife_guild_settings WHERE guild_id = %s", guild_id)
    if settings_peek and settings_peek.loop_mode == "queue" then
      local restored = restore_queue_from_backup(guild_id)
      if restored > 0 then
        row = db1("SELECT id, video_url, title, requester_id, track_uid FROM strife_queue WHERE guild_id = %s AND bot_name = 'strife' ORDER BY id ASC LIMIT 1", guild_id)
      end
    end
    if not row and get_autodj_enabled(guild_id) and autodj_enqueue then
      local ok = autodj_enqueue(guild_id, channel_id)
      if ok then return process_queue_impl(guild_id, channel_id, depth + 1) end
    end
    if not row then
      playback[guild_id] = nil
      clear_stage_topic(guild_id, channel_id)
      persist_playback_state(guild_id, { is_playing = false, is_paused = false, video_url = PG_NULL, title = PG_NULL, track_uid = PG_NULL })
      return false
    end
  end

  local retry_key = guild_id .. ":" .. tostring(row.track_uid or row.video_url)
  local entries, err = resolve_playables(row.video_url)
  if not entries or #entries == 0 then
    local attempts = (resolve_attempts[retry_key] or 0) + 1
    db("DELETE FROM strife_queue WHERE id = %s", row.id)
    if attempts < MAX_RESOLVE_ATTEMPTS then
      resolve_attempts[retry_key] = attempts
      print(("[strife] resolve failed for '%s' (attempt %d/%d): %s -- requeuing"):format(tostring(row.video_url), attempts, MAX_RESOLVE_ATTEMPTS, tostring(err)))
      insert_queue_front(guild_id, row.video_url, row.title, row.requester_id, row.track_uid)
      return false
    end
    resolve_attempts[retry_key] = nil
    print(("[strife] giving up on '%s' after %d failed resolves: %s"):format(tostring(row.video_url), attempts, tostring(err)))
    report_error(guild_id, "runtime", "track resolve failed permanently", ("%s: %s"):format(tostring(row.video_url), tostring(err)))
    delete_backup_track(guild_id, row.track_uid, row.video_url, row.title)
    return process_queue_impl(guild_id, channel_id, depth + 1)
  end
  resolve_attempts[retry_key] = nil
  local track = entries[1]

  db("DELETE FROM strife_queue WHERE id = %s", row.id)

  local settings = get_guild_settings(guild_id)
  local filters, speed = build_filter_payload(settings.filter_mode or "none", 1.0)
  if settings.filter_stack and settings.filter_stack ~= "" and settings.filter_stack ~= settings.filter_mode then
    local f2, speed2 = build_filter_payload(settings.filter_stack, speed)
    for k, v in pairs(f2) do filters[k] = v end
    speed = speed2
  end
  if settings.custom_modifiers_left and settings.custom_modifiers_left > 0 then
    filters = { timescale = { speed = settings.custom_speed or 1.0, pitch = settings.custom_pitch or 1.0 } }
    speed = settings.custom_speed or 1.0
    db("UPDATE strife_guild_settings SET custom_modifiers_left = custom_modifiers_left - 1 WHERE guild_id = %s", guild_id)
  end

  -- /fade: "fade"/"smart"/"mix" all start the new track at volume 0 and ramp
  -- up to the configured volume over fade_seconds (mirrors strife.py's
  -- transition behavior at the level this Lavalink-direct stack can actually
  -- reach -- see the /fade command's "mix" note re: no BPM-matched crossfade).
  local target_volume = settings.volume or 100
  local use_fade = settings.transition_mode == "fade" or settings.transition_mode == "smart" or settings.transition_mode == "mix"

  bot.lavalink:update_player(guild_id, {
    encodedTrack = track.encoded,
    volume = use_fade and 0 or target_volume,
    filters = filters,
  })

  local duration = track.info and math.floor((track.info.length or 0) / 1000) or 0
  local title = row.title or (track.info and track.info.title) or "Unknown"
  update_stage_topic(guild_id, channel_id, title)
  playback[guild_id] = {
    url = row.video_url, title = title, duration = duration, position = 0,
    requester_id = row.requester_id, channel_id = channel_id, track_uid = row.track_uid,
    current_filter = settings.filter_mode or "none", speed = speed, paused = false,
    started_at = os.time(),
  }

  fade_tokens[guild_id] = (fade_tokens[guild_id] or 0) + 1 -- cancel any fade still ramping from the previous track
  if use_fade then
    local my_token = fade_tokens[guild_id]
    local fade_seconds = math.max(0.25, tonumber(settings.fade_seconds) or 3.0)
    copas.addthread(function()
      local steps = 20
      for i = 1, steps do
        if fade_tokens[guild_id] ~= my_token then return end
        copas.sleep(fade_seconds / steps)
        if fade_tokens[guild_id] ~= my_token then return end
        bot.lavalink:set_volume(guild_id, math.floor((i / steps) * target_volume))
      end
    end)
  end
  persist_playback_state(guild_id, {
    channel_id = channel_id, video_url = row.video_url, title = title,
    position_seconds = 0, is_playing = true, is_paused = false, track_uid = row.track_uid,
  })
  track_intel_touch(guild_id, row.video_url, title, row.requester_id, "play_count")
  if row.requester_id then affinity_touch(guild_id, row.requester_id, row.video_url, title, "play_count", nil) end
  return true
end

-- Public entry point: wraps process_queue_impl in a pcall so a bad row,
-- a Lavalink hiccup, or a DB error surfaces in strife_error_events / the
-- error webhook instead of silently killing the calling command/event
-- handler (mirrors lockhart.lua's/tunestream.lua's process_queue error
-- reporting).
local function process_queue(guild_id, channel_id)
  local ok, err = pcall(process_queue_impl, guild_id, channel_id, 0)
  if not ok then
    print("[strife] process_queue error: " .. tostring(err))
    report_error(guild_id, "runtime", "process_queue error", tostring(err))
  end
end

local function stop_playback(guild_id)
  bot.lavalink:stop(guild_id)
  -- home_channel() isn't in scope yet at this point in the file (defined
  -- further down), so this only clears using the in-memory channel_id.
  clear_stage_topic(guild_id, playback[guild_id] and playback[guild_id].channel_id)
  playback[guild_id] = nil
  fade_tokens[guild_id] = (fade_tokens[guild_id] or 0) + 1 -- cancel any in-flight fade-in
  persist_playback_state(guild_id, { is_playing = false, is_paused = false, video_url = PG_NULL, title = PG_NULL, track_uid = PG_NULL })
end

local function current_position(guild_id)
  local data = playback[guild_id]
  if not data then return 0 end
  if data.paused then return data.position or 0 end
  local elapsed = os.time() - (data.updated_at or data.started_at or os.time())
  return math.max(0, (data.position or 0) + elapsed)
end

-- =====================================================================
-- SMART AUTO-DJ / RECOMMENDATIONS
-- (simplified count-based scoring -- strife.py's embedding/BPM/loudness
-- analysis leaned on yt-dlp + aubio audio feature extraction that has no
-- Lua equivalent available here; see final report for the honest scope
-- cut. Likes/dislikes/play/finish/skip counts still drive real picks.)
-- =====================================================================

-- Score formula: reward finishes and likes, penalize skips and dislikes.
local function pick_smart_recommendation(guild_id, listener_ids, avoid_key)
  local rows = db([[
    SELECT url_key, video_url, title,
           (play_count + finish_count * 2 + like_count * 3 - skip_count * 2 - dislike_count * 4) AS score
    FROM strife_track_intelligence
    WHERE guild_id = %s
    ORDER BY score DESC, last_played DESC NULLS LAST
    LIMIT 25
  ]], guild_id) or {}

  local candidates = {}
  for _, r in ipairs(rows) do
    if r.video_url and r.video_url ~= "" and r.url_key ~= avoid_key then
      candidates[#candidates + 1] = r
    end
  end

  if #candidates == 0 then
    -- Fall back to the server's play history: resurface something that
    -- was popular before track_intelligence had anything to say.
    local hist = db([[SELECT title, video_url, COUNT(*) AS n FROM strife_history
                       WHERE guild_id = %s GROUP BY title, video_url ORDER BY n DESC LIMIT 10]], guild_id) or {}
    for _, r in ipairs(hist) do
      candidates[#candidates + 1] = { video_url = r.video_url, title = r.title, score = r.n }
    end
  end
  if #candidates == 0 then return nil, nil end

  -- Weighted-random pick biased toward the top of the list, so the
  -- "best" pick isn't always the exact same track every time.
  local total_weight = 0
  local weights = {}
  for i, c in ipairs(candidates) do
    local w = math.max(1, (#candidates - i + 1))
    weights[i] = w
    total_weight = total_weight + w
  end
  local roll = math.random() * total_weight
  local chosen = candidates[1]
  local acc = 0
  for i, c in ipairs(candidates) do
    acc = acc + weights[i]
    if roll <= acc then chosen = c; break end
  end

  -- Re-resolve through Lavalink so we always play a fresh, valid track.
  local entries = resolve_playables(chosen.video_url)
  if not entries or #entries == 0 then
    entries = resolve_playables(chosen.title)
  end
  if not entries or #entries == 0 then return nil, nil end
  return entries[1], chosen
end

autodj_enqueue = function(guild_id, channel_id)
  local listener_ids = members_in_channel(guild_id, channel_id)
  local track, reco = pick_smart_recommendation(guild_id, listener_ids, nil)
  if not track then return false end
  local title = track.info and track.info.title or (reco and reco.title) or "Unknown"
  local uri = track.info and track.info.uri or (reco and reco.video_url)
  enqueue_track(guild_id, uri, title, requester_bot_id(), new_track_uid())
  db([[INSERT INTO strife_smart_recommendations (guild_id, requester_id, chosen_url, chosen_title, reason)
       VALUES (%s, %s, %s, %s, 'autodj')]], guild_id, requester_bot_id() or PG_NULL, uri, title)
  print(("[strife] Auto-DJ queued '%s' for guild %s"):format(tostring(title), tostring(guild_id)))
  return true
end

local function get_current_track_snapshot(guild_id)
  local data = playback[guild_id]
  if not data then return nil end
  return { url = data.url, title = data.title, track_uid = data.track_uid }
end

local function record_track_feedback(guild_id, user_id, video_url, title, liked)
  track_intel_touch(guild_id, video_url, title, nil, liked and "like_count" or "dislike_count")
  affinity_touch(guild_id, user_id, video_url, title, liked and "like_count" or "dislike_count", liked and 5 or -5)
end

-- =====================================================================
-- LYRICS (best-effort, plain text only -- no synced-line timing without
-- the audio-analysis stack strife.py had via yt-dlp/aubio)
-- =====================================================================

local function http_get_json(url)
  local response_body = {}
  local ok, status = https.request({ url = url, method = "GET", sink = ltn12.sink.table(response_body) })
  if not ok or (type(status) == "number" and status >= 400) then return nil end
  local raw = table.concat(response_body)
  local ok2, decoded = pcall(cjson.decode, raw)
  if not ok2 then return nil end
  return decoded
end

local function url_encode(s)
  return (tostring(s):gsub("([^%w %-%_%.%~])", function(c) return ("%%%02X"):format(c:byte()) end):gsub(" ", "+"))
end

local function fetch_lyrics(title)
  title = tostring(title or "")
  local artist, song = title:match("^(.-)%s*[-\226\128\147]%s*(.+)$")
  if not artist or not song then
    artist, song = "", title
  end
  local url = ("https://api.lyrics.ovh/v1/%s/%s"):format(url_encode(artist ~= "" and artist or song), url_encode(song))
  local ok, data = pcall(http_get_json, url)
  if not ok or not data or not data.lyrics then return nil end
  return data.lyrics
end

-- =====================================================================
-- GATEWAY: voice-state tracking (who's in which channel -- used for
-- listener counts on /voteskip /upvote /downvote and Auto-DJ context)
-- =====================================================================

bot.gateway:on("VOICE_STATE_UPDATE", function(d)
  local guild_id = d.guild_id
  if not guild_id then return end
  voice_states[guild_id] = voice_states[guild_id] or {}
  if not d.channel_id or d.channel_id == cjson.null then
    voice_states[guild_id][d.user_id] = nil
  else
    voice_states[guild_id][d.user_id] = {
      channel_id = d.channel_id,
      is_bot = d.member and d.member.user and d.member.user.bot or (d.user_id == bot.user_id),
    }
  end
  if d.user_id == bot.user_id then
    if not d.channel_id or d.channel_id == cjson.null then
      voice_channel[guild_id] = nil
    else
      voice_channel[guild_id] = d.channel_id
    end
  end
end)

-- =====================================================================
-- LAVALINK EVENTS
-- =====================================================================

bot.lavalink:on("playerUpdate", function(msg)
  local guild_id = msg.guildId
  local data = playback[guild_id]
  if not data then return end
  local state = msg.state or {}
  data.position = math.floor((state.position or 0) / 1000)
  data.updated_at = os.time()
  -- Cheap periodic checkpoint so a crash/restart can see roughly where
  -- playback was; not a full recovery system (see report for the gap).
  persist_playback_state(guild_id, { position_seconds = data.position })
end)

local function handle_track_end(guild_id, reason, track_uid, video_url, title, requester_id, duration)
  if reason == "REPLACED" then return end -- another play() call already superseded this one

  if reason == "FINISHED" then
    track_intel_touch(guild_id, video_url, title, requester_id, "finish_count")
    if requester_id then affinity_touch(guild_id, requester_id, video_url, title, "finish_count", 2) end
    record_history(guild_id, video_url, title, requester_id)

    local settings = get_guild_settings(guild_id)
    local loop_mode = settings.loop_mode or "queue"
    if loop_mode == "queue" then
      enqueue_track(guild_id, video_url, title, requester_id, new_track_uid())
    elseif loop_mode == "song" then
      insert_queue_front(guild_id, video_url, title, requester_id, track_uid or new_track_uid())
    else
      delete_backup_track(guild_id, track_uid, video_url, title)
    end
  else
    track_intel_touch(guild_id, video_url, title, requester_id, "skip_count")
    if requester_id then affinity_touch(guild_id, requester_id, video_url, title, "skip_count", -1) end
    delete_backup_track(guild_id, track_uid, video_url, title)
  end
end

bot.lavalink:on("event", function(msg)
  local guild_id = msg.guildId
  if not guild_id then return end

  if msg.type == "TrackEndEvent" then
    local data = playback[guild_id]
    local channel_id = (data and data.channel_id) or voice_channel[guild_id]
    if data then
      handle_track_end(guild_id, msg.reason, data.track_uid, data.url, data.title, data.requester_id, data.duration)
    end
    if channel_id and msg.reason ~= "REPLACED" then
      process_queue(guild_id, channel_id)
    end
  elseif msg.type == "TrackStuckEvent" then
    print(("[strife] track stuck in guild %s, forcing skip"):format(tostring(guild_id)))
    bot.lavalink:stop(guild_id)
  elseif msg.type == "TrackExceptionEvent" then
    print(("[strife] track exception in guild %s: %s"):format(tostring(guild_id), tostring(msg.exception and msg.exception.message)))
  elseif msg.type == "WebSocketClosedEvent" then
    print(("[strife] Discord voice websocket closed for guild %s (code %s)"):format(tostring(guild_id), tostring(msg.code)))
    -- Lavalink's own link to Discord's voice server died -- distinct from
    -- our own gateway seeing a disconnect, and previously handled with a
    -- log line and nothing else, silently ending playback. Only recover if
    -- we still think we're actively playing (a deliberate stop/leave
    -- already cleared `data` here).
    local data = playback[guild_id]
    local channel_id = (data and data.channel_id) or voice_channel[guild_id]
    if data and channel_id then
      local url, title, requester_id, track_uid = data.url, data.title, data.requester_id, data.track_uid
      copas.addthread(function()
        copas.sleep(2)
        connect_voice(guild_id, channel_id)
        -- The track that was playing was already dequeued before it
        -- started, so it isn't sitting in strife_queue to be picked back up
        -- -- put it back at the front (restarts from the beginning, not the
        -- exact position, but that's a much smaller loss than the track
        -- vanishing outright).
        if url then insert_queue_front(guild_id, url, title, requester_id, track_uid) end
        playback[guild_id] = nil
        process_queue(guild_id, channel_id)
      end)
    end
  end
end)

bot.lavalink:on("ready", function(msg)
  print("[strife] Lavalink session ready: " .. tostring(msg.sessionId))
end)

-- =====================================================================
-- BACKGROUND LOOPS
-- =====================================================================

-- Factored into a named function (rather than left as an anonymous copas
-- thread body) so db_smoke_test.lua can call heartbeat_tick() directly and
-- exercise the strife_metrics %s-placeholder-count/arg-count shape and the
-- swarm_health upsert without waiting on a real 30s timer -- this is exactly
-- the kind of INSERT that has had argument-order/count bugs in sibling bots.
local function heartbeat_tick()
  db([[INSERT INTO swarm_health (bot_name, last_pulse, status) VALUES ('strife', now(), 'online')
       ON CONFLICT (bot_name) DO UPDATE SET last_pulse = now(), status = EXCLUDED.status]])
  for guild_id, data in pairs(playback) do
    local backup_row = db1("SELECT COUNT(*) AS n FROM strife_queue_backup WHERE guild_id = %s AND bot_name = 'strife'", guild_id)
    local backup_count = backup_row and tonumber(backup_row.n) or 0
    db([[INSERT INTO strife_metrics (guild_id, bot_name, voice_connected, connected_channel_id, player_connected,
           player_playing, player_paused, queue_count, backup_queue_count, is_playing_db, is_paused_db,
           position_seconds, duration_seconds, lavalink_ready, updated_at)
         VALUES (%s, 'strife', true, %s, true, %s, %s, %s, %s, %s, %s, %s, %s, true, now())
         ON CONFLICT (guild_id, bot_name) DO UPDATE SET voice_connected = EXCLUDED.voice_connected,
           connected_channel_id = EXCLUDED.connected_channel_id, player_connected = EXCLUDED.player_connected,
           player_playing = EXCLUDED.player_playing, player_paused = EXCLUDED.player_paused,
           queue_count = EXCLUDED.queue_count, backup_queue_count = EXCLUDED.backup_queue_count,
           is_playing_db = EXCLUDED.is_playing_db, is_paused_db = EXCLUDED.is_paused_db,
           position_seconds = EXCLUDED.position_seconds, duration_seconds = EXCLUDED.duration_seconds,
           lavalink_ready = EXCLUDED.lavalink_ready, updated_at = now()]],
      guild_id, data.channel_id or PG_NULL, not data.paused, data.paused, queue_count(guild_id), backup_count,
      not data.paused, data.paused, current_position(guild_id), math.floor(data.duration or 0))
  end
end

local function start_background_loops()
  copas.addthread(function()
    while true do
      copas.sleep(30)
      local ok, err = pcall(heartbeat_tick)
      if not ok then
        print("[strife] heartbeat tick failed: " .. tostring(err))
        report_error(nil, "runtime", "heartbeat tick error", tostring(err))
      end
    end
  end)

  copas.addthread(function()
    while true do
      copas.sleep(PLAYLIST_SYNC_INTERVAL)
      local ok, err = pcall(playlist_sync_tick)
      if not ok then
        print("[strife] playlist sync tick error: " .. tostring(err))
        report_error(nil, "runtime", "playlist sync tick error", tostring(err))
      end
    end
  end)

  -- SwarmPanel/Aria remote-control bridge -- was entirely missing (the
  -- swarm_overrides table only existed via a CREATE TABLE/comment, nothing
  -- ever polled it), so the panel's PAUSE/RESUME/SKIP/STOP/RESTART buttons
  -- silently did nothing for this bot. Same poll-and-execute-then-delete
  -- pattern as alucard.lua/gws.lua/sapphire.lua.
  copas.addthread(function()
    while true do
      copas.sleep(10)
      local ok, err = pcall(function()
        local rows = db("SELECT guild_id, command FROM strife_swarm_overrides WHERE bot_name = 'strife'") or {}
        for _, row in ipairs(rows) do
          local guild_id = tostring(row.guild_id)
          local cmd_name = (row.command or ""):upper()
          local data = playback[guild_id]
          local executed = false
          if cmd_name == "RESTART" then
            db("DELETE FROM strife_swarm_overrides WHERE guild_id = %s AND bot_name = 'strife'", guild_id)
            send_webhook_log("\240\159\164\150 Aria Override", "Aria requested a restart.", COLOR.purple)
            os.exit(0)
          elseif cmd_name == "PAUSE" and data and not data.paused then
            bot.lavalink:set_paused(guild_id, true)
            data.paused = true
            data.position = current_position(guild_id)
            persist_playback_state(guild_id, { is_paused = true, position_seconds = data.position })
            executed = true
          elseif cmd_name == "RESUME" and data and data.paused then
            bot.lavalink:set_paused(guild_id, false)
            data.paused = false
            data.updated_at = os.time()
            persist_playback_state(guild_id, { is_paused = false })
            executed = true
          elseif cmd_name == "SKIP" and data then
            bot.lavalink:stop(guild_id)
            executed = true
          elseif cmd_name == "STOP" then
            stop_playback(guild_id)
            executed = true
          end
          if executed then
            send_webhook_log("\240\159\164\150 Aria Override", ("Aria executed **%s** in guild %s."):format(cmd_name, guild_id), COLOR.purple)
          end
          db("DELETE FROM strife_swarm_overrides WHERE guild_id = %s AND bot_name = 'strife' AND command = %s", guild_id, row.command)
        end
      end)
      if not ok then
        print("[strife] swarm override poll error: " .. tostring(err))
        report_error(nil, "runtime", "swarm override poll error", tostring(err))
      end
    end
  end)

  -- Mirrors strife.py's "Database Janitor" retention pass for error_events
  -- (30-day retention); everything else that task did (history pruning,
  -- cache cleanup) is covered by the documented simplifications in the
  -- header comment.
  copas.addthread(function()
    while true do
      copas.sleep(6 * 3600)
      local ok, err = pcall(db, "DELETE FROM strife_error_events WHERE created_at < now() - INTERVAL '30 days'")
      if not ok then print("[strife] error_events cleanup failed: " .. tostring(err)) end
    end
  end)
end

-- /sleep timer: each call bumps the guild's generation counter so a
-- re-run (or a 0-minute call) invalidates any in-flight timer thread.
local function start_sleep_timer(guild_id, minutes, channel_id)
  sleep_timers[guild_id] = (sleep_timers[guild_id] or 0) + 1
  local my_generation = sleep_timers[guild_id]
  copas.addthread(function()
    copas.sleep(minutes * 60)
    if sleep_timers[guild_id] ~= my_generation then return end -- cancelled/superseded
    stop_playback(guild_id)
    db("DELETE FROM strife_queue WHERE guild_id = %s AND bot_name = 'strife'", guild_id)
    disconnect_voice(guild_id)
    print(("[strife] sleep timer elapsed for guild %s"):format(tostring(guild_id)))
  end)
end

local function cancel_sleep_timer(guild_id)
  sleep_timers[guild_id] = (sleep_timers[guild_id] or 0) + 1
end

-- =====================================================================
-- COMMAND SURFACE  (/strife_main_*)
-- =====================================================================

local OPT = { STRING = 3, INTEGER = 4, BOOLEAN = 5, USER = 6, CHANNEL = 7, ROLE = 8, NUMBER = 10 }
local CH_TEXT, CH_VOICE, CH_STAGE, CH_THREAD = 0, 2, 13, 11

local function user_voice_channel(guild_id, user_id)
  local st = voice_states[guild_id] and voice_states[guild_id][user_id]
  return st and st.channel_id or nil
end

local function home_channel(guild_id)
  local row = db1("SELECT home_vc_id FROM strife_bot_home_channels WHERE guild_id = %s AND bot_name = 'strife'", guild_id)
  return row and row.home_vc_id or nil
end

-- SwarmPanel's control.lua writes uppercase PLAY/RECOVER/LEAVE/SEEK rows into
-- strife_swarm_direct_orders for source_url plays, fleet-supervisor recovery,
-- forced disconnects, and seek-to-position -- none of the simpler overrides
-- poller (above, in start_background_loops) covers these. Defined here
-- rather than alongside that poller since it needs home_channel(), which
-- isn't declared until after that function's body was already parsed.
local function handle_direct_order(order)
  local guild_id = tostring(order.guild_id)
  local cmd = order.command
  local ok, err = pcall(function()
    if cmd == "PLAY" and order.data and order.vc_id and order.vc_id ~= "0" then
      local entries, terr = resolve_playables(order.data)
      if not entries or #entries == 0 then error("could not resolve source: " .. tostring(terr), 0) end
      for _, t in ipairs(entries) do
        enqueue_track(guild_id, t.info.uri, t.info.title, nil)
      end
      if #entries > 1 then shuffle_queue_rows(guild_id, true) end
      if not playback[guild_id] then process_queue(guild_id, order.vc_id) end
    elseif cmd == "RECOVER" then
      local channel_id = (order.vc_id and order.vc_id ~= "0" and order.vc_id) or home_channel(guild_id)
      if channel_id and not playback[guild_id] then process_queue(guild_id, channel_id) end
    elseif cmd == "LEAVE" then
      stop_playback(guild_id)
      disconnect_voice(guild_id)
    elseif cmd == "SEEK" and order.data then
      local target_seconds = math.max(0, tonumber(order.data) or 0)
      local data = playback[guild_id]
      if data then
        bot.lavalink:seek(guild_id, target_seconds * 1000)
        data.position = target_seconds
        data.updated_at = os.time()
        persist_playback_state(guild_id, { position_seconds = target_seconds })
      end
    end
  end)
  if not ok then
    db("UPDATE strife_swarm_direct_orders SET attempts = attempts + 1, last_error = %s WHERE id = %s", tostring(err):sub(1, 500), order.id)
  else
    db("DELETE FROM strife_swarm_direct_orders WHERE id = %s", order.id)
    send_webhook_log("\240\159\164\150 Aria Direct Order", ("Aria executed **%s** in guild %s."):format(cmd, guild_id), COLOR.purple)
  end
end

local function poll_direct_orders()
  local rows = db("SELECT id, guild_id, vc_id, text_channel_id, command, data FROM strife_swarm_direct_orders WHERE bot_name = 'strife' AND claimed_at IS NULL ORDER BY id ASC LIMIT 5") or {}
  for _, order in ipairs(rows) do
    db("UPDATE strife_swarm_direct_orders SET claimed_at = NOW() WHERE id = %s", order.id)
    handle_direct_order(order)
  end
end

local function resolve_join_channel(interaction)
  return home_channel(interaction.guild_id) or user_voice_channel(interaction.guild_id, interaction.member.user.id)
end

-- ---------------------------------------------------------------------
-- Playback & Transport
-- ---------------------------------------------------------------------

bot:command("strife_main_play", {
  description = "Queue a track, URL, livestream, search result, or playlist and start playback if idle.",
  options = {
    { type = OPT.STRING, name = "search", description = "Track name, URL, or search text", required = true, autocomplete = true },
    { type = OPT.STRING, name = "source", description = "Where to search (defaults to YouTube for plain text)",
      choices = { { name = "YouTube", value = "ytmsearch" }, { name = "SoundCloud", value = "scsearch" } } },
  },
}, function(self, interaction)
  defer(interaction, false)
  local search = opt(interaction, "search", "")
  local source = opt(interaction, "source", "ytmsearch")
  if (source == "ytmsearch" or source == "scsearch") and not is_explicit_query(search) then
    search = source .. ":" .. search
  end
  local channel_id = resolve_join_channel(interaction)
  if not channel_id then
    followup_embed(interaction, embed({ title = "\226\157\140 Error", description = "Join a channel first or set a home channel.", color = COLOR.red }))
    return
  end

  local entries, playlist = resolve_playables(search)
  if not entries or #entries == 0 then
    followup_embed(interaction, embed({ title = "\226\157\140 Source Error", description = "Could not load that source: " .. tostring(playlist), color = COLOR.red }))
    return
  end

  local guild_id = interaction.guild_id
  local requester_id = interaction.member.user.id
  for _, t in ipairs(entries) do
    local uri = t.info and t.info.uri
    local title = t.info and t.info.title or "Unknown"
    enqueue_track(guild_id, uri, title, requester_id, new_track_uid())
  end
  if #entries > 1 then shuffle_queue_rows(guild_id, true) end
  if playlist and is_playlist_source(search) then
    local norm = {}
    for _, t in ipairs(entries) do
      if t.info and t.info.uri then norm[#norm + 1] = { uri = t.info.uri, title = t.info.title } end
    end
    set_active_playlist(guild_id, search, norm, requester_id, channel_id)
  end
  local total = queue_count(guild_id)

  local was_idle = playback[guild_id] == nil
  if was_idle then
    followup_embed(interaction, embed({ title = "\240\159\142\182 Queued & Starting", description = ("Added **%d** track(s). Materia charged -- Lavalink engine spinning up!"):format(#entries), color = COLOR.green }))
    process_queue(guild_id, channel_id)
  else
    followup_embed(interaction, embed({ title = "\240\159\147\165 Added to Queue", description = ("Added **%d** track(s). (Queue size: %d)"):format(#entries, total), color = COLOR.blue }))
  end
end)

bot:command("strife_main_playnext", {
  description = "Queue one track to play next, ahead of the existing queue, without clearing current playback.",
  options = { { type = OPT.STRING, name = "search", description = "Track name or URL", required = true } },
}, function(self, interaction)
  if not is_dj(interaction) then return end
  defer(interaction, true)
  local entries, err = resolve_playables(opt(interaction, "search", ""))
  if not entries or #entries == 0 then
    followup_embed(interaction, embed({ description = "Could not resolve that source: " .. tostring(err), color = COLOR.red }), true)
    return
  end
  local t = entries[1]
  local guild_id = interaction.guild_id
  insert_queue_front(guild_id, t.info.uri, t.info.title, interaction.member.user.id, new_track_uid())
  snapshot_queue_backup(guild_id)
  followup_embed(interaction, embed({ description = ("\226\143\173\239\184\143 Queued next: **%s**"):format(t.info.title), color = COLOR.green }), true)
end)

bot:command("strife_main_skip", { description = "Skip the current track and move playback to the next queued item or Auto-DJ recommendation." },
function(self, interaction)
  if not is_dj(interaction) then return end
  local guild_id = interaction.guild_id
  if playback[guild_id] then
    bot.lavalink:stop(guild_id)
    reply_embed(interaction, embed({ description = "\226\143\173\239\184\143 Skipped", color = COLOR.blurple }), true)
  else
    reply_embed(interaction, embed({ description = "\226\157\140 Nothing is playing.", color = COLOR.red }), true)
  end
end)

bot:command("strife_main_stop", { description = "Stop playback, clear the queue, remove recovery state, and reset the bot for this server." },
function(self, interaction)
  if not is_dj(interaction) then return end
  local guild_id = interaction.guild_id
  cancel_sleep_timer(guild_id)
  stop_playback(guild_id)
  db("DELETE FROM strife_queue WHERE guild_id = %s AND bot_name = 'strife'", guild_id)
  clear_active_playlist(guild_id)
  reply_embed(interaction, embed({ title = "\226\143\185 Stopped", description = "Sheathing the Buster Sword. Music stopped and queue cleared.", color = COLOR.red }), true)
end)

bot:command("strife_main_sleep", {
  description = "Stop playback and disconnect after N minutes. Use 0 to cancel an active timer.",
  options = { { type = OPT.INTEGER, name = "minutes", description = "Minutes until playback stops (0 cancels)", required = true, min_value = 0, max_value = 240 } },
}, function(self, interaction)
  if not is_dj(interaction) then return end
  local minutes = opt(interaction, "minutes", 0)
  local guild_id = interaction.guild_id
  cancel_sleep_timer(guild_id)
  if minutes <= 0 then
    reply_embed(interaction, embed({ description = "\226\143\176 Sleep timer cancelled.", color = COLOR.orange }), true)
    return
  end
  start_sleep_timer(guild_id, minutes, interaction.channel_id)
  reply_embed(interaction, embed({ description = ("\240\159\152\180 Sleep timer set for **%d** minute(s). Even SOLDIERs need rest."):format(minutes), color = COLOR.blue }), true)
end)

bot:command("strife_main_pause", { description = "Pause the current track without clearing the queue or playback position." },
function(self, interaction)
  if not is_dj(interaction) then return end
  local guild_id = interaction.guild_id
  local data = playback[guild_id]
  if data and not data.paused then
    bot.lavalink:set_paused(guild_id, true)
    data.paused = true
    data.position = current_position(guild_id)
    persist_playback_state(guild_id, { is_paused = true, position_seconds = data.position })
    reply_embed(interaction, embed({ description = "\226\143\184\239\184\143 Paused", color = COLOR.blue }), true)
  else
    reply_embed(interaction, embed({ description = "\226\157\140 Nothing is currently playing.", color = COLOR.red }), true)
  end
end)

bot:command("strife_main_resume", { description = "Resume the paused track from its current playback position." },
function(self, interaction)
  if not is_dj(interaction) then return end
  local guild_id = interaction.guild_id
  local data = playback[guild_id]
  if data and data.paused then
    bot.lavalink:set_paused(guild_id, false)
    data.paused = false
    data.updated_at = os.time()
    persist_playback_state(guild_id, { is_paused = false })
    reply_embed(interaction, embed({ description = "\226\150\182\239\184\143 Resumed", color = COLOR.green }), true)
  else
    reply_embed(interaction, embed({ description = "\226\157\140 Nothing is currently paused.", color = COLOR.red }), true)
  end
end)

bot:command("strife_main_replay", { description = "Restart the current track from the beginning without changing the queue." },
function(self, interaction)
  if not is_dj(interaction) then return end
  local guild_id = interaction.guild_id
  local data = playback[guild_id]
  if not data then
    reply_embed(interaction, embed({ description = "Nothing playing.", color = COLOR.red }), true)
    return
  end
  bot.lavalink:seek(guild_id, 0)
  data.position = 0; data.updated_at = os.time()
  reply_embed(interaction, embed({ description = "Replaying song.", color = COLOR.green }), true)
end)

local function do_seek(interaction, compute_target, response_text)
  if not is_dj(interaction) then return end
  local guild_id = interaction.guild_id
  local data = playback[guild_id]
  if not data then
    reply_embed(interaction, embed({ description = "Nothing playing.", color = COLOR.red }), true)
    return
  end
  local target = math.max(0, compute_target())
  bot.lavalink:seek(guild_id, target * 1000)
  data.position = target; data.updated_at = os.time()
  reply_embed(interaction, embed({ description = response_text(target), color = COLOR.green }), true)
end

bot:command("strife_main_seek", {
  description = "Jump to an exact time in the current track using seconds from the start.",
  options = { { type = OPT.INTEGER, name = "seconds", description = "Position in seconds", required = true } },
}, function(self, interaction)
  local seconds = opt(interaction, "seconds", 0)
  do_seek(interaction, function() return seconds end, function(t) return ("Seeked to %ds"):format(t) end)
end)

bot:command("strife_main_forward", {
  description = "Jump forward within the current track by the number of seconds you provide.",
  options = { { type = OPT.INTEGER, name = "seconds", description = "Seconds to skip forward", required = true } },
}, function(self, interaction)
  local seconds = opt(interaction, "seconds", 0)
  do_seek(interaction, function() return current_position(interaction.guild_id) + seconds end, function(t) return ("Skipped forward %ds"):format(seconds) end)
end)

bot:command("strife_main_rewind", {
  description = "Jump backward within the current track by the number of seconds you provide.",
  options = { { type = OPT.INTEGER, name = "seconds", description = "Seconds to rewind", required = true } },
}, function(self, interaction)
  local seconds = opt(interaction, "seconds", 0)
  do_seek(interaction, function() return current_position(interaction.guild_id) - seconds end, function(t) return ("Rewound %ds"):format(seconds) end)
end)

bot:command("strife_main_join", { description = "Force the bot to join your current voice channel, or its configured home channel if one is saved." },
function(self, interaction)
  defer(interaction, true)
  local channel_id = resolve_join_channel(interaction)
  if not channel_id then
    followup_text(interaction, "Join a channel first, or set a home channel.", true)
    return
  end
  connect_voice(interaction.guild_id, channel_id)
  followup_embed(interaction, embed({ description = ("Joined <#%s>."):format(channel_id), color = COLOR.green }))
end)

bot:command("strife_main_leave", { description = "Disconnect the bot from voice and clear any pending recovery handoff for this server." },
function(self, interaction)
  if not is_dj(interaction) then return end
  local guild_id = interaction.guild_id
  if voice_channel[guild_id] then
    cancel_sleep_timer(guild_id)
    stop_playback(guild_id)
    db("DELETE FROM strife_queue WHERE guild_id = %s AND bot_name = 'strife'", guild_id)
    disconnect_voice(guild_id)
    reply_embed(interaction, embed({ description = "Left the channel.", color = COLOR.orange }), true)
  else
    reply_embed(interaction, embed({ description = "\226\157\140 I'm not in a voice channel.", color = COLOR.red }), true)
  end
end)

bot:command("strife_main_nowplaying", { description = "Show the current track, progress bar, requester, and live playback status." },
function(self, interaction)
  local guild_id = interaction.guild_id
  local data = playback[guild_id]
  if not data then
    reply_embed(interaction, embed({ description = "Nothing playing.", color = COLOR.red }), true)
    return
  end
  local bar = make_progress_bar(current_position(guild_id), data.duration)
  local e = embed({
    title = "\240\159\142\181 Now Playing",
    description = ("**[%s](%s)**\n\n`%s`"):format(data.title, data.url, bar),
    color = COLOR.blue,
    fields = {
      field("Requested by", resolve_requester_name(guild_id, data.requester_id), true),
      field("Filter", data.current_filter or "none", true),
    },
  })
  reply_embed(interaction, e, true)
end)

-- ---------------------------------------------------------------------
-- Queue Management
-- ---------------------------------------------------------------------

bot:command("strife_main_queue", {
  description = "Show the current queue with paging, requester names, and track positions",
  options = { { type = OPT.INTEGER, name = "page", description = "Page number", min_value = 1 } },
}, function(self, interaction)
  defer(interaction, true)
  local guild_id = interaction.guild_id
  local page = math.max(1, opt(interaction, "page", 1))
  local per_page = 10
  local total = queue_count(guild_id)
  if total == 0 then
    followup_embed(interaction, embed({ description = "Queue empty.", color = COLOR.red }), true)
    return
  end
  local rows = db("SELECT title, requester_id FROM strife_queue WHERE guild_id = %s AND bot_name = 'strife' ORDER BY id ASC LIMIT %s OFFSET %s",
    guild_id, per_page, (page - 1) * per_page) or {}
  local lines = {}
  for i, r in ipairs(rows) do
    lines[#lines + 1] = ("**%d.** %s -- *%s*"):format((page - 1) * per_page + i, r.title, resolve_requester_name(guild_id, r.requester_id))
  end
  local pages = math.max(1, math.ceil(total / per_page))
  followup_embed(interaction, embed({
    title = "\240\159\147\156 Queue", description = table.concat(lines, "\n"), color = COLOR.blurple,
    footer = ("Page %d/%d \226\128\162 %d queued track(s)"):format(page, pages, total),
  }), true)
end)

bot:command("strife_main_shuffle", { description = "Smart-shuffle the upcoming queue while keeping duplicate tracks separated when possible." },
function(self, interaction)
  if not is_dj(interaction) then return end
  local guild_id = interaction.guild_id
  local n = shuffle_queue_rows(guild_id, false)
  if n <= 0 then
    reply_text(interaction, "Queue empty.", true)
    return
  end
  snapshot_queue_backup(guild_id)
  reply_embed(interaction, embed({ description = ("\240\159\148\128 Shuffled %d queued track(s)."):format(n), color = COLOR.green }), true)
end)

bot:command("strife_main_remove", {
  description = "Remove a queued track by its queue number so it will not play later.",
  options = { { type = OPT.INTEGER, name = "index", description = "Queue position (1-based)", required = true, min_value = 1, autocomplete = true } },
}, function(self, interaction)
  if not is_dj(interaction) then return end
  local guild_id = interaction.guild_id
  local index = opt(interaction, "index", 0)
  local row = db1("SELECT id, video_url, title, track_uid FROM strife_queue WHERE guild_id = %s AND bot_name = 'strife' ORDER BY id ASC LIMIT 1 OFFSET %s", guild_id, index - 1)
  if not row then
    reply_text(interaction, "Invalid index.", true)
    return
  end
  db("DELETE FROM strife_queue WHERE id = %s", row.id)
  delete_backup_track(guild_id, row.track_uid, row.video_url, row.title)
  reply_embed(interaction, embed({ description = ("Removed item #%d"):format(index), color = COLOR.green }), true)
end)

bot:command("strife_main_skipto", {
  description = "Drop everything before a chosen queue position and jump playback forward to that track.",
  options = { { type = OPT.INTEGER, name = "index", description = "Queue position to jump to", required = true, min_value = 1, autocomplete = true } },
}, function(self, interaction)
  if not is_dj(interaction) then return end
  defer(interaction, true)
  local guild_id = interaction.guild_id
  local index = opt(interaction, "index", 1)
  local skip_n = math.max(0, index - 1)
  local rows = db("SELECT id, video_url, title, track_uid FROM strife_queue WHERE guild_id = %s AND bot_name = 'strife' ORDER BY id ASC LIMIT %s", guild_id, skip_n) or {}
  for _, r in ipairs(rows) do
    db("DELETE FROM strife_queue WHERE id = %s", r.id)
    delete_backup_track(guild_id, r.track_uid, r.video_url, r.title)
  end
  if playback[guild_id] then bot.lavalink:stop(guild_id) end
  followup_embed(interaction, embed({ description = ("Skipped to #%d"):format(index), color = COLOR.green }), true)
end)

bot:command("strife_main_move", {
  description = "Move a queued track from one queue slot to another without rebuilding the entire session manually.",
  options = {
    { type = OPT.INTEGER, name = "frm", description = "From position", required = true, min_value = 1, autocomplete = true },
    { type = OPT.INTEGER, name = "to", description = "To position", required = true, min_value = 1, autocomplete = true },
  },
}, function(self, interaction)
  if not is_dj(interaction) then return end
  defer(interaction, true)
  local guild_id = interaction.guild_id
  local frm, to = opt(interaction, "frm", 1), opt(interaction, "to", 1)
  local rows = db("SELECT video_url, title, requester_id, track_uid FROM strife_queue WHERE guild_id = %s AND bot_name = 'strife' ORDER BY id ASC", guild_id) or {}
  if frm > #rows or to > #rows or frm < 1 or to < 1 then
    followup_text(interaction, "Invalid index", true)
    return
  end
  local item = table.remove(rows, frm)
  table.insert(rows, math.max(1, math.min(to, #rows + 1)), item)
  db("DELETE FROM strife_queue WHERE guild_id = %s AND bot_name = 'strife'", guild_id)
  for _, r in ipairs(rows) do
    db("INSERT INTO strife_queue (guild_id, bot_name, video_url, title, requester_id, track_uid) VALUES (%s, 'strife', %s, %s, %s, %s)",
      guild_id, r.video_url, r.title, r.requester_id or PG_NULL, r.track_uid or new_track_uid())
  end
  snapshot_queue_backup(guild_id)
  followup_embed(interaction, embed({ description = ("Moved item from %d to %d"):format(frm, to), color = COLOR.green }), true)
end)

bot:command("strife_main_bump", {
  description = "Move a queued track to the front so it plays next",
  options = { { type = OPT.INTEGER, name = "index", description = "Queue position to bump", required = true, min_value = 1, autocomplete = true } },
}, function(self, interaction)
  if not is_dj(interaction) then return end
  local guild_id = interaction.guild_id
  local index = opt(interaction, "index", 1)
  local row = db1("SELECT id, video_url, title, requester_id, track_uid FROM strife_queue WHERE guild_id = %s AND bot_name = 'strife' ORDER BY id ASC LIMIT 1 OFFSET %s", guild_id, index - 1)
  if not row then
    reply_text(interaction, "Invalid index.", true)
    return
  end
  db("DELETE FROM strife_queue WHERE id = %s", row.id)
  insert_queue_front(guild_id, row.video_url, row.title, row.requester_id, row.track_uid)
  snapshot_queue_backup(guild_id)
  reply_embed(interaction, embed({ description = ("\226\172\134\239\184\143 Moved **%s** to play next."):format(row.title), color = COLOR.green }), true)
end)

bot:command("strife_main_clearmine", { description = "Remove your own queued songs without touching other listeners' tracks" },
function(self, interaction)
  defer(interaction, true)
  local guild_id, user_id = interaction.guild_id, interaction.member.user.id
  local row = db1("SELECT COUNT(*) AS n FROM strife_queue WHERE guild_id = %s AND bot_name = 'strife' AND requester_id = %s", guild_id, user_id)
  local removed = row and tonumber(row.n) or 0
  if removed > 0 then
    db("DELETE FROM strife_queue WHERE guild_id = %s AND bot_name = 'strife' AND requester_id = %s", guild_id, user_id)
    db("DELETE FROM strife_queue_backup WHERE guild_id = %s AND bot_name = 'strife' AND requester_id = %s", guild_id, user_id)
  end
  followup_embed(interaction, embed({ description = ("\240\159\167\185 Removed **%d** of your queued track(s)."):format(removed), color = COLOR.green }), true)
end)

bot:command("strife_main_clear", { description = "Clear the upcoming queue, stop playback, and reset stored playback state for this server." },
function(self, interaction)
  if not is_dj(interaction) then return end
  local guild_id = interaction.guild_id
  cancel_sleep_timer(guild_id)
  if playback[guild_id] then bot.lavalink:stop(guild_id) end
  playback[guild_id] = nil
  db("DELETE FROM strife_queue WHERE guild_id = %s AND bot_name = 'strife'", guild_id)
  db("DELETE FROM strife_queue_backup WHERE guild_id = %s AND bot_name = 'strife'", guild_id)
  clear_active_playlist(guild_id)
  persist_playback_state(guild_id, { is_playing = false, is_paused = false, video_url = PG_NULL, title = PG_NULL, position_seconds = 0, track_uid = PG_NULL })
  reply_embed(interaction, embed({ description = "\240\159\151\145\239\184\143 Playback stopped and queue cleared.", color = COLOR.red }), true)
end)

bot:command("strife_main_voteskip", { description = "Start or join a vote skip when no DJ is around to skip directly" },
function(self, interaction)
  local guild_id = interaction.guild_id
  local channel_id = voice_channel[guild_id]
  if not channel_id or not playback[guild_id] then
    reply_text(interaction, "Nothing is playing right now.", true)
    return
  end
  if is_dj(interaction, true) then
    bot.lavalink:stop(guild_id)
    reply_embed(interaction, embed({ description = "\226\143\173\239\184\143 DJ override used: skipped the current track.", color = COLOR.green }), true)
    return
  end
  local listeners = members_in_channel(guild_id, channel_id)
  local user_id = interaction.member.user.id
  local in_channel = false
  for _, uid in ipairs(listeners) do if uid == user_id then in_channel = true end end
  if not in_channel then
    reply_text(interaction, "Join the same voice channel first.", true)
    return
  end
  local required = math.max(2, math.floor(#listeners / 2) + 1)
  vote_skip[guild_id] = vote_skip[guild_id] or {}
  vote_skip[guild_id][user_id] = true
  local count = 0
  for _ in pairs(vote_skip[guild_id]) do count = count + 1 end
  if count >= required then
    vote_skip[guild_id] = nil
    bot.lavalink:stop(guild_id)
    reply_embed(interaction, embed({ description = ("\226\143\173\239\184\143 Vote skip passed with **%d/%d** votes."):format(count, required), color = COLOR.green }))
  else
    reply_embed(interaction, embed({ description = ("\240\159\151\179\239\184\143 Vote recorded: **%d/%d** votes to skip."):format(count, required), color = COLOR.blurple }), true)
  end
end)

local function get_voice_listeners_or_reply(interaction)
  local guild_id = interaction.guild_id
  local channel_id = voice_channel[guild_id]
  if not channel_id then
    reply_text(interaction, "Nothing is playing right now.", true)
    return nil
  end
  local listeners = members_in_channel(guild_id, channel_id)
  local user_id = interaction.member.user.id
  local in_channel = false
  for _, uid in ipairs(listeners) do if uid == user_id then in_channel = true end end
  if not in_channel then
    reply_text(interaction, "Join the same voice channel first.", true)
    return nil
  end
  return listeners
end

bot:command("strife_main_upvote", {
  description = "Upvote a queued track; enough listener upvotes bump it to play next.",
  options = { { type = OPT.INTEGER, name = "index", description = "Queue position", required = true, min_value = 1, autocomplete = true } },
}, function(self, interaction)
  local listeners = get_voice_listeners_or_reply(interaction)
  if not listeners then return end
  local guild_id = interaction.guild_id
  local index = opt(interaction, "index", 1)
  local row = db1("SELECT id, video_url, title, requester_id, track_uid FROM strife_queue WHERE guild_id = %s AND bot_name = 'strife' ORDER BY id ASC LIMIT 1 OFFSET %s", guild_id, index - 1)
  if not row then reply_text(interaction, "Invalid index.", true); return end
  local key = guild_id .. ":" .. row.id
  queue_votes[key] = queue_votes[key] or { up = {}, down = {} }
  local votes = queue_votes[key]
  local user_id = interaction.member.user.id
  votes.down[user_id] = nil
  votes.up[user_id] = true
  local up_count = 0
  for _ in pairs(votes.up) do up_count = up_count + 1 end
  local required = math.max(2, math.floor(#listeners / 2) + 1)
  if up_count >= required then
    queue_votes[key] = nil
    db("DELETE FROM strife_queue WHERE id = %s", row.id)
    insert_queue_front(guild_id, row.video_url, row.title, row.requester_id, row.track_uid)
    snapshot_queue_backup(guild_id)
    reply_embed(interaction, embed({ description = ("\226\172\134\239\184\143 **%s** got enough upvotes and will play next!"):format(row.title), color = COLOR.green }))
  else
    reply_embed(interaction, embed({ description = ("\240\159\148\186 Upvoted **#%d** (%d/%d) to play next."):format(index, up_count, required), color = COLOR.blurple }), true)
  end
end)

bot:command("strife_main_downvote", {
  description = "Downvote a queued track; enough listener downvotes removes it from the queue.",
  options = { { type = OPT.INTEGER, name = "index", description = "Queue position", required = true, min_value = 1, autocomplete = true } },
}, function(self, interaction)
  local listeners = get_voice_listeners_or_reply(interaction)
  if not listeners then return end
  local guild_id = interaction.guild_id
  local index = opt(interaction, "index", 1)
  local row = db1("SELECT id, video_url, title, track_uid FROM strife_queue WHERE guild_id = %s AND bot_name = 'strife' ORDER BY id ASC LIMIT 1 OFFSET %s", guild_id, index - 1)
  if not row then reply_text(interaction, "Invalid index.", true); return end
  local key = guild_id .. ":" .. row.id
  queue_votes[key] = queue_votes[key] or { up = {}, down = {} }
  local votes = queue_votes[key]
  local user_id = interaction.member.user.id
  votes.up[user_id] = nil
  votes.down[user_id] = true
  local down_count = 0
  for _ in pairs(votes.down) do down_count = down_count + 1 end
  local required = math.max(2, math.floor(#listeners / 2) + 1)
  if down_count >= required then
    queue_votes[key] = nil
    db("DELETE FROM strife_queue WHERE id = %s", row.id)
    delete_backup_track(guild_id, row.track_uid, row.video_url, row.title)
    reply_embed(interaction, embed({ description = ("\226\172\135\239\184\143 **%s** got enough downvotes and was removed."):format(row.title), color = COLOR.orange }))
  else
    reply_embed(interaction, embed({ description = ("\240\159\148\187 Downvoted **#%d** (%d/%d) to remove."):format(index, down_count, required), color = COLOR.blurple }), true)
  end
end)

bot:command("strife_main_loop", {
  description = "Choose whether playback loops nothing, the current song, or the full queue.",
  options = { { type = OPT.STRING, name = "mode", description = "Loop mode", required = true,
    choices = { { name = "Off", value = "off" }, { name = "Song", value = "song" }, { name = "Queue", value = "queue" } } } },
}, function(self, interaction)
  if not is_dj(interaction) then return end
  local mode = opt(interaction, "mode", "queue")
  if mode ~= "off" and mode ~= "song" and mode ~= "queue" then
    reply_text(interaction, "Invalid mode.", true); return
  end
  local guild_id = interaction.guild_id
  ensure_guild_settings(guild_id)
  db([[INSERT INTO strife_guild_settings (guild_id, loop_mode) VALUES (%s, %s)
       ON CONFLICT (guild_id) DO UPDATE SET loop_mode = EXCLUDED.loop_mode]], guild_id, mode)
  reply_embed(interaction, embed({ description = ("\240\159\148\129 Looping set to: %s"):format(mode), color = COLOR.green }), true)
end)

-- ---------------------------------------------------------------------
-- Audio & Filters
-- ---------------------------------------------------------------------

bot:command("strife_main_volume", {
  description = "Set the playback volume for this server from 1 to 200 percent.",
  options = { { type = OPT.INTEGER, name = "vol", description = "Volume percent (1-200)", required = true, min_value = 1, max_value = 200 } },
}, function(self, interaction)
  if not is_dj(interaction) then return end
  local guild_id = interaction.guild_id
  local vol = math.max(1, math.min(200, opt(interaction, "vol", 100)))
  ensure_guild_settings(guild_id)
  db([[INSERT INTO strife_guild_settings (guild_id, volume) VALUES (%s, %s)
       ON CONFLICT (guild_id) DO UPDATE SET volume = EXCLUDED.volume]], guild_id, vol)
  if playback[guild_id] then bot.lavalink:set_volume(guild_id, vol) end
  reply_embed(interaction, embed({ description = ("\240\159\148\138 Volume set to %d%%"):format(vol), color = COLOR.green }), true)
end)

bot:command("strife_main_filter", {
  description = "Apply an audio filter such as nightcore, vaporwave, or bass boost to upcoming playback.",
  options = {
    { type = OPT.STRING, name = "mode", description = "Choose an audio filter to apply", required = true, choices = FILTER_CHOICES },
    { type = OPT.STRING, name = "stack", description = "Optional second filter to layer on top (e.g. bassboost + nightcore)", choices = FILTER_CHOICES },
  },
}, function(self, interaction)
  if not is_dj(interaction) then return end
  local mode = opt(interaction, "mode", "none")
  local stack = opt(interaction, "stack", nil)
  if not FILTER_VALUES[mode] then reply_text(interaction, "Invalid filter.", true); return end
  if stack and not FILTER_VALUES[stack] then reply_text(interaction, "Invalid stacked filter.", true); return end
  defer(interaction, true)
  local guild_id = interaction.guild_id
  ensure_guild_settings(guild_id)
  local stack_value = (stack == nil or stack == "none") and nil or stack
  if stack == nil then
    db("UPDATE strife_guild_settings SET filter_mode = %s, custom_speed = 1.0, custom_pitch = 1.0, custom_modifiers_left = 0 WHERE guild_id = %s", mode, guild_id)
  else
    db("UPDATE strife_guild_settings SET filter_mode = %s, filter_stack = %s, custom_speed = 1.0, custom_pitch = 1.0, custom_modifiers_left = 0 WHERE guild_id = %s",
      mode, stack_value or PG_NULL, guild_id)
  end
  local speed = 1.0
  if playback[guild_id] then
    local filters
    filters, speed = build_filter_payload(mode, 1.0)
    if stack_value and stack_value ~= mode then
      local f2, speed2 = build_filter_payload(stack_value, speed)
      for k, v in pairs(f2) do filters[k] = v end
      speed = speed2
    end
    bot.lavalink:update_player(guild_id, { filters = filters })
    playback[guild_id].speed = speed
    playback[guild_id].current_filter = mode
  end
  local label = FILTER_VALUES[mode]
  if stack_value and stack_value ~= mode then
    followup_embed(interaction, embed({ description = ("\240\159\142\155\239\184\143 Filter set to: **%s** + **%s**."):format(label, FILTER_VALUES[stack_value]), color = COLOR.blurple }), true)
  else
    followup_embed(interaction, embed({ description = ("\240\159\142\155\239\184\143 Filter set to: **%s**."):format(label), color = COLOR.blurple }), true)
  end
end)

bot:command("strife_main_fade", {
  description = "Customize track fade transitions, let the bot pick smart fade timing, or enable BPM-matched mixing.",
  options = {
    { type = OPT.STRING, name = "mode", description = "Fade mode", required = true, choices = {
      { name = "Smart Adaptive Fades", value = "smart" }, { name = "Custom Fades", value = "fade" },
      { name = "Beat-Matched Mixing", value = "mix" }, { name = "Disable Fades", value = "off" } } },
    { type = OPT.NUMBER, name = "seconds", description = "Fade length in seconds, from 0.5 to 20" },
    { type = OPT.STRING, name = "curve", description = "Volume curve", choices = {
      { name = "Linear", value = "linear" }, { name = "Smooth", value = "smooth" },
      { name = "Slow Start", value = "ease_in" }, { name = "Soft Land", value = "ease_out" } } },
  },
}, function(self, interaction)
  if not is_dj(interaction) then return end
  local mode = opt(interaction, "mode", "off")
  if mode ~= "off" and mode ~= "fade" and mode ~= "smart" and mode ~= "mix" then
    reply_text(interaction, "Invalid fade mode.", true); return
  end
  defer(interaction, true)
  local curve = opt(interaction, "curve", "smooth")
  if curve ~= "linear" and curve ~= "smooth" and curve ~= "ease_in" and curve ~= "ease_out" then curve = "smooth" end
  local seconds = math.max(0.25, math.min(12.0, opt(interaction, "seconds", 3.0)))
  local guild_id = interaction.guild_id
  ensure_guild_settings(guild_id)
  db("UPDATE strife_guild_settings SET transition_mode = %s, fade_seconds = %s, fade_curve = %s WHERE guild_id = %s", mode, seconds, curve, guild_id)
  local message, color
  if mode == "smart" then
    message = "\240\159\140\138 Smart fades enabled. Short, smooth ramps that adapt to track length and active filters."
    color = COLOR.green
  elseif mode == "fade" then
    message = ("\240\159\140\138 Custom fades enabled: %gs using %s."):format(seconds, curve:gsub("_", " "))
    color = COLOR.green
  elseif mode == "mix" then
    message = "\240\159\142\154\239\184\143 Beat-matched mixing enabled. (Best-effort -- see /strife_main_help notes on filter fidelity.)"
    color = COLOR.green
  else
    message = "\226\143\185\239\184\143 Smooth fades disabled."
    color = COLOR.red
  end
  followup_embed(interaction, embed({ description = message, color = color }), true)
end)

bot:command("strife_main_modify", {
  description = "Apply temporary custom speed and pitch modifiers to the next few tracks in the queue.",
  options = {
    { type = OPT.NUMBER, name = "speed", description = "Speed multiplier (0.5 to 2.0)" },
    { type = OPT.NUMBER, name = "pitch", description = "Pitch multiplier (0.5 to 2.0)" },
    { type = OPT.INTEGER, name = "duration", description = "How many tracks this lasts (default 1)", min_value = 1 },
  },
}, function(self, interaction)
  if not is_dj(interaction) then return end
  defer(interaction, true)
  local guild_id = interaction.guild_id
  ensure_guild_settings(guild_id)
  local speed = math.max(0.5, math.min(2.0, opt(interaction, "speed", 1.0)))
  local pitch = math.max(0.5, math.min(2.0, opt(interaction, "pitch", 1.0)))
  local duration = math.max(1, opt(interaction, "duration", 1))
  local settings = db1("SELECT filter_mode FROM strife_guild_settings WHERE guild_id = %s", guild_id)
  if settings and settings.filter_mode ~= "none" then
    followup_embed(interaction, embed({ description = "\226\157\140 **Conflict:** Disable standard filters via `/strife_main_filter none` first.", color = COLOR.red }), true)
    return
  end
  db("UPDATE strife_guild_settings SET custom_speed = %s, custom_pitch = %s, custom_modifiers_left = %s WHERE guild_id = %s", speed, pitch, duration, guild_id)
  if playback[guild_id] then
    playback[guild_id].speed = speed
    bot.lavalink:update_player(guild_id, { filters = { timescale = { speed = speed, pitch = pitch } } })
  end
  followup_embed(interaction, embed({
    title = "\240\159\142\155\239\184\143 Audio Modifiers Set",
    description = ("**Speed:** %gx\n**Pitch:** %gx\n*Active for the next %d track(s).*"):format(speed, pitch, duration),
    color = COLOR.gold,
  }), true)
end)

-- ---------------------------------------------------------------------
-- Smart Auto-DJ & Discovery
-- ---------------------------------------------------------------------

bot:command("strife_main_radio", {
  description = "Queue several tracks matching a mood or vibe, e.g. 'chill lofi' or 'workout hype'.",
  options = {
    { type = OPT.STRING, name = "mood", description = "Mood or vibe", required = true },
    { type = OPT.INTEGER, name = "count", description = "How many tracks (1-10)", min_value = 1, max_value = 10 },
  },
}, function(self, interaction)
  defer(interaction, false)
  local guild_id = interaction.guild_id
  local channel_id = user_voice_channel(guild_id, interaction.member.user.id)
  if not channel_id then
    followup_embed(interaction, embed({ description = "Join a voice channel first.", color = COLOR.red }))
    return
  end
  local mood = opt(interaction, "mood", "")
  local count = opt(interaction, "count", 5)
  local entries, err = resolve_playables(mood:sub(1, 100) .. " mix")
  if not entries or #entries == 0 then
    followup_embed(interaction, embed({ description = ("Couldn't find tracks for **%s**: %s"):format(mood, tostring(err)), color = COLOR.red }))
    return
  end
  local requester_id = interaction.member.user.id
  local added = 0
  for i, t in ipairs(entries) do
    if i > count then break end
    enqueue_track(guild_id, t.info.uri, t.info.title, requester_id, new_track_uid())
    added = added + 1
  end
  if playback[guild_id] == nil then process_queue(guild_id, channel_id) end
  followup_embed(interaction, embed({ title = "\240\159\147\187 Mood Radio", description = ("Queued **%d** track(s) for the vibe: *%s*."):format(added, mood), color = COLOR.green }))
end)

bot:command("strife_main_autodj", {
  description = "Enable or disable smarter Auto-DJ recommendations when the queue runs dry",
  options = { { type = OPT.BOOLEAN, name = "enabled", description = "Enable Auto-DJ", required = true } },
}, function(self, interaction)
  if not is_dj(interaction) then return end
  defer(interaction, true)
  local enabled = opt(interaction, "enabled", false)
  set_autodj_enabled(interaction.guild_id, enabled)
  followup_embed(interaction, embed({ description = ("\240\159\147\187 Auto-DJ is now **%s**."):format(enabled and "enabled" or "disabled"), color = COLOR.green }), true)
end)

bot:command("strife_main_like", { description = "Teach Auto-DJ to play more tracks like the current one for you." },
function(self, interaction)
  local guild_id = interaction.guild_id
  local snap = get_current_track_snapshot(guild_id)
  if not snap then reply_text(interaction, "Nothing is playing right now.", true); return end
  record_track_feedback(guild_id, interaction.member.user.id, snap.url, snap.title, true)
  reply_embed(interaction, embed({ description = ("Saved your like for **%s**. Auto-DJ will lean toward similar picks."):format(snap.title or "this track"), color = COLOR.green }), true)
end)

bot:command("strife_main_dislike", { description = "Teach Auto-DJ to avoid the current track for your future recommendations." },
function(self, interaction)
  local guild_id = interaction.guild_id
  local snap = get_current_track_snapshot(guild_id)
  if not snap then reply_text(interaction, "Nothing is playing right now.", true); return end
  record_track_feedback(guild_id, interaction.member.user.id, snap.url, snap.title, false)
  reply_embed(interaction, embed({ description = ("Saved your dislike for **%s**. Auto-DJ will avoid it for you."):format(snap.title or "this track"), color = COLOR.orange }), true)
end)

bot:command("strife_main_recommend", {
  description = "Queue a smart recommendation learned from server taste, playlists, and listener feedback.",
  options = { { type = OPT.USER, name = "member", description = "Base the pick on this member's taste instead of yours" } },
}, function(self, interaction)
  defer(interaction, false)
  local guild_id = interaction.guild_id
  local target_id = opt(interaction, "member", interaction.member.user.id)
  local channel_id = voice_channel[guild_id] or home_channel(guild_id) or user_voice_channel(guild_id, interaction.member.user.id)
  if not channel_id then
    followup_embed(interaction, embed({ title = "Source Error", description = "Join a channel first or set a home channel.", color = COLOR.red }))
    return
  end
  local track, reco = pick_smart_recommendation(guild_id, { target_id }, nil)
  if not track then
    followup_embed(interaction, embed({ description = "I could not find a recommendation yet. Play a few tracks or save a playlist first.", color = COLOR.red }))
    return
  end
  local title, uri = track.info.title, track.info.uri
  enqueue_track(guild_id, uri, title, interaction.member.user.id, new_track_uid())
  db("INSERT INTO strife_smart_recommendations (guild_id, requester_id, chosen_url, chosen_title, reason) VALUES (%s, %s, %s, %s, %s)",
    guild_id, interaction.member.user.id, uri, title, "slash:" .. tostring(target_id))
  local title_text
  if playback[guild_id] == nil then
    process_queue(guild_id, channel_id)
    title_text = "Smart Recommendation Starting"
  else
    title_text = "Smart Recommendation Queued"
  end
  followup_embed(interaction, embed({ title = title_text, description = ("Added **%s** based on saved taste. Queue size: %d"):format(title, queue_count(guild_id)), color = COLOR.green }))
end)

bot:command("strife_main_taste", {
  description = "Show the saved taste profile Auto-DJ has learned for you or another member.",
  options = { { type = OPT.USER, name = "member", description = "Whose taste profile to show" } },
}, function(self, interaction)
  local guild_id = interaction.guild_id
  local target_id = opt(interaction, "member", interaction.member.user.id)
  local rows = db([[SELECT title, score, play_count, finish_count, like_count, dislike_count, skip_count
                     FROM strife_user_track_affinity WHERE guild_id = %s AND user_id = %s
                     ORDER BY score DESC LIMIT 8]], guild_id, target_id) or {}
  if #rows == 0 then
    reply_text(interaction, ("No saved taste profile for <@%s> yet."):format(target_id), true)
    return
  end
  local lines, played, finished, liked, disliked, skipped = {}, 0, 0, 0, 0, 0
  for i, r in ipairs(rows) do
    lines[#lines + 1] = ("**%d.** %s *(score %.1f)*"):format(i, r.title or "Unknown Track", tonumber(r.score) or 0)
    played = played + (tonumber(r.play_count) or 0)
    finished = finished + (tonumber(r.finish_count) or 0)
    liked = liked + (tonumber(r.like_count) or 0)
    disliked = disliked + (tonumber(r.dislike_count) or 0)
    skipped = skipped + (tonumber(r.skip_count) or 0)
  end
  local e = embed({
    title = ("STRIFE Taste Profile: <@%s>"):format(target_id), description = table.concat(lines, "\n"), color = COLOR.blurple,
    fields = { field("Signals", ("plays %d | finishes %d | likes %d | dislikes %d | skips %d"):format(played, finished, liked, disliked, skipped), false) },
  })
  reply_embed(interaction, e, true)
end)

-- ---------------------------------------------------------------------
-- Personal Playlists & History
-- ---------------------------------------------------------------------

bot:command("strife_main_playlists", { description = "List your saved personal playlists and how many tracks each one contains" },
function(self, interaction)
  local user_id = interaction.member.user.id
  local rows = db("SELECT playlist_name, COUNT(*) AS n FROM strife_user_playlists WHERE user_id = %s GROUP BY playlist_name ORDER BY playlist_name ASC", user_id) or {}
  if #rows == 0 then reply_text(interaction, "You do not have any saved playlists yet.", true); return end
  local lines = {}
  for i, r in ipairs(rows) do
    if i > 20 then break end
    lines[#lines + 1] = ("\226\128\162 **%s** -- %d track(s)"):format(r.playlist_name, tonumber(r.n))
  end
  reply_embed(interaction, embed({ title = "\240\159\142\188 Your Saved Playlists", description = table.concat(lines, "\n"), color = COLOR.blurple }), true)
end)

bot:command("strife_main_deleteplaylist", {
  description = "Delete one of your saved personal playlists by name",
  options = { { type = OPT.STRING, name = "name", description = "Playlist name", required = true } },
}, function(self, interaction)
  local user_id = interaction.member.user.id
  local name = opt(interaction, "name", "")
  local row = db1("SELECT COUNT(*) AS n FROM strife_user_playlists WHERE user_id = %s AND playlist_name = %s", user_id, name)
  local count = row and tonumber(row.n) or 0
  if count == 0 then reply_text(interaction, "That playlist was not found.", true); return end
  db("DELETE FROM strife_user_playlists WHERE user_id = %s AND playlist_name = %s", user_id, name)
  reply_embed(interaction, embed({ description = ("\240\159\151\145\239\184\143 Deleted **%s** (%d track(s))."):format(name, count), color = COLOR.green }), true)
end)

bot:command("strife_main_savequeue", {
  description = "Save the current queue to one of your personal playlists so you can load it again later.",
  options = { { type = OPT.STRING, name = "name", description = "Playlist name", required = true } },
}, function(self, interaction)
  defer(interaction, true)
  local guild_id, user_id = interaction.guild_id, interaction.member.user.id
  local name = opt(interaction, "name", "")
  local rows = db("SELECT video_url, title FROM strife_queue WHERE guild_id = %s AND bot_name = 'strife' ORDER BY id ASC", guild_id) or {}
  if #rows == 0 then followup_text(interaction, "Queue is empty!", true); return end
  for _, r in ipairs(rows) do
    db("INSERT INTO strife_user_playlists (user_id, playlist_name, video_url, title) VALUES (%s, %s, %s, %s)", user_id, name, r.video_url, r.title)
  end
  followup_embed(interaction, embed({ description = ("\240\159\146\190 Saved **%d** tracks to your personal playlist: **%s**"):format(#rows, name), color = COLOR.green }), true)
end)

bot:command("strife_main_loadqueue", {
  description = "Load one of your saved personal playlists into the active queue and start playback if needed.",
  options = { { type = OPT.STRING, name = "name", description = "Playlist name", required = true } },
}, function(self, interaction)
  defer(interaction, true)
  local guild_id, user_id = interaction.guild_id, interaction.member.user.id
  local name = opt(interaction, "name", "")
  local rows = db("SELECT video_url, title FROM strife_user_playlists WHERE user_id = %s AND playlist_name = %s", user_id, name) or {}
  if #rows == 0 then followup_text(interaction, "Playlist not found or empty.", true); return end
  for _, r in ipairs(rows) do
    enqueue_track(guild_id, r.video_url, r.title, user_id, new_track_uid())
  end
  followup_embed(interaction, embed({ description = ("\240\159\147\130 Loaded **%d** tracks from **%s** into the queue!"):format(#rows, name), color = COLOR.green }), true)
  if playback[guild_id] == nil then
    local channel_id = user_voice_channel(guild_id, user_id)
    if channel_id then process_queue(guild_id, channel_id) end
  end
end)

bot:command("strife_main_leaderboard", { description = "Show the most played tracks from this server based on stored playback history." },
function(self, interaction)
  defer(interaction, true)
  local rows = db("SELECT title, COUNT(*) AS plays FROM strife_history WHERE guild_id = %s GROUP BY title ORDER BY plays DESC LIMIT 10", interaction.guild_id) or {}
  if #rows == 0 then followup_text(interaction, "No play history yet.", true); return end
  local lines = {}
  for i, r in ipairs(rows) do lines[#lines + 1] = ("**%d.** %s *(Played %d times)*"):format(i, r.title, tonumber(r.plays)) end
  followup_embed(interaction, embed({ title = "\240\159\143\134 Server Top Tracks", description = table.concat(lines, "\n"), color = COLOR.gold }), true)
end)

bot:command("strife_main_history", { description = "Show the most recent tracks played in this server from playback history." },
function(self, interaction)
  defer(interaction, true)
  local rows = db("SELECT title FROM strife_history WHERE guild_id = %s ORDER BY played_at DESC LIMIT 5", interaction.guild_id) or {}
  if #rows == 0 then followup_text(interaction, "No history.", true); return end
  local lines = {}
  for _, r in ipairs(rows) do lines[#lines + 1] = "- " .. r.title end
  followup_embed(interaction, embed({ title = "\240\159\147\156 History", description = table.concat(lines, "\n"), color = COLOR.blurple }), true)
end)

bot:command("strife_main_userhistory", {
  description = "Show the most recent tracks requested by a specific user in this server.",
  options = { { type = OPT.USER, name = "member", description = "Member to look up", required = true } },
}, function(self, interaction)
  local guild_id = interaction.guild_id
  local member_id = opt(interaction, "member")
  local rows = db("SELECT id, title, video_url FROM strife_history WHERE guild_id = %s AND requester_id = %s ORDER BY played_at DESC LIMIT 10", guild_id, member_id) or {}
  if #rows == 0 then
    reply_embed(interaction, embed({ description = ("\240\159\147\173 <@%s> hasn't queued any songs yet."):format(member_id), color = COLOR.red }), true)
    return
  end
  local lines = {}
  for i, r in ipairs(rows) do lines[#lines + 1] = ("**%d.** [%s](%s)"):format(i, r.title, r.video_url) end
  reply_embed(interaction, embed({
    title = ("\240\159\142\167 <@%s>'s Play History"):format(member_id), description = table.concat(lines, "\n"), color = COLOR.blue,
    footer = "Use /strife_main_steal <user> <number> to add one to the queue!",
  }), true)
end)

bot:command("strife_main_steal", {
  description = "Copy a track from a member's request history and add it back into the queue.",
  options = {
    { type = OPT.USER, name = "member", description = "Member whose history to steal from", required = true },
    { type = OPT.INTEGER, name = "track_number", description = "Which of their history entries (1 = most recent)", required = true, min_value = 1 },
  },
}, function(self, interaction)
  local guild_id = interaction.guild_id
  local member_id = opt(interaction, "member")
  local track_number = opt(interaction, "track_number", 1)
  local row = db1("SELECT video_url, title FROM strife_history WHERE guild_id = %s AND requester_id = %s ORDER BY played_at DESC LIMIT 1 OFFSET %s", guild_id, member_id, track_number - 1)
  if not row then
    reply_embed(interaction, embed({ description = ("\226\157\140 Could not find track #%d in their history."):format(track_number), color = COLOR.red }), true)
    return
  end
  enqueue_track(guild_id, row.video_url, row.title, interaction.member.user.id, new_track_uid())
  reply_embed(interaction, embed({ title = "\240\159\165\183 Song Stolen!", description = ("Added **%s** to the queue from <@%s>'s history."):format(row.title, member_id), color = COLOR.green }))
  if playback[guild_id] == nil then
    local channel_id = user_voice_channel(guild_id, interaction.member.user.id)
    if channel_id then process_queue(guild_id, channel_id) end
  end
end)

bot:command("strife_main_grab", { description = "Send yourself the currently playing track in a direct message for easy saving or sharing." },
function(self, interaction)
  defer(interaction, true)
  local guild_id = interaction.guild_id
  local data = playback[guild_id]
  if not data then
    followup_embed(interaction, embed({ description = "Nothing is currently playing.", color = COLOR.red }), true)
    return
  end
  local user_id = interaction.member.user.id
  local dm_channel, err = bot.rest:post("/users/@me/channels", { recipient_id = user_id })
  if not dm_channel or not dm_channel.id then
    followup_embed(interaction, embed({ description = "\226\157\140 I can't DM you! Please check your privacy settings.", color = COLOR.red }), true)
    return
  end
  local user_name = (interaction.member.user.global_name or interaction.member.user.username or "there")
  bot.rest:post(("/channels/%s/messages"):format(dm_channel.id), {
    embeds = { embed({
      title = "\240\159\142\181 Track Saved!",
      description = ("Hey **%s**!\nHere is the track you wanted to save:\n\n**[%s](%s)**"):format(user_name, data.title, data.url),
      color = 0x5865F2,
    }) },
  })
  followup_embed(interaction, embed({ description = "\240\159\147\172 Check your DMs!", color = COLOR.green }), true)
end)

-- ---------------------------------------------------------------------
-- Server Configuration
-- ---------------------------------------------------------------------

bot:command("strife_main_sethome", {
  description = "Save this bot's default voice or stage channel for join, autoplay, and recovery behavior.",
  default_member_permissions = "8",
  options = { { type = OPT.CHANNEL, name = "channel", description = "Home voice/stage channel", required = true, channel_types = { CH_VOICE, CH_STAGE } } },
}, function(self, interaction)
  if not is_admin(interaction) then reply_text(interaction, "Administrator only.", true); return end
  defer(interaction, true)
  local channel_id = opt(interaction, "channel")
  db([[INSERT INTO strife_bot_home_channels (guild_id, bot_name, home_vc_id) VALUES (%s, 'strife', %s)
       ON CONFLICT (guild_id, bot_name) DO UPDATE SET home_vc_id = EXCLUDED.home_vc_id]], interaction.guild_id, channel_id)
  followup_embed(interaction, embed({ title = "\240\159\143\160 Home Set", description = ("Home channel set to <#%s>."):format(channel_id), color = COLOR.green }), true)
end)

bot:command("strife_main_setfeedback", {
  description = "Choose the text channel (or thread) for updates, queue actions, and recovery notices.",
  default_member_permissions = "8",
  options = { { type = OPT.CHANNEL, name = "channel", description = "Feedback text channel/thread", required = true, channel_types = { CH_TEXT, CH_THREAD } } },
}, function(self, interaction)
  if not is_admin(interaction) then reply_text(interaction, "Administrator only.", true); return end
  defer(interaction, true)
  local channel_id = opt(interaction, "channel")
  ensure_guild_settings(interaction.guild_id)
  db("UPDATE strife_guild_settings SET feedback_channel_id = %s WHERE guild_id = %s", channel_id, interaction.guild_id)
  followup_embed(interaction, embed({ title = "\226\156\133 Feedback Channel Set", description = ("Updates will be sent to <#%s>."):format(channel_id), color = COLOR.green }), true)
end)

bot:command("strife_main_djrole", {
  description = "Set the server DJ role that can manage restricted playback, queue, and settings commands.",
  default_member_permissions = "8",
  options = { { type = OPT.ROLE, name = "role", description = "DJ role", required = true } },
}, function(self, interaction)
  if not is_admin(interaction) then reply_text(interaction, "Administrator only.", true); return end
  defer(interaction, true)
  local role_id = opt(interaction, "role")
  ensure_guild_settings(interaction.guild_id)
  db("UPDATE strife_guild_settings SET dj_role_id = %s WHERE guild_id = %s", role_id, interaction.guild_id)
  followup_embed(interaction, embed({ description = ("\240\159\142\167 DJ role set to <@&%s>"):format(role_id), color = COLOR.green }), true)
end)

bot:command("strife_main_removedj", { description = "Clear the configured DJ role so only admins or open-access mode can control restricted commands.", default_member_permissions = "8" },
function(self, interaction)
  if not is_admin(interaction) then reply_text(interaction, "Administrator only.", true); return end
  defer(interaction, true)
  ensure_guild_settings(interaction.guild_id)
  db("UPDATE strife_guild_settings SET dj_role_id = %s WHERE guild_id = %s", PG_NULL, interaction.guild_id)
  followup_embed(interaction, embed({ description = "DJ role requirements removed.", color = COLOR.green }), true)
end)

bot:command("strife_main_djmode", { description = "Enable or disable Strict DJ Mode so only admins and the DJ role can use control commands.", default_member_permissions = "8" },
function(self, interaction)
  if not is_admin(interaction) then reply_text(interaction, "Administrator only.", true); return end
  defer(interaction, true)
  local guild_id = interaction.guild_id
  ensure_guild_settings(guild_id)
  local row = db1("SELECT dj_only_mode FROM strife_guild_settings WHERE guild_id = %s", guild_id)
  local new_val = not (row and row.dj_only_mode)
  db("UPDATE strife_guild_settings SET dj_only_mode = %s WHERE guild_id = %s", new_val, guild_id)
  followup_embed(interaction, embed({ description = ("\240\159\142\167 Strict DJ Mode is now **%s**."):format(new_val and "ENABLED" or "DISABLED"), color = COLOR.green }), true)
end)

bot:command("strife_main_247", { description = "Keep the bot connected and ready in voice channels even after playback ends until you disable it.", default_member_permissions = "8" },
function(self, interaction)
  if not is_admin(interaction) then reply_text(interaction, "Administrator only.", true); return end
  defer(interaction, true)
  local guild_id = interaction.guild_id
  ensure_guild_settings(guild_id)
  local row = db1("SELECT stay_in_vc FROM strife_guild_settings WHERE guild_id = %s", guild_id)
  local new_val = not (row and row.stay_in_vc)
  db("UPDATE strife_guild_settings SET stay_in_vc = %s WHERE guild_id = %s", new_val, guild_id)
  followup_embed(interaction, embed({ description = ("\240\159\149\176\239\184\143 24/7 Mode is now **%s**."):format(new_val and "ENABLED" or "DISABLED"), color = COLOR.green }), true)
end)

bot:command("strife_main_settings", { description = "Show the saved playback, DJ, queue, and recovery settings for this server" },
function(self, interaction)
  defer(interaction, true)
  local guild_id = interaction.guild_id
  local s = get_guild_settings(guild_id)
  local autodj_state = get_autodj_enabled(guild_id)
  local e = embed({ title = "\226\154\153\239\184\143 Server Music Settings", color = COLOR.blurple, fields = {
    field("Home Channel", s.home_vc_id and ("<#" .. s.home_vc_id .. ">") or "Not set", true),
    field("Feedback Channel", s.feedback_channel_id and ("<#" .. s.feedback_channel_id .. ">") or "Not set", true),
    field("DJ Role", s.dj_role_id and ("<@&" .. s.dj_role_id .. ">") or "Not set", true),
    field("Volume", s.volume, true),
    field("Loop Mode", s.loop_mode, true),
    field("Filter", s.filter_mode, true),
    field("Transitions", s.transition_mode, true),
    field("Custom Speed/Pitch", ("%gx / %gx (%d left)"):format(s.custom_speed, s.custom_pitch, s.custom_modifiers_left), true),
    field("Strict DJ", s.dj_only_mode and "Enabled" or "Disabled", true),
    field("Auto-DJ", autodj_state and "Enabled" or "Disabled", true),
    field("24/7 Mode", s.stay_in_vc and "Enabled" or "Disabled", true),
  } })
  followup_embed(interaction, e, true)
end)

bot:command("strife_main_restart", { description = "Restart this bot instance immediately for maintenance or recovery. Administrator only.", default_member_permissions = "8" },
function(self, interaction)
  if not is_admin(interaction) then reply_text(interaction, "Administrator only.", true); return end
  reply_text(interaction, "Restarting... Materia reconfiguring, back in a moment.", true)
  copas.addthread(function()
    copas.sleep(1)
    os.exit(0) -- container restart policy brings the process back (matches ARIA_RESTART_STRIFE_CMD)
  end)
end)

-- ---------------------------------------------------------------------
-- Utility
-- ---------------------------------------------------------------------

bot:command("strife_main_ping", { description = "Show the bot websocket latency so you can quickly check responsiveness." },
function(self, interaction)
  reply_embed(interaction, embed({ description = "\240\159\143\147 Pong!", color = COLOR.green }), true)
end)

bot:command("strife_main_uptime", { description = "Show how long this bot process has been running since its last startup." },
function(self, interaction)
  local secs = os.time() - bot.start_time
  local h = math.floor(secs / 3600)
  local m = math.floor((secs % 3600) / 60)
  local s = secs % 60
  reply_embed(interaction, embed({ description = ("\226\143\177\239\184\143 Uptime: %d:%02d:%02d"):format(h, m, s), color = COLOR.green }), true)
end)

bot:command("strife_main_stats", { description = "Show quick bot statistics such as guild count and active player count." },
function(self, interaction)
  local n = 0
  for _ in pairs(playback) do n = n + 1 end
  reply_embed(interaction, embed({ description = ("\240\159\147\138 Active Players: %d"):format(n), color = COLOR.green }), true)
end)

bot:command("strife_main_lyrics", { description = "Show lyrics for the currently playing track, synced to the current position when available." },
function(self, interaction)
  local data = playback[interaction.guild_id]
  if not data then
    reply_embed(interaction, embed({ description = "Nothing is playing.", color = COLOR.red }), true)
    return
  end
  defer(interaction, false)
  local lyrics = fetch_lyrics(data.title)
  if not lyrics then
    followup_embed(interaction, embed({ description = ("No lyrics found for **%s**."):format(data.title), color = COLOR.red }))
    return
  end
  followup_embed(interaction, embed({ title = "\240\159\147\156 " .. data.title, description = lyrics:sub(1, 4000), color = COLOR.blue }))
end)

-- ---------------------------------------------------------------------
-- Interactive Control Panel
-- ---------------------------------------------------------------------

local function build_panel_embed(guild_id)
  local s = get_guild_settings(guild_id)
  local data = playback[guild_id]
  local e = embed({ title = "\240\159\142\155\239\184\143 STRIFE Music Control Panel", color = COLOR.grey })
  e.fields = {}
  if data then
    local bar = make_progress_bar(current_position(guild_id), data.duration)
    local now_playing = ("**[%s](%s)**"):format(data.title or "Playing", data.url or "")
    e.fields[#e.fields + 1] = field("Now Playing", now_playing .. "\n`" .. bar .. "`", false)
    if data.requester_id then
      e.fields[#e.fields + 1] = field("Requested by", resolve_requester_name(guild_id, data.requester_id), true)
    end
    e.fields[#e.fields + 1] = field("Filter", data.current_filter or "none", true)
  else
    e.description = "Nothing is playing right now."
  end
  e.fields[#e.fields + 1] = field("Volume", tostring(s.volume) .. "%", true)
  e.fields[#e.fields + 1] = field("Loop", tostring(s.loop_mode), true)
  e.footer = { text = "STRIFE Main Music System" }
  return e
end

local PANEL_COMPONENTS = {
  { type = 1, components = {
    { type = 2, style = 1, label = "\226\143\175\239\184\143 Play/Pause", custom_id = "strife:panel:playpause" },
    { type = 2, style = 4, label = "\226\143\185\239\184\143 Stop", custom_id = "strife:panel:stop" },
    { type = 2, style = 2, label = "\226\143\173\239\184\143 Skip", custom_id = "strife:panel:skip" },
  } },
  { type = 1, components = {
    { type = 2, style = 2, label = "\226\143\170 -10s", custom_id = "strife:panel:rw" },
    { type = 2, style = 2, label = "\226\143\169 +10s", custom_id = "strife:panel:fw" },
    { type = 2, style = 2, label = "\240\159\148\137 Vol-", custom_id = "strife:panel:voldown" },
    { type = 2, style = 2, label = "\240\159\148\138 Vol+", custom_id = "strife:panel:volup" },
  } },
  { type = 1, components = {
    { type = 2, style = 2, label = "\240\159\148\129 Loop", custom_id = "strife:panel:looptoggle" },
    { type = 2, style = 3, label = "\240\159\148\128 Shuffle", custom_id = "strife:panel:shuffle" },
    { type = 2, style = 2, label = "\240\159\147\156 View Queue", custom_id = "strife:panel:queue" },
  } },
}

bot:command("strife_main_panel", { description = "Post an interactive control panel with playback, queue, and transport buttons." },
function(self, interaction)
  -- reply_embed()/callback() only cover plain content/embeds; the panel also
  -- needs `components`, so build the INTERACTION_CALLBACK payload directly.
  callback(interaction, 4, { embeds = { build_panel_embed(interaction.guild_id) }, components = PANEL_COMPONENTS })
end)

bot:command("strife_main_help", { description = "Show a categorized help menu for all STRIFE music commands and utilities" },
function(self, interaction)
  local e = embed({
    title = "\240\159\151\161\239\184\143 STRIFE Command Deck",
    description = "SOLDIER 1st Class music operations. All commands are namespaced `/strife_main_*`.",
    color = COLOR.blurple,
    fields = {
      field("Playback & Transport", "play, playnext, skip, skipto, stop, sleep, pause, resume, replay, seek, forward, rewind, join, leave, nowplaying", false),
      field("Queue Management", "queue, remove, move, bump, clearmine, clear, shuffle, voteskip, upvote, downvote, loop", false),
      field("Audio & Filters", "volume, filter, fade, modify", false),
      field("Smart Auto-DJ & Discovery", "autodj, radio, recommend, like, dislike, taste", false),
      field("Personal Playlists & History", "playlists, savequeue, loadqueue, deleteplaylist, history, userhistory, leaderboard, steal, grab", false),
      field("Server Configuration", "sethome, setfeedback, djrole, removedj, djmode, 247, settings, restart", false),
      field("Utility", "panel, ping, uptime, stats, lyrics, help", false),
    },
    footer = "STRIFE Main Music System -- Buster Sword Edition",
  })
  reply_embed(interaction, e, true)
end)

-- =====================================================================
-- AUTOCOMPLETE + MESSAGE-COMPONENT (panel button) ROUTING
-- These are extra handlers on the same gateway event bot.lua already
-- listens to for slash-command dispatch (type 2); Gateway:on supports
-- multiple handlers per event, so this doesn't touch swarmlua/bot.lua.
-- =====================================================================

local function focused_opt(interaction)
  local options = interaction.data and interaction.data.options
  if not options then return nil end
  for _, o in ipairs(options) do
    if o.focused then return o end
  end
  return nil
end

local function autocomplete_result(interaction, choices)
  return callback(interaction, 8, { choices = choices })
end

local function trim(s) return tostring(s or ""):match("^%s*(.-)%s*$") end

-- Best-effort YouTube "suggest" lookup for /play autocomplete; never raises,
-- always fast (mirrors strife.py's play_search_autocomplete()).
local function youtube_suggest(query)
  local data = http_get_json("https://suggestqueries.google.com/complete/search?client=firefox&ds=yt&q=" .. url_encode(query))
  if not data or not data[2] then return {} end
  local out = {}
  for _, s in ipairs(data[2]) do
    if #out >= 10 then break end
    out[#out + 1] = tostring(s)
  end
  return out
end

local function queue_index_choices(guild_id, needle)
  local rows = db("SELECT title FROM strife_queue WHERE guild_id = %s AND bot_name = 'strife' ORDER BY id ASC LIMIT 200", guild_id) or {}
  needle = trim(needle):lower()
  local out = {}
  for i, r in ipairs(rows) do
    local title = r.title or "Unknown title"
    if needle == "" or needle == tostring(i) or title:lower():find(needle, 1, true) then
      out[#out + 1] = { name = ("%d. %s"):format(i, title):sub(1, 95), value = i }
      if #out >= 25 then break end
    end
  end
  return out
end

local AUTOCOMPLETE_INDEX_COMMANDS = {
  strife_main_remove = true, strife_main_skipto = true, strife_main_move = true, strife_main_bump = true,
  strife_main_upvote = true, strife_main_downvote = true,
}

bot.gateway:on("INTERACTION_CREATE", function(interaction)
  if interaction.type == 4 then -- APPLICATION_COMMAND_AUTOCOMPLETE
    local name = interaction.data and interaction.data.name
    local focused = focused_opt(interaction)
    if not focused then autocomplete_result(interaction, {}); return end
    local ok, choices = pcall(function()
      if name == "strife_main_play" and focused.name == "search" then
        local current = trim(focused.value)
        local suggestions = {}
        if #current >= 2 and not is_explicit_query(current) then
          suggestions = youtube_suggest(current)
        end
        if #suggestions == 0 and current ~= "" then suggestions = { current } end
        local out = {}
        for i, s in ipairs(suggestions) do
          if i > 10 then break end
          out[#out + 1] = { name = s:sub(1, 100), value = s:sub(1, 100) }
        end
        return out
      elseif AUTOCOMPLETE_INDEX_COMMANDS[name] and (focused.name == "index" or focused.name == "frm" or focused.name == "to") then
        return queue_index_choices(interaction.guild_id, focused.value)
      end
      return {}
    end)
    autocomplete_result(interaction, ok and choices or {})
    return
  end

  if interaction.type == 3 then -- MESSAGE_COMPONENT (panel buttons)
    local cid = interaction.data and interaction.data.custom_id or ""
    local action = cid:match("^strife:panel:(.+)$")
    if not action then return end
    local guild_id = interaction.guild_id

    -- bot.lua's own on_error hook only covers type=2 (APPLICATION_COMMAND)
    -- handler failures; this whole branch is a second, bot-file-owned
    -- listener on the same gateway event, so wrap it in its own pcall +
    -- report_error, or button failures would never reach
    -- strife_error_events / the error webhook.
    local panel_ok, panel_err = pcall(function()
      local function refresh() update_message(interaction, { embeds = { build_panel_embed(guild_id) }, components = PANEL_COMPONENTS }) end

      if action == "playpause" then
        if not is_dj(interaction, true) then callback(interaction, 4, { content = "DJ role required.", flags = 64 }); return end
        local data = playback[guild_id]
        if data then
          if data.paused then
            bot.lavalink:set_paused(guild_id, false)
            data.paused = false; data.updated_at = os.time()
            persist_playback_state(guild_id, { is_paused = false })
          else
            bot.lavalink:set_paused(guild_id, true)
            data.position = current_position(guild_id); data.paused = true
            persist_playback_state(guild_id, { is_paused = true, position_seconds = data.position })
          end
          refresh()
        else
          callback(interaction, 4, { content = "Nothing is playing.", flags = 64 })
        end
      elseif action == "stop" then
        if not is_dj(interaction, true) then callback(interaction, 4, { content = "DJ role required.", flags = 64 }); return end
        stop_playback(guild_id)
        refresh()
      elseif action == "skip" then
        if not is_dj(interaction, true) then callback(interaction, 4, { content = "DJ role required.", flags = 64 }); return end
        if playback[guild_id] then
          bot.lavalink:stop(guild_id)
          callback(interaction, 4, { content = "\226\143\173\239\184\143 Skipped to next track.", flags = 64 })
        else
          callback(interaction, 4, { content = "Nothing to skip.", flags = 64 })
        end
      elseif action == "rw" or action == "fw" then
        if not is_dj(interaction, true) then callback(interaction, 4, { content = "DJ role required.", flags = 64 }); return end
        if not playback[guild_id] then callback(interaction, 4, { content = "Nothing playing.", flags = 64 }); return end
        local delta = (action == "rw") and -10 or 10
        local new_pos = math.max(0, current_position(guild_id) + delta)
        bot.lavalink:seek(guild_id, new_pos * 1000)
        playback[guild_id].position = new_pos; playback[guild_id].updated_at = os.time()
        refresh()
      elseif action == "voldown" or action == "volup" then
        if not is_dj(interaction, true) then callback(interaction, 4, { content = "DJ role required.", flags = 64 }); return end
        local s = get_guild_settings(guild_id)
        local vol = math.max(1, math.min(200, (s.volume or 100) + ((action == "volup") and 10 or -10)))
        db("UPDATE strife_guild_settings SET volume = %s WHERE guild_id = %s", vol, guild_id)
        if playback[guild_id] then bot.lavalink:set_volume(guild_id, vol) end
        refresh()
      elseif action == "looptoggle" then
        if not is_dj(interaction, true) then callback(interaction, 4, { content = "DJ role required.", flags = 64 }); return end
        local s = get_guild_settings(guild_id)
        local next_mode = ({ off = "queue", queue = "song", song = "off" })[s.loop_mode] or "queue"
        db("UPDATE strife_guild_settings SET loop_mode = %s WHERE guild_id = %s", next_mode, guild_id)
        refresh()
      elseif action == "shuffle" then
        if not is_dj(interaction, true) then callback(interaction, 4, { content = "DJ role required.", flags = 64 }); return end
        local n = shuffle_queue_rows(guild_id, false)
        if n <= 0 then callback(interaction, 4, { content = "Queue empty.", flags = 64 }); return end
        snapshot_queue_backup(guild_id)
        callback(interaction, 4, { content = "\240\159\148\128 Queue successfully shuffled!", flags = 64 })
      elseif action == "queue" then
        local rows = db("SELECT title FROM strife_queue WHERE guild_id = %s AND bot_name = 'strife' ORDER BY id ASC LIMIT 10", guild_id) or {}
        if #rows == 0 then callback(interaction, 4, { content = "Queue is empty.", flags = 64 }); return end
        local lines = { "**Current Queue:**" }
        for i, r in ipairs(rows) do lines[#lines + 1] = ("%d. %s"):format(i, r.title) end
        callback(interaction, 4, { content = table.concat(lines, "\n"), flags = 64 })
      end
    end)
    if not panel_ok then
      print("[strife] panel button handler error: " .. tostring(panel_err))
      report_error(guild_id, "command_error", "panel:" .. tostring(action), tostring(panel_err))
    end
  end
end)

-- =====================================================================
-- BOOT
-- =====================================================================

-- BUGFIX: the Discord gateway sends a fresh READY (not RESUMED) on every
-- invalidated session -- routine reconnects, not just process restarts --
-- and this handler used to redo its full boot-resume/background-loop-spawn
-- pass every single time, which both force-restarted/discarded queued
-- tracks on reconnect and piled up duplicate background loops. Gate on
-- did_initial_ready so it runs exactly once per process (matching what it was
-- actually written for).
local did_initial_ready = false

bot.gateway:on("READY", function()
  if did_initial_ready then return end
  did_initial_ready = true
  send_webhook_log("\240\159\159\162 Node Online", "STRIFE is online and syncing with the swarm.", COLOR.green)
  -- Rejoin and resume for any guild that was playing (or had a non-empty
  -- queue) when this process last stopped -- container recreates/restarts
  -- otherwise leave the bot sitting disconnected with a full queue and no
  -- way back in short of a manual /play.
  copas.addthread(function()
    -- Wait for the Lavalink websocket to actually be connected before
    -- touching the queue -- process_queue deletes each row before resolving
    -- it, so racing this against Lavalink's own (often slow, cold-start)
    -- connect meant every "no response from Lavalink" failure permanently
    -- destroyed that queued track instead of just skipping playback.
    local waited = 0
    while not (bot.lavalink and bot.lavalink.session_id) and waited < 60 do
      copas.sleep(1)
      waited = waited + 1
    end
    local rows = db("SELECT DISTINCT guild_id FROM strife_bot_home_channels WHERE bot_name = 'strife'") or {}
    for _, row in ipairs(rows) do
      local gid = row.guild_id
      local channel_id = home_channel(gid)
      if channel_id then
        local has_queue = queue_count(gid) > 0
        local was_playing = db1("SELECT is_playing FROM strife_playback_state WHERE guild_id = %s AND bot_name = 'strife'", gid)
        if has_queue or (was_playing and was_playing.is_playing) then
          print(("[strife] [%s] resuming on boot (queue=%s, was_playing=%s)"):format(tostring(gid), tostring(has_queue), tostring(was_playing and was_playing.is_playing)))
          process_queue(gid, channel_id)
        end
      end
    end
  end)
end)

bootstrap_schema()

local function command_count()
  local n = 0
  for _ in pairs(bot.commands) do n = n + 1 end
  return n
end

-- Exposes internals for the DB/logic smoke test (db_smoke_test.lua); not
-- reached in normal operation (that script sets this env var itself).
-- Mirrors lockhart.lua's/tunestream's STRIFE_DRY_RUN=test export pattern.
if env("STRIFE_DRY_RUN") == "test" then
  return {
    db = db, db1 = db1,
    ensure_guild_settings = ensure_guild_settings, get_guild_settings = get_guild_settings,
    home_channel = home_channel, get_autodj_enabled = get_autodj_enabled, set_autodj_enabled = set_autodj_enabled,
    enqueue_track = enqueue_track, insert_queue_front = insert_queue_front,
    snapshot_queue_backup = snapshot_queue_backup, shuffle_queue_rows = shuffle_queue_rows,
    restore_queue_from_backup = restore_queue_from_backup,
    queue_count = queue_count, delete_backup_track = delete_backup_track,
    track_key = track_key, record_track_feedback = record_track_feedback,
    pick_smart_recommendation = pick_smart_recommendation,
    persist_playback_state = persist_playback_state,
    mark_voice_connected = mark_voice_connected, mark_voice_disconnected = mark_voice_disconnected,
    heartbeat_tick = heartbeat_tick, report_error = report_error, persist_error_event = persist_error_event,
    playback = playback, bot = bot, command_count = command_count,
  }
elseif env("STRIFE_DRY_RUN") then
  print(("[strife] dry run OK: %d commands registered, db/lavalink/gateway objects constructed."):format(command_count()))
  os.exit(0)
end

start_background_loops()

copas.addthread(function()
  while true do
    copas.sleep(5)
    local ok, err = pcall(poll_direct_orders)
    if not ok then
      print("[strife] direct order poll error: " .. tostring(err))
      report_error(nil, "runtime", "direct order poll error", tostring(err))
    end
  end
end)

local function recovery_watchdog()
  local stalled = db("SELECT DISTINCT guild_id FROM strife_queue WHERE bot_name = 'strife'") or {}
  for _, row in ipairs(stalled) do
    local guild_id = tostring(row.guild_id)
    if not playback[guild_id] then
      local channel_id = voice_channel[guild_id] or home_channel(guild_id)
      if channel_id then process_queue(guild_id, channel_id) end
    end
  end
end

copas.addthread(function()
  while true do
    copas.sleep(30)
    local ok, err = pcall(recovery_watchdog)
    if not ok then
      print("[strife] recovery_watchdog error: " .. tostring(err))
      report_error(nil, "runtime", "recovery_watchdog error", tostring(err))
    end
  end
end)

print(("[strife] booting -- %d commands registered, Lavalink at %s:%d"):format(command_count(), LAVALINK_HOST, LAVALINK_PORT))

bot:run()
