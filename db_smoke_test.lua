-- Live-DB smoke test for strife.lua's Postgres write paths (mirrors
-- lockhart/tunestream's db_smoke_test.lua pattern). discord_music_strife
-- started completely empty -- no prior MariaDB migration, no pre-existing
-- tables -- so this run also exercises bootstrap_schema() (CREATE TABLE IF
-- NOT EXISTS for every strife_* table + swarm_health) as its first order of
-- business, via strife.lua's own dofile() at require time below. Every test
-- below uses a distinct, clearly-fake TEST_GUILD / TEST_USER* id and cleans
-- up its own rows at the end. There is no real pre-existing guild data in
-- this database to guard against disturbing (confirmed empty via `\dt`
-- before this port), so there is no "pre-existing real guild row untouched"
-- guard-rail check at the end.
--
-- Run with:
--   STRIFE_DRY_RUN=test STRIFE_DB_PORT=5432 STRIFE_DB_NAME=discord_music_strife luajit db_smoke_test.lua

package.path = "./lib/?.lua;./lib/?/init.lua;" .. package.path

-- Expects STRIFE_DRY_RUN=test in the environment already (set by the caller).
local A = dofile("strife.lua")

-- Lua table constructors silently drop any key whose value is written as
-- literal `nil` (the key never gets created, so pairs() never sees it) --
-- that's exactly why strife.lua's own call sites pass this pgmoon NULL
-- sentinel instead of `nil` whenever a patch needs to explicitly clear a
-- column. Using real Lua `nil` here would silently no-op those columns
-- instead of clearing them, which very nearly hid as a false test failure.
local pgmoon = require("pgmoon")
local PG_NULL = pgmoon.Postgres.NULL

local TEST_GUILD = "999900031"
local TEST_USER = "999900032"
local TEST_USER2 = "999900033"

local pass, fail = 0, 0
local function check(label, ok, err)
  if ok then
    pass = pass + 1
    print("OK   " .. label)
  else
    fail = fail + 1
    print("FAIL " .. label .. " -> " .. tostring(err))
  end
end

-- guild settings upsert/read/write
A.ensure_guild_settings(TEST_GUILD)
local settings = A.get_guild_settings(TEST_GUILD)
check("get_guild_settings returns row", settings ~= nil)
A.db("UPDATE strife_guild_settings SET volume = %s, loop_mode = %s WHERE guild_id = %s", 77, "song", TEST_GUILD)
settings = A.get_guild_settings(TEST_GUILD)
check("volume persisted", tonumber(settings.volume) == 77, tostring(settings.volume))
check("loop_mode persisted", settings.loop_mode == "song", tostring(settings.loop_mode))

-- queue lifecycle
A.enqueue_track(TEST_GUILD, "https://example.com/a", "Track A", TEST_USER, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
A.enqueue_track(TEST_GUILD, "https://example.com/b", "Track B", TEST_USER, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
A.insert_queue_front(TEST_GUILD, "https://example.com/z", "Track Z (front)", TEST_USER, "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz")
check("queue_count after 3 inserts", A.queue_count(TEST_GUILD) == 3, A.queue_count(TEST_GUILD))
local rows = A.db("SELECT video_url, title FROM strife_queue WHERE guild_id = %s AND bot_name = 'strife' ORDER BY id ASC", TEST_GUILD)
check("insert_queue_front puts Z first", rows and rows[1] and rows[1].title == "Track Z (front)", rows and rows[1] and rows[1].title)

A.snapshot_queue_backup(TEST_GUILD)
local backup_count = A.db1("SELECT COUNT(*)::int AS n FROM strife_queue_backup WHERE guild_id = %s AND bot_name = 'strife'", TEST_GUILD)
check("backup snapshot matches queue size", backup_count and tonumber(backup_count.n) == 3, backup_count and backup_count.n)

local n = A.shuffle_queue_rows(TEST_GUILD, true)
check("shuffle_queue_rows returns count", n == 3, n)

-- clear queue, then restore from backup (the loop_mode=="queue" recovery
-- path process_queue_impl falls back to when the live queue is empty)
A.db("DELETE FROM strife_queue WHERE guild_id = %s AND bot_name = 'strife'", TEST_GUILD)
check("queue emptied", A.queue_count(TEST_GUILD) == 0, A.queue_count(TEST_GUILD))
local restored = A.restore_queue_from_backup(TEST_GUILD)
check("restore_queue_from_backup restores 3", restored == 3 and A.queue_count(TEST_GUILD) == 3, restored)

-- delete_backup_track (used by remove/skipto/downvote/bump)
A.delete_backup_track(TEST_GUILD, nil, "https://example.com/z", "Track Z (front)")
local backup_after = A.db1("SELECT COUNT(*)::int AS n FROM strife_queue_backup WHERE guild_id = %s AND bot_name = 'strife'", TEST_GUILD)
check("delete_backup_track removes one row", backup_after and tonumber(backup_after.n) == 2, backup_after and backup_after.n)

-- track_intelligence / user_track_affinity: every NOT-NULL counter column
-- (queued_count, play_count, finish_count, skip_count, like_count,
-- dislike_count, total_listen_seconds / score) must be zero-filled on first
-- insert, per this file's own SCHEMA_TABLES CREATE TABLE statements (every
-- counter column declared NOT NULL DEFAULT 0 -- Postgres only auto-fills the
-- DEFAULT for columns an INSERT's column-list omits entirely, so every write
-- path below still must be checked to confirm it never names a counter
-- without a value).
check("enqueue_track's queued_count upsert ran (>=1) for track A", (function()
  local ti = A.db1("SELECT queued_count FROM strife_track_intelligence WHERE guild_id = %s AND url_key = %s", TEST_GUILD, A.track_key("https://example.com/a", "Track A"))
  return ti and tonumber(ti.queued_count) >= 1, ti and tostring(ti.queued_count)
end)())

local ok6, err6 = pcall(A.record_track_feedback, TEST_GUILD, TEST_USER, "https://example.com/a", "Track A", true)
check("record_track_feedback like (NOT NULL counter zero-fill)", ok6, err6)
local ok7, err7 = pcall(A.record_track_feedback, TEST_GUILD, TEST_USER2, "https://example.com/b", "Track B", false)
check("record_track_feedback dislike", ok7, err7)

local key_a = A.track_key("https://example.com/a", "Track A")
local ti = A.db1("SELECT queued_count, play_count, finish_count, like_count, dislike_count, skip_count, total_listen_seconds FROM strife_track_intelligence WHERE guild_id = %s AND url_key = %s", TEST_GUILD, key_a)
check("track_intelligence row has all counters non-null",
  ti and ti.queued_count ~= nil and ti.play_count ~= nil and ti.finish_count ~= nil
    and ti.like_count ~= nil and ti.dislike_count ~= nil and ti.skip_count ~= nil and ti.total_listen_seconds ~= nil,
  tostring(ti))
check("track_intelligence like_count == 1 after one like", ti and tonumber(ti.like_count) == 1, ti and tostring(ti.like_count))

local aff = A.db1("SELECT queued_count, play_count, finish_count, skip_count, like_count, dislike_count, score FROM strife_user_track_affinity WHERE guild_id = %s AND user_id = %s AND url_key = %s", TEST_GUILD, TEST_USER, key_a)
check("user_track_affinity row has all counters non-null",
  aff and aff.queued_count ~= nil and aff.play_count ~= nil and aff.finish_count ~= nil
    and aff.skip_count ~= nil and aff.like_count ~= nil and aff.dislike_count ~= nil and aff.score ~= nil,
  tostring(aff))
check("user_track_affinity score positive after a queue + a like", aff and tonumber(aff.score) > 0, aff and tostring(aff.score))
check("user_track_affinity queued_count from enqueue_track >= 1", aff and tonumber(aff.queued_count) >= 1, aff and tostring(aff.queued_count))

-- autodj toggle
A.set_autodj_enabled(TEST_GUILD, true)
check("autodj enabled true", A.get_autodj_enabled(TEST_GUILD) == true)
A.set_autodj_enabled(TEST_GUILD, false)
check("autodj enabled false", A.get_autodj_enabled(TEST_GUILD) == false)

-- smart recommendation (falls back to "server favorites" / history fallback query)
local ok9, rec, reason = pcall(A.pick_smart_recommendation, TEST_GUILD, { TEST_USER }, nil)
check("pick_smart_recommendation runs without error", ok9, rec)

-- playback_state upsert/reset (the write path used by process_queue,
-- /clear, /stop's eventual TrackEndEvent settle, and /sleep)
A.persist_playback_state(TEST_GUILD, {
  channel_id = "111", video_url = "https://example.com/a", title = "Track A",
  position_seconds = 42, is_playing = true, is_paused = false, track_uid = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
})
local ps = A.db1("SELECT position_seconds, is_playing FROM strife_playback_state WHERE guild_id = %s AND bot_name = 'strife'", TEST_GUILD)
check("playback_state upsert persisted (INSERT path)", ps and tonumber(ps.position_seconds) == 42 and ps.is_playing == true, tostring(ps))
A.persist_playback_state(TEST_GUILD, { position_seconds = 55 })
ps = A.db1("SELECT position_seconds, is_playing FROM strife_playback_state WHERE guild_id = %s AND bot_name = 'strife'", TEST_GUILD)
check("playback_state upsert persisted (UPDATE path)", ps and tonumber(ps.position_seconds) == 55 and ps.is_playing == true, tostring(ps))
local ok_clear, err_clear = pcall(A.persist_playback_state, TEST_GUILD, { is_playing = false, is_paused = false, video_url = PG_NULL, title = PG_NULL, position_seconds = 0, track_uid = PG_NULL })
check("persist_playback_state runs with PG_NULL-valued patch fields", ok_clear, err_clear)
ps = A.db1("SELECT video_url, is_playing, position_seconds FROM strife_playback_state WHERE guild_id = %s AND bot_name = 'strife'", TEST_GUILD)
check("persist_playback_state actually clears the row", ps and ps.video_url == nil and ps.is_playing == false and tonumber(ps.position_seconds) == 0, tostring(ps))

-- voice_state: the reconnect_attempts NOT-NULL gotcha (this file's own
-- SCHEMA_TABLES declares reconnect_attempts INTEGER NOT NULL DEFAULT 0 --
-- confirming mark_voice_connected's INSERT still works even though it
-- never names reconnect_attempts itself, relying on the column DEFAULT).
local ok12, err12 = pcall(A.mark_voice_connected, TEST_GUILD, "222")
check("mark_voice_connected (reconnect_attempts NOT NULL zero-fill)", ok12, err12)
local vs = A.db1("SELECT connected_channel_id::text AS connected_channel_id, desired_connected, reconnect_attempts FROM strife_voice_state WHERE guild_id = %s AND bot_name = 'strife'", TEST_GUILD)
check("voice_state connected row correct", vs and vs.connected_channel_id == "222" and vs.desired_connected == true and tonumber(vs.reconnect_attempts) == 0, tostring(vs))
local ok13, err13 = pcall(A.mark_voice_disconnected, TEST_GUILD)
check("mark_voice_disconnected runs", ok13, err13)
vs = A.db1("SELECT connected_channel_id, desired_connected FROM strife_voice_state WHERE guild_id = %s AND bot_name = 'strife'", TEST_GUILD)
check("voice_state disconnected row correct", vs and vs.connected_channel_id == nil and vs.desired_connected == false, tostring(vs))

-- heartbeat_tick / strife_metrics (the %s-placeholder-count vs args-supplied
-- gotcha that has bitten other sibling bots: 10 %s placeholders in the
-- VALUES clause must line up 1:1 with the 10 args passed to db(), or values
-- shift into the wrong column -- e.g. a duration landing in position_seconds).
A.playback[TEST_GUILD] = {
  channel_id = "111", url = "https://example.com/a", title = "Track A",
  duration = 200, position = 55, updated_at = os.time(), paused = false, track_uid = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
}
A.enqueue_track(TEST_GUILD, "https://example.com/c", "Track C", TEST_USER, "cccccccccccccccccccccccccccccccc") -- so queue_count > 0
local ok15, err15 = pcall(A.heartbeat_tick)
check("heartbeat_tick runs with an active player", ok15, err15)
local metrics = A.db1("SELECT queue_count, is_playing_db, is_paused_db, position_seconds, duration_seconds FROM strife_metrics WHERE guild_id = %s AND bot_name = 'strife'", TEST_GUILD)
check("strife_metrics row written", metrics ~= nil, tostring(metrics))
check("strife_metrics is_paused_db is boolean false, not a position/duration value", metrics and metrics.is_paused_db == false, metrics and tostring(metrics.is_paused_db))
check("strife_metrics position_seconds is a plausible position (current_position, not duration/title)", metrics and tonumber(metrics.position_seconds) ~= nil and tonumber(metrics.position_seconds) < 300, metrics and tostring(metrics.position_seconds))
check("strife_metrics duration_seconds == 200 (not swapped with position)", metrics and tonumber(metrics.duration_seconds) == 200, metrics and tostring(metrics.duration_seconds))
A.playback[TEST_GUILD] = nil

-- error_events (report_error)
local ok14, err14 = pcall(A.report_error, TEST_GUILD, "smoke_test", "smoke test error", "this is a test error event, safe to ignore")
check("report_error / persist_error_event runs", ok14, err14)
local ee = A.db1("SELECT title, error_type FROM strife_error_events WHERE guild_id = %s AND error_type = 'smoke_test' ORDER BY created_at DESC LIMIT 1", TEST_GUILD)
check("error_events row persisted", ee and ee.title == "smoke test error", tostring(ee))

-- swarm_health (shared table, written unconditionally by heartbeat_tick every tick)
local sh = A.db1("SELECT status FROM swarm_health WHERE bot_name = 'strife'")
check("swarm_health row written by heartbeat_tick", sh and sh.status == "online", tostring(sh))

-- command registration sanity
check("59+ commands registered (57 base + 247 + panel)", A.command_count() >= 59, A.command_count())

-- cleanup: remove every row this test wrote, and ONLY rows scoped to
-- TEST_GUILD/TEST_USER*.
A.db("DELETE FROM strife_queue WHERE guild_id = %s AND bot_name = 'strife'", TEST_GUILD)
A.db("DELETE FROM strife_queue_backup WHERE guild_id = %s AND bot_name = 'strife'", TEST_GUILD)
A.db("DELETE FROM strife_playback_state WHERE guild_id = %s AND bot_name = 'strife'", TEST_GUILD)
A.db("DELETE FROM strife_guild_settings WHERE guild_id = %s", TEST_GUILD)
A.db("DELETE FROM strife_swarm_toggles WHERE guild_id = %s", TEST_GUILD)
A.db("DELETE FROM strife_track_intelligence WHERE guild_id = %s", TEST_GUILD)
A.db("DELETE FROM strife_user_track_affinity WHERE guild_id = %s", TEST_GUILD)
A.db("DELETE FROM strife_voice_state WHERE guild_id = %s AND bot_name = 'strife'", TEST_GUILD)
A.db("DELETE FROM strife_error_events WHERE guild_id = %s", TEST_GUILD)
A.db("DELETE FROM strife_metrics WHERE guild_id = %s AND bot_name = 'strife'", TEST_GUILD)

local leftover = A.db1([[
  SELECT
    (SELECT COUNT(*) FROM strife_queue WHERE guild_id = %s) +
    (SELECT COUNT(*) FROM strife_queue_backup WHERE guild_id = %s) +
    (SELECT COUNT(*) FROM strife_playback_state WHERE guild_id = %s) +
    (SELECT COUNT(*) FROM strife_guild_settings WHERE guild_id = %s) +
    (SELECT COUNT(*) FROM strife_track_intelligence WHERE guild_id = %s) +
    (SELECT COUNT(*) FROM strife_user_track_affinity WHERE guild_id = %s) +
    (SELECT COUNT(*) FROM strife_voice_state WHERE guild_id = %s) +
    (SELECT COUNT(*) FROM strife_error_events WHERE guild_id = %s) +
    (SELECT COUNT(*) FROM strife_metrics WHERE guild_id = %s) AS n
]], TEST_GUILD, TEST_GUILD, TEST_GUILD, TEST_GUILD, TEST_GUILD, TEST_GUILD, TEST_GUILD, TEST_GUILD, TEST_GUILD)
check("cleanup left zero test rows behind", leftover and tonumber(leftover.n) == 0, leftover and tostring(leftover.n))

print(string.format("\n%d passed, %d failed", pass, fail))
if fail > 0 then os.exit(1) end
