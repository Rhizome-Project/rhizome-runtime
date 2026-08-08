CREATE TABLE IF NOT EXISTS telegram_dm_map (
	workspace_id TEXT NOT NULL,
	message_id TEXT NOT NULL,
	from_agent_id TEXT NOT NULL,
	to_agent_id TEXT NOT NULL,
	telegram_chat_id INTEGER NOT NULL,
	telegram_message_id INTEGER NOT NULL,
	reply_message_id TEXT,
	sent_at TEXT NOT NULL,
	replied_at TEXT,
	PRIMARY KEY (telegram_chat_id, telegram_message_id)
);
