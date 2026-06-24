UPDATE ma_session
SET updated_at = CURRENT_TIMESTAMP,
    last_message_at = CURRENT_TIMESTAMP
WHERE thread_id = :thread_id;
