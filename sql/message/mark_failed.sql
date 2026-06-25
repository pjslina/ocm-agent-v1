UPDATE ma_message
SET status = 'failed',
    ext = COALESCE(ext, '{}'::jsonb) || :err_json
WHERE message_id = :message_id;
