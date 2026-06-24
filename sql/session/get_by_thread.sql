SELECT thread_id, biz_id, w3_account, title, status,
       created_at, updated_at, last_message_at, ext
FROM ma_session
WHERE thread_id = :thread_id;
