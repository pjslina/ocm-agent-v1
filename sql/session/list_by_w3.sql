SELECT thread_id, biz_id, w3_account, title, status,
       created_at, updated_at, last_message_at, ext
FROM ma_session
WHERE w3_account = :w3_account
  AND (:biz_id IS NULL OR biz_id = :biz_id)
  AND (:before IS NULL OR last_message_at < :before)
ORDER BY last_message_at DESC NULLS LAST, created_at DESC
LIMIT :limit;
