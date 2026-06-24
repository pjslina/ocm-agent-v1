SELECT message_id, thread_id, seq, role, content,
       content_meta, status, route, request_id, created_at, ext
FROM ma_message
WHERE thread_id = :thread_id
  AND status IN ('complete', 'partial')
ORDER BY seq DESC
LIMIT :limit;
