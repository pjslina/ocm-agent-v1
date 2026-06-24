INSERT INTO ma_message (
    message_id, thread_id, seq, role, content,
    content_meta, status, route, request_id, ext
) VALUES (
    :message_id, :thread_id, :seq, :role, :content,
    :content_meta, :status, :route, :request_id, :ext
);
