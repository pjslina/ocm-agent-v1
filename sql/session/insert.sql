INSERT INTO ma_session (
    thread_id, biz_id, w3_account, title, status, ext
) VALUES (
    :thread_id, :biz_id, :w3_account, :title, 'active', :ext
)
ON CONFLICT (thread_id) DO NOTHING;
