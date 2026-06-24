UPDATE ma_message
SET status = 'partial'
WHERE message_id = :message_id;
