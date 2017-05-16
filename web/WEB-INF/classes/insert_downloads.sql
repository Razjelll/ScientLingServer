INSERT INTO Downloads(user_fk, set_fk)
VALUES (?, ?)
ON CONFLICT DO NOTHING