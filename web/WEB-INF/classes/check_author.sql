SELECT COUNT(1)
FROM Sets
WHERE id=? AND author_fk = (
  SELECT id FROM Users WHERE login=?
)