SELECT S.id, S.name, S.languagel1_fk AS l1, S.languagel2_fk AS l2
FROM Sets S LEFT OUTER JOIN Users U ON S.author_fk = U.id
WHERE U.login = ?