WITH userDownload AS (
	SELECT set_fk,user_fk,
	1 AS download,
		rating
	FROM Downloads
	WHERE user_fk = ?
)
SELECT S.id, S.name, L1.name AS l1, L2.name AS l2, U.login AS author, S.description, S.words_count AS num_words,
	S.size,
	(SELECT COUNT(user_fk) FROM Downloads WHERE set_fk = S.id) AS download_count,
	S.added_date,
	CASE WHEN US.download=1 THEN 'true' ELSE 'false' END AS user_download,
	US.rating AS user_rating,
	(SELECT AVG(rating) FROM Downloads WHERE set_fk = S.id) AS rating
FROM Sets S LEFT OUTER JOIN Languages L1 ON S.languagel1_fk = L1.id
	LEFT OUTER JOIN Languages L2 ON S.languagel2_fk = L2.id
	LEFT OUTER JOIN Users U ON S.author_fk = U.id
	LEFT OUTER JOIN userDownload US ON US.set_fk = S.id
	WHERE S.id = ?
	