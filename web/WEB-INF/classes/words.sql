SELECT W.content,
	(SELECT string_agg(W1.content, '; ')
		FROM Translations T LEFT OUTER JOIN Words W1 ON T.translation_fk = W1.id
		WHERE T.item_fk = I.id) AS translations,
	(SELECT  D.content ||'::'||COALESCE(DT.content,'')
		FROM ItemDefinitions ID LEFT OUTER JOIN Definitions D ON ID.definition_fk = D.id
		LEFT OUTER JOIN Definitions DT ON ID.translation_fk = DT.id
		WHERE item_fk = I.id) AS definition,
	I.category_fk AS category,
	I.part_of_speech_fk AS part,
	I.difficulty ,
	(SELECT string_agg(S.content ||'::'|| COALESCE(S2.content,''),'; ')
		FROM ExemplarySentences ES LEFT OUTER JOIN Sentences S ON ES.sentence_fk = S.id
		LEFT OUTER JOIN Sentences S2 ON ES.translation_fk = S2.id
		WHERE ES.item_fk = I.id) AS sentences,
	(SELECT string_agg(H.content, ';')
		FROM ItemHints IH LEFT OUTER JOIN Hints H ON IH.hint_fk = H.id
		WHERE IH.item_fk = I.id) AS hints,
	I.image_name AS image,
	I.record_name AS record,
	L.id AS lesson
FROM Items I LEFT OUTER JOIN Words W ON I.word_fk = W.id
	LEFT OUTER JOIN Lessons L ON I.lesson_fk = L.id