SELECT S.id, S.name,S.words_count as words_count,size, description,author_fk AS author , COUNT(D.user_fk) AS downloads
          , L1.name as l1, L2.name as l2, catalog,
          (SELECT AVG(rating) FROM Downloads WHERE set_fk = S.id) AS rating
            FROM Sets S JOIN Lessons L ON L.set_fk = S.id
            JOIN Items I ON I.lesson_fk = L.id
            LEFT OUTER JOIN Languages L1 ON S.languagel1_fk = L1.id
            LEFT OUTER JOIN Languages L2 ON S.languagel2_fk = L2.id
            LEFT OUTER JOIN Downloads D ON S.id = D.set_fk
            ?
GROUP BY S.id, S.name,size, description, author_fk , L1.name, L2.name