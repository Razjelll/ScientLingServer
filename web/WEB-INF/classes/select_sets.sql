SELECT S.id, S.name,COUNT(1) as words_count,size, description,author_fk AS author ,rating, download_count AS downloads
          ,images_size, records_size, L1.name as l1, L2.name as l2
            FROM Sets S JOIN Lessons L ON L.set_fk = S.id
            JOIN Items I ON I.lesson_fk = L.id
            LEFT OUTER JOIN Languages L1 ON S.languagel1_fk = L1.id
            LEFT OUTER JOIN Languages L2 ON S.languagel2_fk = L2.id
            ?
GROUP BY S.id, S.name,size, description,author_fk ,rating, download_count,  images_size, records_size, L1.name, L2.name