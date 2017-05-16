WITH val(content) AS
( VALUES (?) ),
ins AS (INSERT INTO Words(content) 
		SELECT content FROM val
		ON CONFLICT (content) DO NOTHING
		RETURNING id, content),
sel AS (SELECT id, W.content FROM Words W JOIN val ON W.content =val.content )
SELECT COALESCE(ins.id, sel.id)
FROM val LEFT JOIN ins ON ins.content = val.content
LEFT JOIN  sel ON sel.content = val.content