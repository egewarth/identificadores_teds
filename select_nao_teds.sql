select * from empenhos_teds 
where
  (
    metodo = 'ted ou nc invalido'
    OR metodo = 'vinculo nao encontrado'
  )
  -- indica que o favorecido da nota de empenho é o próprio emissor
  AND (left(ne_ccor,6) != ne_ccor_favorecido)
  AND NOT(
    (doc_observacao like '%TED%'
    AND
    doc_observacao not like '%METEDOLOGIAS%'
    )
    
    OR
    (ne_ccor_descricao like '%TED%'
    AND
    ne_ccor_descricao not like '%METEDOLOGIAS%'
    )
    
    OR
    fonte_recursos_detalhada_descricao like '%TED%'
    
    OR ne_ccor_descricao like '%DESCENTRALIZADA%'
    OR doc_observacao like '%DESCENTRALIZADA%'
    OR fonte_recursos_detalhada_descricao like '%DESCENTRALIZADA%'
  )

