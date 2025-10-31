with
empenhos_sem_vinculo_ted as(
  select
    *,
    right(ne_ccor, 12) as ne,
    left(ne_ccor,6) as orgao_id,
    null as nc,
    null as num_transf,
    'sem vinculo' as metodo
  from siafi.empenhos_tesouro
  where
    ne_ccor_descricao ~* '\bTED[[:space:]:/().-]*(S/?[VN]|S/?VINCULO)'
    or ne_ccor_descricao ~* 'SEM[[:space:]]+VINC[[:space:]]*(ULO|/TED)'

),
empenhos_filtrados as(
  select
    *
  from siafi.empenhos_tesouro
  where 
    ne_ccor_descricao !~* '\bTED[[:space:]:/().-]*(S/?[VN]|S/?VINCULO)'
    and ne_ccor_descricao !~* 'SEM[[:space:]]+VINC[[:space:]]*(ULO|/TED)'

),
empenhos_orgaos_metodo_1 as (
  select
      *,
      -- Uma série de extrações que servirão de identificadores 
      right(ne_ccor, 12) as ne,
      left(ne_ccor,6) as orgao_id,
      regexp_substr(ne_ccor_descricao, '([0-9]{4}NC[0-9]+)') as nc,
      replace(
        (regexp_match(
          ne_ccor_descricao,
          '(?:TED|(?:NUM[[:space:].-]*)?(?:TRANSFERENCIA|TRANSF(?:\.|\:)|TRANF(?:\.|\:))|N[ºo]|NUMERO|CRICAO)[[:space:].:-]*([0-9]{6}|1[A-Z0-9]{5}|[0-9]{3}\.[0-9]{3})',
          'i'
        ))[1],
          '.',
          ''
      ) as num_transf,
      'metodo 1' as metodo
  from empenhos_filtrados
),

empenhos_restantes_metodo_1 as(
select * from empenhos_orgaos_metodo_1 where num_transf is null AND nc is null
),

empenhos_orgaos_metodo_2 as (
  select
    -- seleciona todas as colunas do órgãos 1, exceto nc e num_transf
      emissao_mes,emissao_dia,ne_ccor,ne_num_processo,ne_info_complementar,ne_ccor_descricao,doc_observacao,natureza_despesa,natureza_despesa_descricao,ne_ccor_favorecido,ne_ccor_favorecido_descricao,ne_ccor_ano_emissao,ptres,fonte_recursos_detalhada,fonte_recursos_detalhada_descricao,despesas_empenhadas,despesas_liquidadas,despesas_pagas,restos_a_pagar_inscritos,restos_a_pagar_pagos,dt_ingest,ne,orgao_id,nc,
      replace(
          (regexp_match(
            ne_ccor_descricao,
            '.*(?:NT|NOTA[[:space:]]+DE[[:space:]]+TRANSFERENCIA)[:.[:space:]]*((?=[A-Za-z0-9]*[0-9])[A-Za-z0-9]{6})',
            'i'
          ))[1],
          '.',
          ''
      ) as num_transf,
      'metodo 2' as metodo
  from empenhos_restantes_metodo_1 p
),

empenhos_restantes_metodo_2 as(
select * from empenhos_orgaos_metodo_2 where num_transf is null AND nc is null
),

empenhos_orgaos_metodo_3 as (
  select
    -- seleciona todas as colunas do órgãos 1, exceto nc e num_transf
      emissao_mes,emissao_dia,ne_ccor,ne_num_processo,ne_info_complementar,ne_ccor_descricao,doc_observacao,natureza_despesa,natureza_despesa_descricao,ne_ccor_favorecido,ne_ccor_favorecido_descricao,ne_ccor_ano_emissao,ptres,fonte_recursos_detalhada,fonte_recursos_detalhada_descricao,despesas_empenhadas,despesas_liquidadas,despesas_pagas,restos_a_pagar_inscritos,restos_a_pagar_pagos,dt_ingest,ne,orgao_id,nc,
      replace(
          (regexp_match(
            ne_ccor_descricao,
            '.*(?:(?:TED(?:[[:space:]]*[-.N∞øº°∅()]*))[[:space:]]*|(?:SIAFI[[:space:]]+N∫))[[:space:].-]*((?=[A-Za-z0-9]*[0-9])[A-Za-z0-9]{6})',
            'i'
          ))[1],
          '.',
          ''
      ) as num_transf,
      'metodo 3' as metodo
  from empenhos_restantes_metodo_2 p
),

empenhos_restantes_metodo_3 as(
select
emissao_mes,emissao_dia,ne_ccor,ne_num_processo,ne_info_complementar,ne_ccor_descricao,doc_observacao,natureza_despesa,natureza_despesa_descricao,ne_ccor_favorecido,ne_ccor_favorecido_descricao,ne_ccor_ano_emissao,ptres,fonte_recursos_detalhada,fonte_recursos_detalhada_descricao,despesas_empenhadas,despesas_liquidadas,despesas_pagas,restos_a_pagar_inscritos,restos_a_pagar_pagos,dt_ingest,ne,orgao_id,nc,num_transf,
'vinculo nao encontrado' as metodo
from empenhos_orgaos_metodo_3 where num_transf is null AND nc is null
),

empenhos_teds as(
select * from empenhos_sem_vinculo_ted
UNION ALL
select * from empenhos_orgaos_metodo_1 where num_transf is not null OR nc is not null
UNION ALL
select * from empenhos_orgaos_metodo_2 where num_transf is not null OR nc is not null
UNION ALL
select * from empenhos_orgaos_metodo_3 where num_transf is not null OR nc is not null
UNION ALL
select * from empenhos_restantes_metodo_3
)

select
  *
from empenhos_teds
where
metodo = 'vinculo nao encontrado'
AND
((doc_observacao like '%TED%'
AND
doc_observacao not like '%METEDOLOGIAS%')
OR
(ne_ccor_descricao like '%TED%'
AND
ne_ccor_descricao not like '%METEDOLOGIAS%')
)
