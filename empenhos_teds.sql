with ncs_source as (
    select 
        *,
        regexp_substr(ne_ccor_descricao, '([0-9]+NC[0-9]+)|([0-9]*NC[0-9]+)') as in_text
    from siafi.empenhos_tesouro
),
not_ncs as (
    select
        *
    from ncs_source
    where in_text is null
),
ipea_nes as (
select
  *,
  replace(
      (
          regexp_match(
              ne_ccor_descricao,
              '(FERENCIA|NUMERO|Nº|TED|CRICAO|TRANSF.|CAO|TRANSFERENCIA )(\s|^|-|)([0-9]{6}|1\w{5}|[0-9]{3}\.[0-9]{3})(\s|$|\.|,|-|\/)'
          )
      )[3],
      '.',
      ''
  ) as num_transf
  from not_ncs
  where
    left(ne_ccor, 6) = '113601'
),
ipea_teds as (
select
  *
from ipea_nes
where num_transf is not null
)

select *
from ipea_teds
