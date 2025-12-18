import os
import json
import logging
import psycopg2
import requests
import re

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

# Lista global para armazenar os padrões aprendidos
# Estrutura: {"tipo": "TED", "pattern": r"..."}
KNOWN_REGEXES = []

PADROES_IRRELEVANTES = [
    "TED SEM VINCULO COM TED",
    "(SEM VINC/TED)",
    "PROCESSO MOROSO",
    "IMPORTANCIA EMPENHADA PARA ATENDER",

]


REGEX_PRE_PRONTOS = [
    # --- PADRÕES SIMPLES ---
    {"tipo": "NUMERO_TRANSF", "pattern": r"N[°º∫]\s*\d+"},
    {"tipo": "TED", "pattern": r"TED\s*([0-9]{3,6})"},
    {"tipo": "NUMERO_TRANSF", "pattern": r"TRANSFERENCI[AA]\s*([0-9]{6})"},
    {"tipo": "NOTA_CREDITO", "pattern": r"(20[0-9]{2}(?:NC|ND)[0-9]{6})"},
    {"tipo": "TED", "pattern": r"TED\s*([0-9]{2}-[0-9]{4})"},

    # --- PADRÃO SOLICITADO ANTERIORMENTE ---
    {"tipo": "Processo_SEI", "pattern": r"PROC\s+\d+"},

    # --- VARIAÇÕES DE PROCESSO SEI (Convertidas da lista antiga) ---
    {"tipo": "Processo_SEI", "pattern": r"PROC\s+SEI\s+\d{5}\.\d{6}\.\d{4}\.\d{2}"},
    {"tipo": "Processo_SEI", "pattern": r"PROCESSO[:\s]*([0-9]{5}\.[0-9]{6}/[0-9]{4}-[0-9]{2})"},
    {"tipo": "Processo_SEI", "pattern": r"PROCESSO\s*SEI\s*(?:N[°ºo∞]\s*)?([0-9]{5}\.[0-9]{6}/[0-9]{4}-[0-9]{2})"},
    {"tipo": "Processo_SEI", "pattern": r"PROC\.?\s*SEI[:\s]*([0-9]{5}\.[0-9]{6}/[0-9]{4}-[0-9]{2})"},
    {"tipo": "Processo_SEI", "pattern": r"PROCESSO\s*SEI[:\s]*([0-9]{5}\.[0-9]{6}/[0-9]{4}-[0-9]{2})"},
    {"tipo": "Processo_SEI", "pattern": r"PROC\.\(S\)\s*NUM\.\(S\)\s*:\s*[0-9]{5}\.[0-9]{6}/[0-9]{4}-[0-9]{2}"},
    {"tipo": "Processo_SEI", "pattern": r"PROC\.?\s*SEI\s*[0-9]{5}\.[0-9]{6}\.[0-9]{4}\.[0-9]{2}"},
    {"tipo": "Processo_SEI", "pattern": r"PROC\.?\s*SEI\s*(?:N[°ºo]?\s*)?[0-9]{6,7}/[0-9]{4}-[0-9]{2}"},
    {"tipo": "Processo_SEI", "pattern": r"PROCESSO\s*SEI\.{0,2}\s*[0-9]{4,6}/[0-9]{4}-[0-9]{2}"},
    {"tipo": "Processo_SEI", "pattern": r"(?:PROCESSO\s*SEI\s*[:]?\s*|PROC\.?\s*)[0-9]{5}\.[0-9]{6}/[0-9]{4}-[0-9]{2}"},
    {"tipo": "Processo_SEI", "pattern": r"PROC(?:ESSO)?\s*SEI\s*[:]?\s*(?:N[°ºo∞]\s*)?\d{3,6}/\d{4}-\d{2}"},
    {"tipo": "Processo_SEI", "pattern": r"(?i)(?:PROCESSO\s*SEI|PROC\.?)(?:\s*[:#]?\s*)[0-9]{5}\.[0-9]{6}\.[0-9]{4}\.[0-9]{2}"},
    {"tipo": "Processo_SEI", "pattern": r"SEI\s*[0-9]{5}\.[0-9]{6}/[0-9]{4}-[0-9]{2}"},
    {"tipo": "Processo_SEI", "pattern": r"PROC(?:ESSO)?\s*SEI\s*[:]?\s*[0-9]+(?:\s*\.\s*[0-9]+)*"},
    {"tipo": "Processo_SEI", "pattern": r"SEI\s*[0-9]{5}\.[0-9]{7}\.[0-9]{4}\.[0-9]{2}"},
    {"tipo": "Processo_SEI", "pattern": r"SEI\s*[0-9]{5}\.[0-9]{6}\.[0-9]{4}\.[0-9]{2}"},
    {"tipo": "Processo_SEI", "pattern": r"SEI\s*[0-9]{5}\.[0-9]{6}\.[0-9]{4}"},
    {"tipo": "Processo_SEI", "pattern": r"(?i)(PROC\.?\s*SEI\s*\d{1,5}/\d{4}-\d{2})"},

    # --- VARIAÇÕES DE TED (Convertidas da lista antiga) ---
    {"tipo": "TED", "pattern": r"TED\s*[A-Za-z0-9]+(?:[\/\-][A-Za-z0-9]+)*"},
    {"tipo": "TED", "pattern": r"TED\s+(?:[A-Z]+(?:[-/][A-Z]+)*\s+)*N[∫]\s*\d{2}/\d{4}"},
    {"tipo": "TED", "pattern": r"TED\s*(?:[A-Z0-9\.]+\s*)?\d{2}[./-]\d{4}"},
    {"tipo": "TED", "pattern": r"TED\s*\.?\s*[0-9]{1,6}(?:/[0-9]{4})?"},

    {"tipo": "Processo_SEI", "pattern": r"PROC\s*SEI\s*\.?\s*[0-9]{5}\.[0-9]{6}\.[0-9]{4}\.[0-9]{2}"},
    {"tipo": "Processo_SEI", "pattern": r"PROCESSO\s+SEI\s+(?:N[°º§]?\s*)?\d{5}\.\d{6}/\d{4}-\d{2}"},


    {"tipo": "Processo_SEI", "pattern": r"SEI\s*\d+/\d{4}-\d{1,2}"},
    {"tipo": "Processo_SEI", "pattern": r"PROC\.?\s*[0-9]{5}\.[0-9]{8}\.[0-9]{4}\.[0-9]{2}"},
    {"tipo": "Processo_SEI", "pattern": r"PROC\.?\s*SEI\s*[0-9]{5}/[0-9]{2}-[0-9]{2}"},
    {"tipo": "Processo_SEI", "pattern": r"PROCESSO\s+SEI\s+N[∫º°\u00B0\u221E]?\s*\d{5}\.\d{6}/\d{4}-\d{2}"},
    {"tipo": "Processo_SEI", "pattern": r"(?:S\s*EI|PROCESSO\s*SEI)\s*[:]?\s*\d{5}\.\d{6}/\d{4}-\d{2}"},
    {"tipo": "Processo_SEI", "pattern": r"(?:PROC(?:\.|ETO)?\s*(?:SEI)?[:\s]*)[0-9]{5}\.[0-9]{6,7}[\/\s]?[0-9]{4}[-\.][0-9]{2}"},
    {"tipo": "Processo_SEI", "pattern": r"PROC\.?\s*[0-9]+/[0-9]{4}-[0-9]+"},
    {"tipo": "Processo_SEI", "pattern": r"PROC\.\s*SEI\.\s*\d{5}\.\d{6}/\d{4}-\d{2}"},
    {"tipo": "Processo_SEI", "pattern": r"PROC\.?\s*SEI\s*[:]?\s*[0-9]{5}\.[0-9]{4}\.[0-9]{4}\.[0-9]{2}"},
    {"tipo": "Processo_SEI", "pattern": r"PROC\.?\s+SEI\s+N\u221E\s*\d{5}/\d{4}-\d{2}"},
    {"tipo": "Processo_SEI", "pattern": r"PROC\.?\s*SEI\s*(?:N[°º§]?\s*)?\d{5}/\d{4}-\d{2}"},
    {"tipo": "Processo_SEI", "pattern": r"(?:PROC\.|PROCESSO\s*SEI:?)\s*[0-9]{5}\.[0-9]{6}\.[0-9]{4}-[0-9]{2}"},
    {"tipo": "Processo_SEI", "pattern": r"(PROCESSO\s*(?:SEI\s*)?N[°ºo∞]\s*\d{5}\.\d{6}/\d{4}-\d{2})"},
    {"tipo": "Processo_SEI", "pattern": r"PROCESSO\s*(?:SEI)?\s*(?:N[°º]?|N§)?\s*\d{5}\.\d{6}/\d{4}-\d{2}"},
    {"tipo": "Processo_SEI", "pattern": r"PROCESSO\s*[0-9]{6}\.[0-9]{4}\.[0-9]"},
    {"tipo": "Processo_SEI", "pattern": r"PROC\.?\s*SEI\s*[ø:]?\s*[0-9]{5}\.[0-9]{6}/[0-9]{4}[ø-]?[0-9]{2}"},
    {"tipo": "Processo_SEI", "pattern": r"PROC\.?\s*SEI\s*[:,]?\s*[0-9]{5}\.[0-9]{6}/[0-9]{4}-[0-9]{2}"},

    {"tipo": "NUMERO_TRANSF", "pattern": r"NUM\.\(S\):\s*\d+\.\d+"},
    {"tipo": "NUMERO_TRANSF", "pattern": r"N\u222B\s*SIAFI\s*\d{1,9}"},

    {"tipo": "TED", "pattern": r"\bTED\s*[-–]\s*\d{1,6}\b"},
    {"tipo": "TED", "pattern": r"TED\s*(?:N[∫]?\s*)?[0-9]{6,}/[0-9]{4}"},
    {"tipo": "TED", "pattern": r"TED(?:\s+[A-Z]+)*(?:\s*-\s*[A-Z]+)*(?:\s*-\s*\d{3,6})"},
    {"tipo": "TED", "pattern": r"TED\s+(?:SIAFI\s+)?N[∫]?\s*\d+"},
    
    {"tipo": "NOTA_CREDITO", "pattern": r"([0-9]{4}(?:NC|ND)[0-9]{1,6})"}
]









def contem_padrao_irrelevante(texto):
    """Verifica se o texto contém algum padrão irrelevante."""
    if not texto:
        return False
    texto_upper = texto.upper()
    for padrao in PADROES_IRRELEVANTES:
        if padrao in texto_upper:
            return True
    return False

def get_postgres_conn():
    # Substitua pelos seus dados locais
    return "dbname=postgres user=postgres password=postgres host=127.0.0.1 port=5433"

def buscar_notas(schema="siafi", tabela="empenhos_tesouro", limit=None):
    """Busca notas de empenho do PostgreSQL com unique_key completo."""
    conn = psycopg2.connect(get_postgres_conn())
    sql = f"""
        SELECT 
            ne_ccor,
            natureza_despesa,
            doc_observacao,
            ne_ccor_ano_emissao,
            emissao_dia,
            emissao_mes,
            despesas_empenhadas,
            despesas_liquidadas,
            despesas_pagas,
            ne_ccor_descricao,
            fonte_recursos_detalhada_descricao
        FROM {schema}.{tabela}
        WHERE ne_ccor_descricao IS NOT NULL 
           OR doc_observacao IS NOT NULL
           OR fonte_recursos_detalhada_descricao IS NOT NULL
        {f'LIMIT {limit}' if limit else ''}
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        dados = [{
            "unique_key": {
                "ne_ccor": r[0],
                "natureza_despesa": r[1],
                "doc_observacao": r[2],
                "ne_ccor_ano_emissao": r[3],
                "emissao_dia": r[4],
                "emissao_mes": r[5],
                "despesas_empenhadas": r[6],
                "despesas_liquidadas": r[7],
                "despesas_pagas": r[8],
            },
            "colunas_analisadas": {
            "ne_ccor_descricao": r[9],
            "doc_observacao": r[2],
            "fonte_recursos_detalhada_descricao": r[10],
            }
        } for r in cur.fetchall()]
    
    log.info(f"Carregados {len(dados)} registros")
    return dados


def testar_regexes(texto, regexes_list, tipos_ignorados=None):
    """
    Testa uma lista de regexes (dicionários com 'tipo' e 'pattern') contra o texto.
    Retorna uma lista de dicionários com {tipo, valor, regex_usado} se encontrar.
    """
    resultados = []
    if not texto:
        return resultados

    tipos_encontrados = set()
    if tipos_ignorados:
        tipos_encontrados.update(tipos_ignorados)

    for item in regexes_list:
        pattern = item['pattern']
        tipo = item['tipo']
        
        # Se já encontramos um valor válido (com número) para este tipo, pula
        if tipo in tipos_encontrados:
            continue

        try:
            # Procura todas as ocorrências
            matches = re.findall(pattern, texto)
            for match in matches:
                # match pode ser uma tupla se houver grupos, preferimos a string completa ou o primeiro grupo
                val = match if isinstance(match, str) else match[0]
                
                # Se o valor extraído contém número, marca o tipo como encontrado e adiciona
                if re.search(r'\d', str(val)):
                    resultados.append({
                        "tipo": tipo,
                        "valor_extraido": val,
                        "regex_usado": pattern
                    })
                    tipos_encontrados.add(tipo)
                    # Garante apenas um resultado por tipo (o primeiro encontrado)
                    break

        except re.error:
            log.warning(f"Regex inválido ignorado: {pattern}")
            continue
            
    return resultados


def criar_prompt_linha(texto, tipos_encontrados=None, tipos_suspeitos=None):
    """Cria prompt para análise de uma única linha e geração de regex."""
    
    contexto_especifico = ""
    if tipos_encontrados:
        contexto_especifico += f"\n                - Tipos JÁ encontrados (IGNORE estes): {', '.join(tipos_encontrados)}"
    if tipos_suspeitos:
        contexto_especifico += f"\n                - Tipos com INDÍCIOS (FOQUE nestes): {', '.join(tipos_suspeitos)}"

    return {
        "messages": [{
            "role": "user",
            "content": (
                f"""
                Você é um assistente especializado em finanças públicas brasileiras.
                Analise o texto abaixo proveniente de uma Nota de Empenho ou SIAFI.
                Sua tarefa é identificar se existe algum identificador financeiro ou administrativo relevante.{contexto_especifico}
               
                Texto para análise: "{texto}"

                Tipos de interesse:
                - "TED" (Termo de Execução Descentralizada, ex: "TED 05-2023", "TED 965836/2024")
                - "NUMERO_TRANSF" (código de transferência SIAFI/TransfereGov, ex: "TRANSFERENCIA 950660", "TRANSF. 123456", N∫: 00005/2024)
                - "NOTA_CREDITO" (identificador NC/ND, ex: "2024NC800017", "2023ND000123")
                - "Processo_SEI" (número de processo administrativo, ex: "PROCESSO SEI: 01300.004254/2024-61", "PROC. SEI N° 23007.011362/2024-93", "PROC  20244023182")

                Se encontrar algo relevante:
                1. Extraia o valor exato.
                2. Classifique o tipo.
                3. Crie uma Expressão Regular (Regex) em Python capaz de extrair esse identificador. 
                   IMPORTANTE: O Regex deve ser genérico o suficiente para capturar variações do mesmo padrão (ex: capturar qualquer TED similar), mas específico o suficiente para não pegar texto comum.
                   Exemplo: Para "TED 123/2024", use algo como "TED\\s*([0-9]{2}-[0-9]{4})" e não apenas o texto fixo.
                4. Sempre faça o regex que pegue o nome + número, não apenas o número isolado. Então se está escrito "CIENTIFICO E TECNOLOGICO (CNPQ) , CONFORME PROCESSO SEI:01300.004254/2024-61, PREGAO ELETRONICO...", o regex deve capturar "PROCESSO SEI:01300.004254/2024-61" e não apenas "01300.004254/2024-61".
                5. O valor extraído SEMPRE tem que ser nome + número, nunca apenas o número, nem apenas o nome (palavra solta)

                Retorne APENAS um JSON válido no seguinte formato (sem blocos de código markdown):
                {{
                    "encontrou": true,
                    "itens": [
                        {{
                            "tipo": "TED",
                            "valor_extraido": "TED 123/2024",
                            "regex_sugerido": "TED\\\\s*([0-9]{2}-[0-9]{4})"
                        }},
                        {{
                            "tipo": "Processo_SEI",
                            "valor_extraido": "PROCESSO SEI:01300.004254/2024-61",
                            "regex_sugerido": "PROCESSO\\\\s*SEI\\\\s*(?:N[°ºo∞]\\\\s*)?([0-9]{5}\\\\.[0-9]{6}/[0-9]{4}-[0-9]{2})"
                        }}
                    ]
                }}

                Se NÃO encontrar nada relevante, retorne:
                {{
                    "encontrou": false,
                    "itens": []
                }}
                """
            )
        }]
    }


def criar_prompt_fix_regex(texto, regex_falho, tipo, valor_esperado, valor_encontrado=None):
    """Cria prompt para corrigir um regex que falhou."""
    msg_erro = f"Regex que falhou: '{regex_falho}'"
    if valor_encontrado is not None:
        msg_erro += f"\n                - O regex extraiu: '{valor_encontrado}' (mas esperávamos '{valor_esperado}')"
    else:
        msg_erro += f"\n                - O regex NÃO extraiu nada."

    return {
        "messages": [{
            "role": "user",
            "content": (
                f"""
                O Regex sugerido anteriormente FALHOU na validação.
                
                Contexto:
                - Texto original: "{texto}"
                - Tipo de dado: {tipo}
                - Valor que deve ser extraído EXATAMENTE: "{valor_esperado}"
                {msg_erro}
                
                Tarefa:
                Forneça um NOVO Regex Python corrigido que consiga extrair EXATAMENTE o valor esperado.
                
                Retorne APENAS um JSON no formato:
                {{
                    "regex_corrigido": "seu_novo_regex_aqui"
                }}
                """
            )
        }]
    }


def enviar_llm(prompt_json, host, model):
    """Envia prompt para LLM local e processa a resposta."""
    import re
    headers = {"Content-Type": "application/json"}

    body = {
        "model": model, 
        "messages": prompt_json["messages"], 
        "temperature": 0,
        "format": "json",
        "stream": False
    }
    
    try:
        resp = requests.post(f"{host}/api/chat", headers=headers, json=body, timeout=180)
        if resp.status_code == 404:
            body.pop("format", None)
            body["response_format"] = {"type": "json_object"}
            resp = requests.post(f"{host}/v1/chat/completions", headers=headers, json=body, timeout=180)
            
        resp.raise_for_status()
        
        resp_json = resp.json()
        if "message" in resp_json:
            content = resp_json["message"]["content"]
        elif "choices" in resp_json:
            content = resp_json["choices"][0]["message"]["content"]
        else:
            return None

        if "```" in content:
            content = re.sub(r"```json|```", "", content).strip()

        parsed = json.loads(content)
        return parsed

    except Exception as e:
        log.error(f"Erro no LLM: {e}")
        return None


def processar(llm_host, llm_model, limit=None):
    """Processa notas linha a linha, aprendendo regexes."""
    dados = buscar_notas(limit=limit)
    resultados = []
    
    campos_analise = ["ne_ccor_descricao", "doc_observacao", "fonte_recursos_detalhada_descricao"]

    # Inicializa o arquivo JSON vazio (ou com lista vazia)
    with open("resultados_llm.json", "w", encoding="utf-8") as f:
        f.write('{\n"resultados": [\n')

    primeiro_registro = True
    
    # Dicionário para rastrear candidatos a padrões irrelevantes
    # Estrutura: "TRECHO_TEXTO": {"total": 0, "vazios": 0}
    candidatos_irrelevantes = {}

    for i, item in enumerate(dados):
        log.info(f"Processando item {i+1}/{len(dados)}")
        
        item_resultados = {
            "sequencial" : i+1,
            "unique_key": item["unique_key"],
            "colunas_analisadas": item["colunas_analisadas"],
            "identificadores_encontrados": []
        }
        
        for campo in campos_analise:
            texto = item["colunas_analisadas"].get(campo)
            if not texto:
                continue
            
            # Verifica se contém padrões irrelevantes antes de processar
            if contem_padrao_irrelevante(texto):
                continue

            # Identifica se há algum número no texto
            tem_numero = bool(re.search(r'\d', texto))
            if not tem_numero:
                continue
                
            # 1. Tenta extrair com regexes pré-definidos
            matches = testar_regexes(texto, REGEX_PRE_PRONTOS)
            
            # Coleta tipos já encontrados para não repetir na busca de aprendidos
            tipos_ja_encontrados = {m['tipo'] for m in matches}

            # 2. Tenta extrair com regexes aprendidos, ignorando tipos já encontrados
            matches_aprendidos = testar_regexes(texto, KNOWN_REGEXES, tipos_ignorados=tipos_ja_encontrados)
            matches.extend(matches_aprendidos)
            
            # 3. Verifica se existem informações faltantes dos 4 tipos principais
            # Normaliza os tipos encontrados
            tipos_encontrados_norm = set()
            for m in matches:
                t_lower = m['tipo'].lower()
                if "ted" in t_lower: 
                    tipos_encontrados_norm.add("TED")
                elif "processo" in t_lower: # Abrangendo "Processo" e "Processo SEI"
                    tipos_encontrados_norm.add("Processo_SEI")
                elif "nota_credito" in t_lower or "nc" in t_lower or "nd" in t_lower: 
                    tipos_encontrados_norm.add("NOTA_CREDITO")
                elif "transf" in t_lower: # Abrangendo "Numero de transferencia" e "NUMERO_TRANSF"
                    tipos_encontrados_norm.add("NUMERO_TRANSF")

            # Definição dos indicadores para tipos NÃO encontrados
            tipos_suspeitos = []
            
            # Padrão flexível para encontrar variações de "TED S/V"
            # Explicação: Procura TED, seguido opcionalmente por espaço/barra, depois S, opcionalmente barra/ponto, depois V
            padrao_ignorar = r"TED[:\s\./-]*S[\s\./-]*V"

            if "TED" not in tipos_encontrados_norm and \
            re.search(r"\bTED\b", texto, re.IGNORECASE) and \
            not re.search(padrao_ignorar, texto, re.IGNORECASE) and \
            not re.search(r"S[\s./]*V", texto, re.IGNORECASE):
                
                tipos_suspeitos.append("TED")

            #if "TED" not in tipos_encontrados_norm and re.search(r"\bTED\b", texto, re.IGNORECASE) and not re.search(r"\bTED TED S/V\b", texto, re.IGNORECASE):
            
            if "Processo_SEI" not in tipos_encontrados_norm and re.search(r"(?<![A-Za-z])PROC(?:ESSO)?(?:\.|\b)", texto, re.IGNORECASE):
                tipos_suspeitos.append("Processo_SEI")
                
            if "NOTA_CREDITO" not in tipos_encontrados_norm and re.search(r"\d+NC\d+", texto, re.IGNORECASE):
                tipos_suspeitos.append("NOTA_CREDITO")
                
            if "NUMERO_TRANSF" not in tipos_encontrados_norm and re.search(r"(N∫|N[º°]|N[o]\.|transf)", texto, re.IGNORECASE):
                tipos_suspeitos.append("NUMERO_TRANSF")

            tem_indicio_faltante = len(tipos_suspeitos) > 0
            
            # Se encontrou indícios de algo que falta OU (não encontrou nada E tem números), chama a LLM
            precisa_llm = tem_indicio_faltante #or (not matches and re.search(r'\d', texto))

            if precisa_llm:
                log.info(f"Indícios de informação não capturada no campo '{campo}'. Suspeitas: {tipos_suspeitos}. Consultando LLM...")
                log.info(f"Texto: {texto}")
                # log.info(texto)
                resposta_llm = enviar_llm(criar_prompt_linha(texto, list(tipos_encontrados_norm), tipos_suspeitos), llm_host, llm_model)
                  
                if resposta_llm and resposta_llm.get("encontrou"):
                    novos_itens = resposta_llm.get("itens", [])
                    for novo in novos_itens:
                        regex = novo.get("regex_sugerido")
                        tipo = novo.get("tipo")
                        valor_esperado = novo.get("valor_extraido")
                        
                        # Verifica se o tipo já foi encontrado (evita duplicidade via LLM)
                        t_novo_lower = tipo.lower()
                        ja_tem = False
                        if "ted" in t_novo_lower and "TED" in tipos_encontrados_norm: ja_tem = True
                        elif "processo" in t_novo_lower and "Processo_SEI" in tipos_encontrados_norm: ja_tem = True
                        elif ("nota_credito" in t_novo_lower or "nc" in t_novo_lower) and "NOTA_CREDITO" in tipos_encontrados_norm: ja_tem = True
                        elif "transf" in t_novo_lower and "NUMERO_TRANSF" in tipos_encontrados_norm: ja_tem = True
                        
                        if ja_tem:
                            log.info(f"Tipo '{tipo}' sugerido pela LLM já existe nos resultados. Ignorando.")
                            continue

                        # Loop de tentativas (max 3) para acertar o regex
                        for tentativa in range(3):
                            if not regex:
                                break
                                
                            sucesso_regex = False
                            valor_encontrado_regex = None
                            try:
                                # Testa a extração com o novo regex
                                encontrados = re.findall(regex, texto)
                                
                                # Flatten para pegar strings
                                encontrados_flat = [val if isinstance(val, str) else val[0] for val in encontrados]
                                
                                # Validação estrita: O valor esperado deve estar entre os encontrados
                                if valor_esperado in encontrados_flat:
                                    sucesso_regex = True
                                    valor_encontrado_regex = valor_esperado
                                    
                                    # Verifica duplicidade antes de adicionar
                                    if not any(k['pattern'] == regex for k in KNOWN_REGEXES):
                                        KNOWN_REGEXES.append({"tipo": tipo, "pattern": regex})
                                        log.info(f"Novo regex aprendido e validado: {regex} ({tipo})")
                                    
                                    # Adiciona o resultado validado
                                    matches.append({
                                        "tipo": tipo,
                                        "valor_extraido": valor_esperado,
                                        "regex_usado": regex
                                    })
                                    break # Sucesso, sai do loop de retry
                                else:
                                    valor_encontrado_regex = encontrados_flat if encontrados_flat else None
                                    log.warning(f"Tentativa {tentativa+1}/3 falhou: Regex extraiu {valor_encontrado_regex}, mas esperava '{valor_esperado}'.")

                            except re.error as e:
                                log.error(f"Tentativa {tentativa+1}/3 falhou: Regex inválido '{regex}' ({e})")
                            
                            # Se chegou aqui, falhou. Tenta corrigir se não for a última tentativa.
                            if tentativa < 2:
                                log.info("Pedindo correção do regex para a LLM...")
                                resp_fix = enviar_llm(criar_prompt_fix_regex(texto, regex, tipo, valor_esperado, valor_encontrado_regex), llm_host, llm_model)
                                if resp_fix and "regex_corrigido" in resp_fix:
                                    regex = resp_fix["regex_corrigido"]
                                    log.info(f"LLM sugeriu correção: {regex}")
                                else:
                                    log.warning("LLM não retornou correção válida.")
                                    break

            # Adiciona matches (seja de regex antigo ou novo) ao resultado do item
            if matches:
                for m in matches:
                    m["origem_coluna"] = campo
                    item_resultados["identificadores_encontrados"].append(m)
            
            # --- LÓGICA DE APRENDIZADO DE PADRÕES IRRELEVANTES ---
            # Se o texto for longo o suficiente, extrai um trecho fixo (char 10 ao 60)
            if texto and len(texto) > 50:
                trecho = texto[10:60].upper()
                
                if trecho not in candidatos_irrelevantes:
                    candidatos_irrelevantes[trecho] = {"total": 0, "vazios": 0}
                
                candidatos_irrelevantes[trecho]["total"] += 1
                
                # Se não encontrou nenhum identificador neste campo, conta como "vazio"
                if not matches:
                    candidatos_irrelevantes[trecho]["vazios"] += 1

        resultados.append(item_resultados)

        # A cada 100 registros, avalia padrões irrelevantes
        if (i + 1) % 100 == 0:
            for padrao, stats in candidatos_irrelevantes.items():
                # Se repetiu pelo menos 10 vezes e em TODAS as vezes não encontrou nada
                if stats["total"] >= 10 and stats["total"] == stats["vazios"]:
                    if padrao not in PADROES_IRRELEVANTES:
                        PADROES_IRRELEVANTES.append(padrao)
                        log.info(f"Novo padrão irrelevante aprendido: '{padrao}' (repetiu {stats['total']} vezes sem sucesso)")
            
            # Limpa o dicionário para o próximo lote de 100
            candidatos_irrelevantes = {}

        # A cada 20.000 registros (ou no último), escreve no disco e limpa memória
        if (i + 1) % 20000 == 0 or (i + 1) == len(dados):
            log.info(f"Salvando lote parcial no disco... ({len(resultados)} registros)")
            with open("resultados_llm.json", "a", encoding="utf-8") as f:
                for res in resultados:
                    if not primeiro_registro:
                        f.write(",\n")
                    json.dump(res, f, indent=2, ensure_ascii=False)
                    primeiro_registro = False
            
            # Limpa a lista da memória
            resultados = []

    # Fecha o array JSON no final
    with open("resultados_llm.json", "a", encoding="utf-8") as f:
        f.write("\n],\n")
        
        regexes_agrupados = {}
        for item in KNOWN_REGEXES:
            t = item['tipo']
            p = item['pattern']
            if t not in regexes_agrupados:
                regexes_agrupados[t] = []
            if p not in regexes_agrupados[t]:
                regexes_agrupados[t].append(p)
                
        f.write('"regexes_aprendidos": ')
        json.dump(regexes_agrupados, f, indent=2, ensure_ascii=False)
        f.write("\n}")

    return [] # Retorna vazio pois já salvou tudo


if __name__ == "__main__":
    # Exemplo de regex inicial (opcional, pode começar vazio)
    # KNOWN_REGEXES.append({"tipo": "TED", "pattern": r"TED\s*\d+/\d{4}"})

    processar(
        llm_host="http://10.0.0.10:11434",
        llm_model="gpt-oss:120b",
       # limit=20000
    )
    
    log.info(f"Processamento concluído. Regexes aprendidos: {len(KNOWN_REGEXES)}")


    # ne_info_complementar
