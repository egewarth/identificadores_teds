# XP_conference_26

# Hybrid-Regex-LLM: Budgetary Interoperability Agent

**A Design Science Research (DSR) project to restore data interoperability between Brazilian Government Systems (SIAFI & TransfereGov) using an Agile, LLM-in-the-loop approach.**

## Abstract

This projetct presents a method for analyzing and integrating data from federal government systems responsible for budgetary execution under credit decentralization agreements among federal entities of the Brazilian government. Due to the lack of interoperability among these systems, we proposed an agile, data-driven approach to extract identifiers from unstructured fields in commitment notes (notas de empenho). Initially, we mapped formatting patterns across 21 federal agencies to manually construct a reference base of regular expressions (regex). To ensure scalability, we implemented a continuous feedback system in which Large Language Models (LLMs) assist the system in expanding its regex base by automatically generating and validating new extraction rules for unknown patterns. As a result, we developed an analysis and identifier extraction strategy that enables interoperability between systems, even within unstructured fields, and facilitates the development of a Digital Government.

## Methodology

This study adopts the **Design Science Research (DSR)** paradigm. The artifact was developed following an **agile and incremental approach**, evolving through two distinct cycles:

1.  **Deterministic Cycle:** Manual mining of SQL data and crafting of static Regex patterns.
2.  **Hybrid Cycle (Self-Healing):** An automated pipeline where an LLM acts as a developer, creating and validating new Regex rules for unknown data patterns.


## How It Works

The system processes unstructured text fields (e.g., `ne_ccor_descricao`, `doc_observacao`) through the following workflow:

1.  **Pattern Application:** Applies a library of valid, pre-tested Regex patterns.
2.  **Gap Detection:** If no identifier is found but keywords (e.g., "TED", "PROC", "TRANSF") are present, the record is flagged as "Suspicious".
3.  **LLM Consultation:** The text is sent to a local LLM (e.g., GPT-OSS 120b) via API.
      * *Task 1:* Extract the semantic value (e.g., "TED 123/2024").
      * *Task 2 (Meta-Programming):* Write a generic Python Regex to capture this pattern.
4.  **Self-Validation:** The system executes the generated Regex against the original text.
      * *Success:* The Regex is added to the `KNOWN_REGEXES` knowledge base.
      * *Failure:* The error is sent back to the LLM for correction (up to 3 retries).
5.  **Optimization:** Irrelevant patterns (recurring noise) are mapped and ignored to save compute resources.

## Tech Stack

  * **Language:** Python 3.x
  * **Database:** PostgreSQL (SIAFI Data)
  * **AI/LLM:** Local LLM API (compatible with Ollama/OpenAI format)
  * **Libraries:** `psycopg2`, `requests`, `re`, `json`, `logging`

## Key Results

Comparing the manual approach vs. the Hybrid Agile approach on \~320k records:

| Metric | Manual Regex Approach | Hybrid LLM Approach |
| :--- | :--- | :--- |
| **Unique Regex Rules** | 11 (Static) | **320 (Learned)** |
| **Extracted Identifiers** | 52,577 | **111,563** |
| **Valid Systemic Links** | 22,846 | **36,898 (+61%)** |
| **"Orphan" TEDs Identified**| 1,647 | **4,831** |

The hybrid model significantly reduced false negatives and successfully mapped rare writing variations ("long tail") that manual engineering could not scale to address.

## Installation & Usage

### Prerequisites

  * Python 3.8+
  * PostgreSQL database containing the `siafi.empenhos_tesouro` table.
  * Access to an LLM API endpoint (local or remote).

### Setup

1.  **Clone the repository:**

    ```bash
    git clone https://github.com/egewarth/identificadores_teds.git
    cd identificadores_teds
    ```

2.  **Install dependencies:**

    ```bash
    pip install -r requirements.txt
    ```

3.  **Configure Database:**
    Update the `get_postgres_conn()` function in `main.py` with your credentials:

    ```python
    return "dbname=your_db user=your_user password=your_pass host=localhost port=5432"
    ```

4.  **Configure LLM:**
    Update the `processar()` call at the bottom of the script:

    ```python
    processar(
        llm_host="http://localhost:11434", # Your LLM API URL
        llm_model="gpt-oss:120b"           # Your Model Name
    )
    ```

5.  **Run the Agent:**

    ```bash
    python main.py
    ```

## Project Structure

  * `main.py`: Core logic containing the feedback loop, database connection, and LLM interaction.
  * `empenhos_teds.sql`: SQL query implementing multiple sequential extraction methods for TED identifiers from SIAFI database.
  * `select_nao_teds.sql`: SQL filter to identify false positives in TED classification.
  * `select_numero_inscricao_infor_complementar.sql`: SQL query to extract transfer identifiers from alternative field patterns.
  * `resultados_llm_sample.json`: Sample of LLM-generated results (50 records from 319,680 total records).
  * `result_analysis.ipynb`: Jupyter notebook with exploratory analysis and visualizations of extraction results.
  * `Cycle comparison tables.pdf`: Comparative analysis tables between regex and hybrid extraction cycles.

## Contribution

Contributions are welcome\! Please focus on improving the prompt engineering in `criar_prompt_linha` or optimizing the PostgreSQL queries.



