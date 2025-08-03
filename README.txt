PROJETO PRONTO: EXTRAIR DADOS (INEP/IBGE) -> POSTGRES 9.3 -> ORGANIZAR COM AIRFLOW E DBT
========================================================================================

O QUE VOCÊ VAI TER AQUI
-----------------------
1) Um arquivo "docker-compose.yml" que sobe dois serviços:
   - "airflow": para agendar/rodar as tarefas.
   - "dbt-cli": utilitário para rodar dbt manualmente se quiser.
2) Uma DAG do Airflow que executa:
   - Seu script Python (coleta da API e gravação no Postgres).
   - dbt (organiza os dados em camadas finais - GOLD - como VISÃO).
3) Um projeto dbt simples (sem jargões) para criar VISÕES (views) organizadas.

ANTES DE COMEÇAR
----------------
1) Instale o Docker Desktop no Windows (padrão Next > Next). 
2) Baixe esta pasta, descompacte em um local simples (ex.: C:\projeto_inep).
3) Abra o arquivo ".env.example", copie e salve como ".env".
4) No ".env", preencha os dados do seu Postgres 9.3 (host, porta, banco, usuário, senha).
   - Se o servidor Postgres 9.3 for a SUA máquina, use POSTGRES_HOST=host.docker.internal.
   - Teste sua senha e banco antes.
5) (Opcional) Se quiser mudar a senha do Airflow, ajuste no docker-compose em "users create".

SUBIR OS SERVIÇOS
-----------------
1) No Windows, abra o "Prompt de Comando" ou "PowerShell".
2) Vá até a pasta do projeto. Ex.: 
   cd C:\projeto_inep
3) Rode:
   docker compose up -d
4) Aguarde ~1 a 3 minutos. 
5) Abra no navegador: http://localhost:8080
   Usuário: admin   Senha: admin

RODAR O FLUXO
-------------
1) No Airflow (tela web), ative a DAG "inep_full_pipeline".
2) Clique em "Play" (botão ►) para rodar agora.
3) A DAG faz:
   - "extract_safe": roda o script Python e grava no seu Postgres 9.3
     * BRONZE: tabela public.inep_api_raw (JSON)
     * SILVER: tabelas "achatadas" por endpoint (tudo texto)
   - "dbt_run": cria VISÕES na camada GOLD (ex.: gold_dim_escola, gold_fato_ideb)
   - "dbt_test": roda testes básicos (se houverem)

ONDE VER RESULTADOS
-------------------
- No seu Postgres 9.3:
  * BRONZE: public.inep_api_raw
  * SILVER: public.api_ideb_escolas, public.api_ideb_resumo_uf, public.api_ideb_escola_id
  * GOLD (via dbt): schema "gold" (visões): gold.gold_dim_escola, gold.gold_fato_ideb
- Para ver as visões GOLD: conecte seu cliente SQL favorito e rode: 
  SELECT * FROM gold.gold_dim_escola LIMIT 10;

PARAR TUDO
----------
- No PowerShell/Prompt:
  docker compose down

AJUSTES QUE VOCÊ PODE QUERER FAZER
----------------------------------
- Trocar de modo SAFE para FULL (vai baixar BEM mais dados). Na tela do Airflow,
  edite a tarefa "extract_safe" (ou troque no arquivo DAG) para usar "--mode FULL".
- Ajustar a pausa entre chamadas à API: parâmetro "--sleep 0.3" (segundos) no DAG.
- Se quiser GOLD mais elaborado (chaves, deduplicação), edite os arquivos do dbt
  em "dbt/models/gold". Aqui deixei bem simples para compatibilidade com Postgres 9.3.
- Se o dbt der erro por conta da versão antiga do Postgres, mantenha o GOLD como VISÃO
  simples (já está assim) ou use um Postgres mais novo SÓ para as visões GOLD.

DÚVIDAS COMUNS
--------------
- "O Airflow abriu mas não vejo a DAG": espere 1-2 minutos; depois recarregue a página.
- "Erro ao conectar no Postgres": confira o arquivo .env (host, porta, banco, usuário, senha).
- "Script muito lento": aumente "--sleep" e rode "SAFE" (menos dados).

ATENÇÃO IMPORTANTE
------------------
- O Postgres 9.3 é MUITO antigo. Este projeto foi feito para funcionar nele, por isso
  os dados ficam como JSON (sem truques avançados) e as VISÕES GOLD são simples.