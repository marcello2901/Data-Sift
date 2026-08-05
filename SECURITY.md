# Segurança do DataSift

Documento de arquitetura e operação da camada de segurança (`security/`).
Escrito para quem administra o sistema — você — e para quem for mexer no
código depois.

---

## 1. Modelo de ameaça

O DataSift processa **dados laboratoriais de pacientes**. Mesmo com os termos
de uso exigindo anonimização, na prática chegam planilhas com código de
barras, idade, sexo e resultados — que, combinados, reidentificam pessoas. O
sistema é tratado como se manipulasse dado pessoal sensível sob a LGPD.

Contra quem o sistema se defende, em ordem de probabilidade:

| Adversário | Capacidade | Principal defesa |
|---|---|---|
| Alguém sem conta que descobre a URL | Acessa qualquer rota pública | `require_login()` no topo de **toda** página |
| Bot de força bruta | Milhares de tentativas de senha | Limite por conta **e** por IP, bloqueio exponencial |
| Usuário legítimo de outro laboratório | Sessão válida, tenta ver dados alheios | `org_id` obrigatório no repositório, sentinela `ACROSS_ALL_ORGS` |
| Usuário que monta a planilha enviada | Controla nomes de coluna e valores | Lista de permissão de identificadores, escape de SQL e HTML |
| Ex-funcionário com senha antiga | Credencial válida até ser revogada | Desativação derruba sessões; revalidação a cada 30 s |
| Quem obtém um dump do Postgres | Lê hashes, tokens, auditoria | Argon2id + pimenta fora do banco; token só em SHA-256; cadeia de auditoria |
| Administrador de laboratório mal-intencionado | Cria usuários no próprio tenant | Não pode atribuir papel acima do seu; tudo auditado |

**O que este sistema não resolve.** Não há proteção contra o operador do
Streamlit Community Cloud, que roda o processo. Não há criptografia de dados
em repouso além do que Supabase/Neon já fazem. Não há detecção de exfiltração
lenta — apenas o registro, em auditoria, de quem exportou o quê e quando.

---

## 2. Restrições que a plataforma impõe

Três características do Streamlit Community Cloud moldaram o desenho. Vale
conhecê-las antes de julgar as decisões:

**O disco é efêmero.** O container é recriado sem aviso e o disco volta ao
estado do repositório. Por isso o banco de identidade **precisa** ser externo
(`DATASIFT_DATABASE_URL`). Sem ele o sistema funciona com SQLite, mas você
perde usuários, sessões e auditoria no próximo redeploy — e o painel de
segurança avisa isso em vermelho.

**Não existe proxy reverso seu.** Não dá para configurar Nginx, WAF ou
Cloudflare na frente. Consequências diretas: o rate limiting **tem** de estar
no Python (está, em `security/ratelimit.py`), e não é possível definir
cabeçalhos de segurança (`Content-Security-Policy`, `X-Frame-Options`,
`Strict-Transport-Security`). O TLS é terminado pela infraestrutura do
Streamlit e não é configurável.

**Um processo atende todos os usuários.** Não há isolamento por requisição
como em Django ou FastAPI. `st.session_state` é por sessão de navegador, mas
os caches (`@st.cache_data`, `@st.cache_resource`) são globais do processo.
Isso é tratado em `security/tenancy.py` — ver a seção 5.

---

## 3. Autenticação

**Hash de senha: Argon2id** (`m=64 MiB, t=3, p=2`), o parâmetro recomendado
pelo OWASP. É *memory-hard*, que é o que encarece ataque com GPU. Se
`argon2-cffi` não estiver instalado, o sistema cai para PBKDF2-HMAC-SHA256 com
600.000 iterações e **avisa no painel**; o algoritmo fica gravado no próprio
hash e a senha migra sozinha para Argon2id no login seguinte, sem pedir nada
ao usuário.

**Pimenta (pepper).** Antes do hash, a senha passa por HMAC-SHA256 com
`DATASIFT_PEPPER`, um segredo que existe só na configuração do servidor. O sal
protege contra rainbow tables; a pimenta cobre o cenário em que alguém leva o
dump do Postgres mas não o processo — sem ela, o dump já basta para atacar as
senhas offline.

**Mensagem de erro única.** Senha errada, e-mail inexistente, conta desativada
e laboratório suspenso devolvem exatamente o mesmo texto, e o caminho do
usuário inexistente ainda gasta o tempo de uma verificação real de senha.
Diferenciar esses casos — por texto ou por tempo de resposta — entrega ao
atacante a lista de contas válidas, que é a parte cara de um ataque.

**Segundo fator (TOTP).** Opcional por usuário, compatível com Google
Authenticator, Authy, 1Password e Bitwarden. Implementado em stdlib, sem
dependência nova. O segredo é cifrado em repouso (Fernet com chave derivada da
pimenta via HKDF) e há **proteção a replay**: o contador usado fica registrado
e um código só vale uma vez. Sem isso, um código visto por cima do ombro
continua válido por até 90 segundos.

**Política de senha (NIST SP 800-63B).** Mínimo de 12 caracteres, senhas
comuns bloqueadas, senhas derivadas do nome/e-mail bloqueadas, sem reuso das
últimas 5. **Não** exigimos "maiúscula + número + símbolo" e a expiração
periódica vem desligada. Isso é deliberado: regras de composição e troca a
cada 90 dias produzem `Laboratorio2024!` virando `Laboratorio2025!` — pior que
uma frase longa e estável. `PASSWORD_MAX_AGE_DAYS` existe para quem precisa
atender a uma auditoria que ainda cobra rotação.

**Não existe "manter conectado".** Persistir o login entre recargas exigiria
cookie via componente de terceiros ou token na URL. Token em URL vaza por
histórico do navegador, cabeçalho `Referer` e log de servidor. A troca é
consciente: **recarregar a página exige novo login**. Se um usuário reclamar
disso, é o comportamento pretendido, não um defeito.

---

## 4. Sessões

| Propriedade | Valor | Configurável em |
|---|---|---|
| Expiração por inatividade | 30 min | `SESSION_IDLE_MINUTES` |
| Expiração absoluta | 8 h | `SESSION_ABSOLUTE_HOURS` |
| Revalidação contra o banco | 30 s | `SESSION_REVALIDATE_SECONDS` |

O token tem 256 bits e o banco guarda apenas seu SHA-256 — quem ler a tabela
`sessions` não consegue se passar por ninguém. Como o token já é aleatório e
de alta entropia, hash rápido é o correto: Argon2 só faz sentido contra
segredos de baixa entropia.

**Sobre a janela de 30 segundos.** O Streamlit reexecuta o script inteiro a
cada clique, o que significaria uma ida ao Postgres por interação. A sessão é
revalidada no máximo a cada 30 s, e sempre com `force_revalidate=True` nas
telas administrativas. A consequência honesta: **ao desativar um usuário, a
sessão dele morre em até 30 segundos, não instantaneamente.** Para expulsar
alguém na hora, use *Encerrar sessões* no painel — a revogação é verificada
sem atraso.

Sessões são revogadas automaticamente em: logout, troca de senha, mudança de
papel, desativação da conta, suspensão do laboratório, troca de laboratório, e
quando o usuário clica em *Encerrar todas as sessões*.

---

## 5. Multilocação

O tenant é o **laboratório** (`organizations`). Usuários do mesmo laboratório
compartilham contexto; laboratórios diferentes nunca se enxergam. O superadmin
atravessa todos.

**Onde o isolamento é imposto.** No repositório (`security/repository.py`),
não na interface. Toda consulta que devolve mais de um usuário exige escopo
explícito: passar `None` levanta `TenantScopeError` em vez de virar
silenciosamente "todos". Atravessar organizações exige escrever
`ACROSS_ALL_ORGS` de propósito. Uma tentativa de travessia indevida é
**registrada na auditoria** antes de a exceção subir — travessia raramente é
erro de digitação, é sinal de parâmetro adulterado ou bug de autorização.

**Cache do Streamlit.** Este é o ponto específico da plataforma. `@st.cache_data`
é global do processo, indexado pelos argumentos da função. A regra para decidir
se uma função cacheada precisa da chave de tenant:

- Indexada pelo **conteúdo** (o próprio DataFrame, os bytes do arquivo): segura
  sem chave. Colidir exige entradas idênticas, que produzem saída idêntica —
  nenhuma informação atravessa. É o caso de `run_harris_boyd(df, …)`,
  `to_excel(df)`, `to_csv(df)`, `carregar_planilha(conteudo, nome)`.
- Indexada por uma **referência** ao conteúdo (caminho, id, nome): **precisa**
  da chave. A chave é um apelido, não o dado. Era o caso de
  `_read_csv_engine(path, …)`, que agora recebe `tenant_cache_key(user)`.

Ao adicionar qualquer `@st.cache_data` novo, aplique essa regra.

**Papéis e permissões** (`security/models.py`). O código pergunta por
*capacidade* (`user.has_permission(PERM_DATA_EXPORT)`), nunca por cargo
(`if role == "admin"`). Papel desconhecido recebe conjunto vazio — nega por
padrão.

| Papel | Analisa | Envia | Exporta | Gerencia usuários | Gerencia laboratórios | Auditoria |
|---|---|---|---|---|---|---|
| `superadmin` | ✅ | ✅ | ✅ | ✅ todos | ✅ | global |
| `org_admin` | ✅ | ✅ | ✅ | ✅ do seu lab | ❌ | do seu lab |
| `analyst` | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| `viewer` | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ |

Um `org_admin` não consegue criar um `superadmin` nem promover alguém acima de
si — é o que impede escalada de privilégio a partir de uma conta
administrativa comprometida.

---

## 6. Vulnerabilidades corrigidas no código existente

As quatro primeiras foram confirmadas com exploração funcional antes da
correção, não deduzidas por leitura.

### 6.1 Injeção de SQL via nome de coluna (crítica)

O `DataProcessor` montava SQL com `f'"{col}"'`, sem escapar aspas internas. Os
nomes de coluna vêm do **cabeçalho da planilha enviada** — texto arbitrário
escolhido por quem monta o arquivo, não estrutura confiável.

Uma coluna chamada:

```
Idade" || (SELECT CAST(COUNT(*) AS VARCHAR) FROM local_df) || "Segredo
```

produzia um identificador que fechava as aspas e emendava uma subconsulta. Na
verificação, a subconsulta **executou** e devolveu dados de outra coluna
concatenados ao resultado. Como o DuckDB roda em processo e tem acesso a
disco, isso é execução de consulta arbitrária no servidor.

*Correção:* `sanitize.safe_column_ref()` confere o nome contra as colunas reais
do DataFrame (lista de permissão) e só então cita com escape correto
(`"` → `""`). Coluna que não existe vira `FALSE`, não um identificador.

### 6.2 Operador arbitrário na cláusula WHERE (alta)

`OPERATOR_MAP.get(op, op)` devolvia intacto qualquer operador que não
conhecesse. Um operador `> 0 OR 1=1 OR 0 >` transformava o filtro
`Idade > 999` em condição sempre verdadeira — na verificação, devolveu 5 de 5
linhas em vez de 0.

*Correção:* `sanitize.normalize_operator()` trabalha com lista fechada;
qualquer coisa fora dela vira `FALSE`. Idem para o conector lógico
(`AND`/`OR`/`BETWEEN`).

### 6.3 XSS armazenado via valor da planilha (alta)

O valor da coluna Sexo era interpolado em `st.markdown(..., unsafe_allow_html=True)`
sem escape. Um valor como `<img src=x onerror=...>` executa no navegador de
quem abre a análise — inclusive no seu, ao revisar os dados de um cliente.

*Correção:* `sanitize.escape_html()` no ponto de renderização;
`escape_attr()` em `make_help_icon`, que injeta em atributo HTML.

### 6.4 Bomba de descompressão (alta)

`load_dataframe` chamava `z.read(nome)` sem verificar o tamanho descompactado.
Um ZIP de dezenas de KB pode expandir para GB e derrubar o processo — que no
Streamlit Cloud é **compartilhado por todos os laboratórios**. Não é travar a
própria sessão: é negação de serviço geral.

*Correção:* `uploads.validate_upload()` inspeciona o índice central **sem
descompactar** — tamanho declarado, total acumulado, razão de compressão,
número de entradas, caminhos com `../`, ZIP aninhado.

### 6.5 Vazamento de informação em mensagem de erro (média)

`st.session_state.filter_error = f"SQL Processing Error: {e}"` entregava a
consulta montada, nomes de coluna e caminhos internos — material de
reconhecimento para quem sonda o sistema.

*Correção:* `sanitize.redact_error()` devolve mensagem genérica com um código
de correlação; o detalhe real vai para a auditoria sob o mesmo código.

### 6.6 Arquivo temporário deixado em disco (média)

`NamedTemporaryFile(delete=False)` com `os.remove` no caminho feliz. Uma
exceção no meio da leitura pulava o `remove`, deixando a planilha clínica no
disco do servidor indefinidamente.

*Correção:* `uploads.secure_tempfile()`, com remoção em `finally` e permissão
`0600`. O sufixo do arquivo, que vinha do nome enviado pelo usuário, passou a
ser higienizado.

### 6.7 Tipo de arquivo confiado pela extensão (média)

Só a extensão era verificada. *Correção:* checagem de assinatura (*magic
bytes*) para planilhas e para os anexos JPG/PNG/PDF da página de Impacto —
estes últimos importam porque os bytes vão inteiros para dentro do PDF
assinado que o laboratório trata como comprovante.

---

## 7. Limites de taxa

Todos com bloqueio exponencial (5 → 10 → 20 → 40 min, teto de 12 h) e estado
no banco, para sobreviver ao restart do container. Se o banco cair, há espelho
em memória do processo: perde-se o compartilhamento entre réplicas, mas o
atacante continua limitado. Falhar *aberto* anularia justamente a proteção que
mais importa durante um incidente.

| Operação | Limite | Janela |
|---|---|---|
| Login por conta | 5 | 15 min |
| Login por IP | 25 | 15 min |
| Upload | 30 | 10 min |
| Processamento (filtro, estratificação, Harris-Boyd) | 60 | 5 min |
| Exportação | 40 | 10 min |
| Troca de senha | 5 | 1 h |
| Ação administrativa | 100 | 5 min |

O login é contado nas **duas** dimensões: por conta (barra força bruta contra
um alvo) e por IP (barra varredura de senha comum contra muitas contas — o
ataque que a contagem por conta não enxerga).

O limite de exportação é o mais relevante contra exfiltração: uma conta
comprometida baixando a base em lote esbarra nele e aparece na auditoria.

---

## 8. Auditoria

Cada registro carrega o hash do anterior, formando uma cadeia. Quem apagar ou
editar uma linha no meio quebra o encadeamento de todas as seguintes, e o
botão *Verificar integridade* aponta exatamente onde. Isso importa porque quem
administra o Postgres tem acesso de escrita à própria auditoria — sem
encadeamento, um log é apenas uma sugestão.

**Regra inegociável: nada de dado de paciente no log.** Registra-se *quem fez
o quê*, nunca *sobre qual resultado clínico*. `audit._scrub()` é a última
barreira: descarta chaves de aparência sensível mesmo que o chamador as envie
por engano, trunca strings longas e remove caracteres de controle (que
permitiriam forjar linhas falsas para quem lê o log como texto).

Eventos registrados incluem: login (sucesso, falha, bloqueio, conta travada),
logout, expiração de sessão, troca e redefinição de senha, ativação e remoção
de 2FA, criação/alteração/desativação/exclusão de usuário, criação e suspensão
de laboratório, acesso negado, **violação de tenant**, upload, upload
recusado, exportação e estouro de limite de taxa.

Auditoria nunca derruba a operação auditada: se o banco estiver fora do ar, o
evento é perdido e o app continua. Negar login porque o log falhou
transformaria um problema de observabilidade em indisponibilidade total.

Retenção padrão: 730 dias (`AUDIT_RETENTION_DAYS`). Guardar log para sempre é
passivo de privacidade, não zelo — a LGPD pede prazo definido.

---

## 9. Instalação

### 9.1 Banco (Supabase, plano gratuito)

1. Crie um projeto em https://supabase.com.
2. *Project Settings → Database → Connection string → **Transaction***.
3. Copie a string da **porta 6543** (o pooler), não a da 5432.

O app abre uma conexão por operação — padrão que sobrevive melhor ao modelo de
threads do Streamlit e aos restarts do Cloud. O pooler é o que torna isso
barato.

### 9.2 Segredos no Streamlit Cloud

Em *Settings → Secrets*, cole:

```toml
[datasift]
DATABASE_URL = "postgresql://postgres.SEUPROJETO:SENHA@aws-0-sa-east-1.pooler.supabase.com:6543/postgres"
PEPPER = "<saída de: python -c 'import secrets; print(secrets.token_urlsafe(48))'>"
BOOTSTRAP_ADMIN_EMAIL = "marcello613@gmail.com"
```

Os *Secrets* do Streamlit Cloud persistem mesmo quando o container é recriado
— é por isso que a recuperação automática funciona.

### 9.3 Primeiro acesso

1. Abra o app. O esquema do banco é criado sozinho.
2. Não havendo superadmin, o sistema cria um e mostra a senha **uma única vez**
   na tela de login. Copie antes de recarregar.
3. Entre e troque a senha (será exigido).
4. Ative o 2FA em *Conta → Verificação em duas etapas*.
5. Em *Administração → Laboratórios*, crie os laboratórios.
6. Em *Administração → Usuários*, crie as contas. Cada uma nasce com senha
   provisória exibida uma vez, com troca obrigatória no primeiro acesso.

### 9.4 Desenvolvimento local

Sem `DATABASE_URL`, o sistema usa SQLite em `.datasift/datasift.db` (ignorado
pelo Git). Defina ao menos `DATASIFT_BOOTSTRAP_ADMIN_EMAIL`:

```bash
export DATASIFT_BOOTSTRAP_ADMIN_EMAIL="voce@exemplo.com"
export DATASIFT_PEPPER="qualquer-coisa-longa-para-testes"
streamlit run app.py
```

---

## 10. Operação

**Novo usuário.** *Administração → Usuários → Criar*. Entregue a senha
provisória por um canal diferente do e-mail que serve de login. Ela aparece
uma única vez.

**Alguém saiu da equipe.** *Desativar acesso* — derruba as sessões na hora e
preserva a rastreabilidade do que a pessoa fez. Prefira sempre desativar a
excluir. A exclusão só faz sentido em pedido formal de eliminação de dados
sob LGPD; os registros de auditoria são preservados de qualquer forma.

**Suspeita de conta comprometida.** Na ordem: *Encerrar sessões* (efeito
imediato) → *Redefinir senha* → conferir *Auditoria* filtrando por
`login.success` e `data.exported` para essa conta.

**Conta travada por tentativas.** O bloqueio expira sozinho. Para liberar
antes, use *Desbloquear conta*, que também zera o limite de taxa.

**Usuário perdeu o celular do 2FA.** *Remover 2FA* no painel. A ação fica na
auditoria — é também o caminho que um admin comprometido usaria para derrubar
o segundo fator de alguém.

**Resposta a incidente.** *Segurança → Esvaziar cache de dados* remove os
dados clínicos da memória compartilhada do processo imediatamente. Custa uma
releitura para quem estiver trabalhando.

**Verificação periódica sugerida.** Mensalmente: *Auditoria → Verificar
integridade*; revisar `login.failure` e `tenant.violation`; conferir se todo
admin tem 2FA ativo; rodar *Limpar sessões expiradas* e *Aplicar retenção da
auditoria*.

---

## 11. Ao mexer no código

**Página nova em `pages/`.** Comece com `require_login()` **antes** de
qualquer leitura de arquivo ou consulta. No Streamlit, cada arquivo em
`pages/` é uma rota pública independente: sem essa chamada, a página é
acessível sem login por mais protegido que o resto do app esteja. Esconder do
menu não protege nada — a rota continua funcionando.

**SQL novo para o DuckDB.** Identificador só entra via
`sanitize.safe_column_ref(nome, df.columns)`. Operador só via
`sanitize.normalize_operator()`. Valor de texto via `sanitize.quote_literal()`,
número via `sanitize.safe_number()`.

**`unsafe_allow_html` com qualquer valor que não seja literal.** Passe por
`sanitize.escape_html()`, ou `escape_attr()` se for dentro de atributo.

**`@st.cache_data` novo.** Aplique a regra da seção 5.

**Consulta nova no repositório.** Escopo de organização explícito. Se a função
puder atravessar tenants, exija `ACROSS_ALL_ORGS` do chamador.

**Nunca** registre em auditoria: senha, token, segredo TOTP ou qualquer valor
vindo de célula de planilha.

---

## 12. Verificação

A camada foi validada com 80 verificações automatizadas cobrindo: hash e
pimenta, TOTP com replay, isolamento entre laboratórios, matriz de permissões,
política de senha, revogação de sessão, limites de taxa, cadeia de auditoria
(incluindo detecção de adulteração direta no banco) e validação de upload
(incluindo bomba de descompressão real). As injeções de SQL foram testadas
contra o DuckDB de verdade, com planilhas hostis, antes e depois da correção.

Para reexecutar, os scripts estão descritos no histórico do commit desta
camada.
