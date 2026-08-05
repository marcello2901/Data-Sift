# -*- coding: utf-8 -*-
"""
Isolamento entre laboratórios (multilocação) dentro de um processo Streamlit.

Multilocação no Streamlit tem uma característica que costuma passar
despercebida: **um único processo Python atende todos os usuários ao mesmo
tempo**. Não há isolamento por requisição como em Django ou FastAPI. Quem
compartilha o processo compartilha o interpretador, o heap e — o ponto crítico
— os caches.

O que já é isolado e o que não é:

- ``st.session_state`` **é** por sessão de navegador. Não vaza entre usuários.
  Mas persiste enquanto a aba viver, então precisa ser limpo no logout
  (:func:`wipe_session_data`), senão a próxima pessoa a usar aquela máquina
  encontra a planilha da anterior ainda carregada.

- ``@st.cache_data`` e ``@st.cache_resource`` **não são** por sessão. São
  globais do processo, indexados pelos argumentos da função.

Sobre o cache, sendo preciso quanto ao risco real no código de hoje: as
funções cacheadas do DataSift são indexadas por conteúdo do arquivo
(``carregar_planilha``) ou por caminho temporário aleatório
(``_read_csv_engine``), então **não** dá para demonstrar hoje uma leitura
cruzada entre laboratórios — colidir a chave exigiria já ter o arquivo. Os
problemas concretos são outros dois:

1. Dados clínicos de um laboratório permanecem residentes na memória
   compartilhada depois que a sessão dele acabou, sem prazo definido — o que
   é um problema de retenção sob LGPD, e amplia o que um vazamento de memória
   ou um *core dump* entregaria.

2. A propriedade "não vaza" depende hoje de um detalhe frágil: que os
   argumentos sejam únicos por acaso. No dia em que alguém cachear algo
   indexado por argumento comum — nome de coluna, faixa etária, configuração
   de filtro — dois laboratórios colidem a chave e um recebe o resultado
   calculado sobre os dados do outro. Silenciosamente, sem erro.

**A regra para decidir se uma função cacheada precisa da chave de tenant** é
olhar o que compõe a chave do cache:

- Indexada pelo **conteúdo** (o próprio DataFrame, os bytes do arquivo): é
  segura sem chave de tenant. Colidir a chave exige entradas idênticas, e
  entradas idênticas produzem a mesma saída — nenhuma informação atravessa.
  É o caso de ``run_harris_boyd(df, …)``, ``to_excel(df)``, ``to_csv(df)`` e
  ``carregar_planilha(conteudo, nome)``.

- Indexada por uma **referência** ao conteúdo (caminho de arquivo, id, nome):
  precisa da chave de tenant. A chave é um apelido, não o dado; dois
  laboratórios podem coincidir no apelido e divergir no conteúdo. Era o caso
  de ``_read_csv_engine(path, …)``, que passou a receber ``_tenant``.

:func:`tenant_cache_key` fecha esse segundo grupo: incluí-la como primeiro
argumento torna a separação estrutural em vez de acidental.
"""

from __future__ import annotations

from typing import Any, Iterable, Optional

from . import audit
from .models import ROLE_SUPERADMIN, User

# Chaves de ``st.session_state`` que carregam dados de planilha. São apagadas
# no logout, na troca de conta e quando a sessão expira.
_DATA_STATE_KEYS = (
    "dados_salvos",
    "id_arquivo_atual",
    "filtered_df",
    "filtered_result",
    "stratified_results",
    "analysis_params",
    "analysis_results",
    "filter_rules",
    "stratum_rules",
    "ref_limits_list",
    "confirm_stratify",
    "col_idade",
    "col_sexo",
    "col_dados",
    # Páginas de Repetições e Impacto
    "df_repeticoes",
    "df_original",
    "df_repeticao",
    "resultado_analise",
    "base_manual",
    "df_impacto",
    "resultado_impacto",
)


class TenantViolation(PermissionError):
    """Tentativa de acessar recurso de outro laboratório."""


def tenant_cache_key(user: Optional[User]) -> str:
    """
    Chave de isolamento para funções cacheadas.

    Passe como **primeiro argumento** de toda função decorada com
    ``@st.cache_data``::

        @st.cache_data(max_entries=4)
        def carregar_planilha(_tenant: str, conteudo: bytes, nome: str): ...

        df = carregar_planilha(tenant_cache_key(user), conteudo, nome)

    O valor não é usado no corpo da função — ele existe só para compor a
    chave, garantindo que a entrada de um laboratório jamais seja servida a
    outro, independentemente de os demais argumentos coincidirem.
    """
    if user is None:
        return "anon"
    return f"org:{user.org_id}"


def user_cache_key(user: Optional[User]) -> str:
    """
    Isolamento por usuário, um grau mais estrito.

    Use onde o resultado depende de escolhas individuais e não deveria ser
    reaproveitado nem por um colega do mesmo laboratório.
    """
    if user is None:
        return "anon"
    return f"user:{user.id}"


def scoped_key(user: Optional[User], key: str) -> str:
    """
    Prefixa uma chave de ``st.session_state`` com a identidade atual.

    Rede de proteção para o caso de uma aba ser reaproveitada por outra conta
    sem recarregar a página: o estado antigo simplesmente não é encontrado sob
    a chave nova.
    """
    return f"{user_cache_key(user)}::{key}"


def can_access_org(user: Optional[User], org_id: Optional[str]) -> bool:
    """Regra única de travessia entre laboratórios."""
    if user is None or not org_id:
        return False
    if user.role == ROLE_SUPERADMIN:
        return True
    return user.org_id == org_id


def assert_org_access(user: Optional[User], org_id: Optional[str], what: str = "recurso") -> None:
    """
    Impõe o isolamento e **registra a violação**.

    Uma tentativa de travessia raramente é acidente de digitação: é sinal de
    parâmetro adulterado ou de bug de autorização. Nos dois casos você quer
    saber que aconteceu, então isso vai para a auditoria antes de a exceção
    subir.
    """
    if can_access_org(user, org_id):
        return

    audit.record(
        audit.TENANT_VIOLATION,
        audit.OUTCOME_DENIED,
        actor_id=user.id if user else None,
        actor_email=user.email if user else None,
        org_id=user.org_id if user else None,
        target=what,
        detail={"org_solicitada": str(org_id)[:64]},
    )
    raise TenantViolation(
        "Você não tem acesso a este recurso."
    )


def visible_org_ids(user: Optional[User]) -> Optional[list[str]]:
    """
    Organizações que o usuário pode enxergar.

    ``None`` significa "todas" e só acontece para o superadmin — quem chama
    deve tratar esse caso explicitamente, nunca convertê-lo em lista vazia.
    """
    if user is None:
        return []
    if user.role == ROLE_SUPERADMIN:
        return None
    return [user.org_id]


def wipe_session_data(extra_keys: Iterable[str] = ()) -> None:
    """
    Remove do ``st.session_state`` tudo que carrega dado de planilha.

    Chamado no logout, na expiração e ao trocar de conta. Sem isso, o dado
    clínico do laboratório anterior continua na memória da sessão e reaparece
    na tela do próximo login feito naquela mesma aba.
    """
    try:
        import streamlit as st
    except Exception:
        return

    for key in list(_DATA_STATE_KEYS) + list(extra_keys):
        st.session_state.pop(key, None)

    # Chaves prefixadas por :func:`scoped_key`, de qualquer identidade.
    for key in [k for k in list(st.session_state.keys()) if "::" in str(k)]:
        st.session_state.pop(key, None)


def clear_caches() -> None:
    """
    Esvazia os caches globais do Streamlit.

    Operação do superadmin, não algo do logout: derruba o cache de *todos* os
    laboratórios ao mesmo tempo. Útil como resposta imediata a incidente —
    "tire agora o dado clínico da memória compartilhada" — ao custo de uma
    releitura para quem estiver trabalhando.
    """
    try:
        import streamlit as st

        st.cache_data.clear()
    except Exception:
        pass


def describe_isolation(user: Optional[User]) -> dict[str, Any]:
    """Resumo do escopo vigente, exibido no painel de administração."""
    if user is None:
        return {"escopo": "não autenticado"}
    if user.role == ROLE_SUPERADMIN:
        return {
            "escopo": "global",
            "laboratorios_visiveis": "todos",
            "chave_de_cache": tenant_cache_key(user),
        }
    return {
        "escopo": "laboratório",
        "laboratorios_visiveis": user.org_id,
        "chave_de_cache": tenant_cache_key(user),
    }
