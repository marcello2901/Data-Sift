# -*- coding: utf-8 -*-
"""
Página de administração do DataSift.

O ``require_login`` abaixo é a barreira real desta página. No Streamlit, cada
arquivo em ``pages/`` é uma rota pública independente: quem digitar o endereço
desta página chega aqui sem passar pelo ``app.py``. Por isso a verificação de
permissão é a **primeira coisa** executada, antes de qualquer import pesado ou
consulta ao banco.

``force_revalidate=True`` obriga a consultar o estado da conta no banco a cada
carga, ignorando a janela de cache de 30 segundos. Numa tela que cria usuários
e muda perfis, o atraso aceitável para "esta conta ainda é administradora?" é
zero.
"""

import streamlit as st

st.set_page_config(page_title="DataSift — Administração", page_icon="favicon.png", layout="wide")

from security import admin_ui, ui  # noqa: E402
from security.guard import require_login  # noqa: E402
from security.models import PERM_USERS_MANAGE  # noqa: E402

user = require_login(
    permission=PERM_USERS_MANAGE,
    force_revalidate=True,
    page_name="Administração",
)

ui.render_account_sidebar(user)
ui.render_security_notices(user)

admin_ui.render(user)
