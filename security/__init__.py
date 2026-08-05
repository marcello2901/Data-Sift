# -*- coding: utf-8 -*-
"""
Camada de segurança do DataSift.

Este pacote concentra tudo que decide *quem* pode fazer *o quê* e *sobre quais
dados*. O app de análise (``app.py`` e ``pages/``) não deve implementar
nenhuma regra de segurança por conta própria — deve apenas chamar
:func:`security.guard.require_login` no topo do script e usar o contexto
retornado.

Camadas (de baixo para cima):

- ``config``      — parâmetros e segredos, lidos de env/secrets. Nada hardcoded.
- ``crypto``      — hash de senha (Argon2id), tokens opacos, TOTP, cifra do segredo 2FA.
- ``db``          — conexão SQLite, esquema e migrações.
- ``repository``  — acesso a dados. Única camada que fala SQL de negócio.
- ``audit``       — trilha de auditoria encadeada (tamper-evident).
- ``ratelimit``   — limites de taxa e bloqueio progressivo.
- ``auth``        — login, sessões, política de senha, 2FA.
- ``tenancy``     — contexto de tenant e isolamento de cache/estado.
- ``sanitize``    — sanitização de entrada (SQL, HTML, nomes de arquivo).
- ``uploads``     — validação de arquivos enviados (tipo, tamanho, zip bomb).
- ``guard``       — API pública usada pelas páginas.
- ``ui``/``admin_ui`` — telas de login, conta e gerenciamento de usuários.
"""

__all__ = [
    "config",
    "crypto",
    "db",
    "repository",
    "audit",
    "ratelimit",
    "auth",
    "tenancy",
    "sanitize",
    "uploads",
    "guard",
]
