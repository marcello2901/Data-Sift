# -*- coding: utf-8 -*-
"""
Papéis, permissões e entidades.

O controle de acesso é baseado em papel (RBAC) com permissões **explícitas**.
Nada de checar ``if role == "admin"`` espalhado pelo código: o app pergunta
por *capacidade* (``pode exportar?``), não por *cargo*. Assim, criar um papel
novo depois é editar uma tabela aqui, e não caçar comparações de string por
todo o projeto — que é exatamente como se abre um furo de autorização.

Hierarquia de multilocação::

    superadmin  → você. Atravessa organizações. Único que gerencia laboratórios.
    org_admin   → gerencia usuários do próprio laboratório. Não enxerga os outros.
    analyst     → usa as ferramentas de análise, envia e exporta dados.
    viewer      → analisa e vê resultados na tela, mas não envia nem exporta.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

# --- Papéis ---------------------------------------------------------------

ROLE_SUPERADMIN = "superadmin"
ROLE_ORG_ADMIN = "org_admin"
ROLE_ANALYST = "analyst"
ROLE_VIEWER = "viewer"

ALL_ROLES = (ROLE_SUPERADMIN, ROLE_ORG_ADMIN, ROLE_ANALYST, ROLE_VIEWER)

ROLE_LABELS = {
    ROLE_SUPERADMIN: "Superadministrador (global)",
    ROLE_ORG_ADMIN: "Administrador do laboratório",
    ROLE_ANALYST: "Analista",
    ROLE_VIEWER: "Somente leitura",
}

ROLE_DESCRIPTIONS = {
    ROLE_SUPERADMIN: "Acesso total, inclusive a todos os laboratórios e à auditoria global.",
    ROLE_ORG_ADMIN: "Cria e gerencia usuários do próprio laboratório. Não vê outros laboratórios.",
    ROLE_ANALYST: "Usa as ferramentas, envia planilhas e exporta resultados.",
    ROLE_VIEWER: "Executa análises e vê resultados, mas não pode enviar nem exportar arquivos.",
}

# --- Permissões -----------------------------------------------------------

PERM_DATA_ANALYZE = "data.analyze"
PERM_DATA_UPLOAD = "data.upload"
PERM_DATA_EXPORT = "data.export"
PERM_USERS_MANAGE = "users.manage"
PERM_ORGS_MANAGE = "orgs.manage"
PERM_AUDIT_VIEW_ORG = "audit.view.org"
PERM_AUDIT_VIEW_ALL = "audit.view.all"
PERM_SECURITY_SETTINGS = "security.settings"

_ROLE_PERMISSIONS: dict[str, frozenset[str]] = {
    ROLE_SUPERADMIN: frozenset({
        PERM_DATA_ANALYZE, PERM_DATA_UPLOAD, PERM_DATA_EXPORT,
        PERM_USERS_MANAGE, PERM_ORGS_MANAGE,
        PERM_AUDIT_VIEW_ORG, PERM_AUDIT_VIEW_ALL, PERM_SECURITY_SETTINGS,
    }),
    ROLE_ORG_ADMIN: frozenset({
        PERM_DATA_ANALYZE, PERM_DATA_UPLOAD, PERM_DATA_EXPORT,
        PERM_USERS_MANAGE, PERM_AUDIT_VIEW_ORG,
    }),
    ROLE_ANALYST: frozenset({
        PERM_DATA_ANALYZE, PERM_DATA_UPLOAD, PERM_DATA_EXPORT,
    }),
    ROLE_VIEWER: frozenset({
        PERM_DATA_ANALYZE,
    }),
}

# Papéis que um usuário pode atribuir a outro. Um org_admin não consegue
# fabricar um superadmin nem promover alguém acima de si — é o que impede
# escalada de privilégio a partir de uma conta administrativa comprometida.
_ASSIGNABLE_ROLES: dict[str, tuple[str, ...]] = {
    ROLE_SUPERADMIN: ALL_ROLES,
    ROLE_ORG_ADMIN: (ROLE_ORG_ADMIN, ROLE_ANALYST, ROLE_VIEWER),
    ROLE_ANALYST: (),
    ROLE_VIEWER: (),
}

STATUS_ACTIVE = "active"
STATUS_DISABLED = "disabled"


def permissions_for(role: str) -> frozenset[str]:
    """Papel desconhecido devolve conjunto vazio: nega por padrão."""
    return _ROLE_PERMISSIONS.get(role, frozenset())


def assignable_roles(role: str) -> tuple[str, ...]:
    return _ASSIGNABLE_ROLES.get(role, ())


@dataclass(frozen=True)
class Organization:
    id: str
    name: str
    slug: str
    status: str
    created_at: str
    created_by: Optional[str] = None
    notes: Optional[str] = None

    @property
    def is_active(self) -> bool:
        return self.status == STATUS_ACTIVE


@dataclass(frozen=True)
class User:
    id: str
    org_id: str
    email: str
    display_name: str
    role: str
    status: str
    must_change_password: bool
    totp_enabled: bool
    created_at: str
    last_login_at: Optional[str] = None
    locked_until: Optional[str] = None
    failed_attempts: int = 0
    password_changed_at: Optional[str] = None
    created_by: Optional[str] = None

    @property
    def is_active(self) -> bool:
        return self.status == STATUS_ACTIVE

    @property
    def is_superadmin(self) -> bool:
        return self.role == ROLE_SUPERADMIN

    @property
    def role_label(self) -> str:
        return ROLE_LABELS.get(self.role, self.role)

    @property
    def permissions(self) -> frozenset[str]:
        return permissions_for(self.role)

    def has_permission(self, permission: str) -> bool:
        return permission in self.permissions


@dataclass(frozen=True)
class SessionInfo:
    id: str
    user_id: str
    org_id: str
    created_at: str
    last_seen_at: str
    expires_at: str
    ip: Optional[str] = None


def user_from_row(row: dict) -> User:
    """Converte linha crua do banco em :class:`User`, sem expor o hash da senha."""
    return User(
        id=row["id"],
        org_id=row["org_id"],
        email=row["email"],
        display_name=row["display_name"],
        role=row["role"],
        status=row["status"],
        must_change_password=bool(row.get("must_change_password")),
        totp_enabled=bool(row.get("totp_enabled")),
        created_at=row.get("created_at", ""),
        last_login_at=row.get("last_login_at"),
        locked_until=row.get("locked_until"),
        failed_attempts=int(row.get("failed_attempts") or 0),
        password_changed_at=row.get("password_changed_at"),
        created_by=row.get("created_by"),
    )


def org_from_row(row: dict) -> Organization:
    return Organization(
        id=row["id"],
        name=row["name"],
        slug=row["slug"],
        status=row["status"],
        created_at=row.get("created_at", ""),
        created_by=row.get("created_by"),
        notes=row.get("notes"),
    )
