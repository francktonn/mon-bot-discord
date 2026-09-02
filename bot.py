"""
Bot Discord de "Profil"
========================

Permet à chaque membre d'un serveur de se créer une petite fiche de profil :
- une photo de profil (image)
- une couleur de profil personnalisée
- une bio (texte)
- des liens Letterboxd et/ou AniList (max 2, un par service)
- un compteur du nombre de messages envoyés sur le serveur
- un temps total passé en vocal sur le serveur

Commandes slash (/) :
    /profil editor     -> ouvre un menu déroulant pour créer/modifier son profil
                           (bio, photo, couleur, liens, suppression) via des
                           sous-menus et des formulaires (modals), sans avoir
                           à mémoriser de commandes séparées.
    /profil view [membre]        -> afficher le profil (le sien ou celui d'un autre membre)

Le compteur de messages s'incrémente automatiquement à chaque message envoyé
par un membre sur le serveur (les messages des bots ne sont pas comptés).

Le temps de vocal s'accumule automatiquement dès qu'un membre rejoint un salon
vocal jusqu'à ce qu'il en reparte (les changements de salon vocal à vocal ne
réinitialisent pas le chrono, seule une déconnexion complète l'arrête).

Les données sont stockées dans une base PostgreSQL externe (Neon, Supabase, ou
tout autre Postgres compatible), configurée via la variable d'environnement
DATABASE_URL. Contrairement à un fichier SQLite local, cette base survit aux
redéploiements et redémarrages, même sur les hébergeurs sans disque persistant
(comme Render en plan gratuit). Un profil par (serveur, utilisateur) donc
chaque serveur a ses propres profils.
"""

import os
import re
import time
import asyncio
import unicodedata
from contextlib import contextmanager
from datetime import datetime, time as dt_time, timezone, timedelta
from zoneinfo import ZoneInfo

import discord
import psycopg2
import psycopg2.extras
from discord import app_commands
from discord.ext import commands, tasks
from dotenv import load_dotenv
from aiohttp import web

load_dotenv()

TOKEN = os.getenv("DISCORD_TOKEN")
# URL de connexion à la base Postgres (fournie par Neon, Supabase, Render Postgres...).
# Contrairement à un fichier SQLite local, une base externe survit aux redémarrages
# et redéploiements, même sur les plans gratuits sans disque persistant.
DATABASE_URL = os.getenv("DATABASE_URL")
MAX_LINKS = 2
MAX_BIO_LEN = 500
EMBED_COLOR = 0x5865F2  # blurple (couleur par défaut si aucune n'est choisie)

MONTHS_FR = [
    "janvier", "février", "mars", "avril", "mai", "juin",
    "juillet", "août", "septembre", "octobre", "novembre", "décembre",
]

# Heure française (gère automatiquement CET/CEST) à laquelle les anniversaires
# du jour sont annoncés.
PARIS_TZ = ZoneInfo("Europe/Paris")
BIRTHDAY_ANNOUNCE_TIME = dt_time(hour=9, minute=0, tzinfo=PARIS_TZ)

# Système d'événements : catégorie où sont rangés les salons temporaires,
# et délai après lequel un événement non clôturé manuellement est nettoyé automatiquement.
# ID de la catégorie Discord à utiliser en priorité (si elle existe bien sur le
# serveur où la commande est utilisée) ; sinon, le bot retombe sur une recherche/
# création par nom (utile si le bot tourne sur plusieurs serveurs différents).
EVENT_CATEGORY_ID = 1539441756939100260
EVENT_CATEGORY_NAME = "🗓️ Événements"
EVENT_AUTO_CLEANUP_HOURS = 6



# Seuls ces deux services sont acceptés comme liens de profil (un lien par service).
LINK_SERVICES = {
    "letterboxd": {
        "label": "Letterboxd",
        "domain": "letterboxd.com",
        "emoji": "🎬",
        "url_template": "https://letterboxd.com/{username}/",
    },
    "anilist": {
        "label": "AniList",
        "domain": "anilist.co",
        "emoji": "📺",
        "url_template": "https://anilist.co/user/{username}/",
    },
}


def resolve_link_url(service_key: str, raw_input: str) -> tuple[str | None, str | None]:
    """Construit/valide l'URL d'un lien Letterboxd ou AniList à partir de ce que
    l'utilisateur a tapé (un simple pseudo, ou une URL complète). Renvoie
    (url, None) si c'est valide, ou (None, message_erreur) sinon."""
    service = LINK_SERVICES[service_key]
    raw_input = raw_input.strip()

    if raw_input.lower().startswith(("http://", "https://")):
        if service["domain"] not in raw_input.lower():
            return None, f"Cette URL ne pointe pas vers {service['domain']}."
        return raw_input, None

    # Sinon, on considère que c'est un simple pseudo et on construit l'URL nous-mêmes.
    username = raw_input.strip("/ ")
    if not username or not re.fullmatch(r"[A-Za-z0-9_\-]{1,50}", username):
        return None, "Pseudo invalide (lettres, chiffres, tirets et underscores uniquement)."
    return service["url_template"].format(username=username), None


# ---------------------------------------------------------------------------
# Base de données (PostgreSQL, hébergée en externe pour survivre aux redéploiements)
# ---------------------------------------------------------------------------

@contextmanager
def get_db():
    """Ouvre une connexion Postgres pour la durée du bloc `with`, la valide
    (commit) si tout s'est bien passé, l'annule (rollback) sinon, puis la
    referme. Une nouvelle connexion à chaque appel est volontaire : ça évite
    les soucis de connexions "mortes" quand la base se met en veille
    (ex : Neon en scale-to-zero) entre deux actions du bot."""
    if not DATABASE_URL:
        raise RuntimeError(
            "La variable d'environnement DATABASE_URL n'est pas définie. "
            "Configure-la avec l'URL de connexion de ta base Postgres (voir le README)."
        )
    conn = psycopg2.connect(DATABASE_URL)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    with get_db() as db, db.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS profiles (
                guild_id BIGINT NOT NULL,
                user_id BIGINT NOT NULL,
                bio TEXT,
                avatar_url TEXT,
                message_count INTEGER NOT NULL DEFAULT 0,
                voice_seconds INTEGER NOT NULL DEFAULT 0,
                profile_color INTEGER,
                birthday_day INTEGER,
                birthday_month INTEGER,
                PRIMARY KEY (guild_id, user_id)
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS links (
                guild_id BIGINT NOT NULL,
                user_id BIGINT NOT NULL,
                label TEXT NOT NULL,
                url TEXT NOT NULL,
                position INTEGER NOT NULL,
                PRIMARY KEY (guild_id, user_id, label)
            )
            """
        )
        # Migration légère pour les bases créées avec une version antérieure du bot.
        # Postgres gère nativement "IF NOT EXISTS", plus besoin de vérifier à la main.
        cur.execute("ALTER TABLE profiles ADD COLUMN IF NOT EXISTS message_count INTEGER NOT NULL DEFAULT 0")
        cur.execute("ALTER TABLE profiles ADD COLUMN IF NOT EXISTS voice_seconds INTEGER NOT NULL DEFAULT 0")
        cur.execute("ALTER TABLE profiles ADD COLUMN IF NOT EXISTS profile_color INTEGER")
        cur.execute("ALTER TABLE profiles ADD COLUMN IF NOT EXISTS birthday_day INTEGER")
        cur.execute("ALTER TABLE profiles ADD COLUMN IF NOT EXISTS birthday_month INTEGER")

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS events (
                id SERIAL PRIMARY KEY,
                guild_id BIGINT NOT NULL,
                organizer_id BIGINT NOT NULL,
                name TEXT NOT NULL,
                description TEXT,
                event_datetime TIMESTAMPTZ NOT NULL,
                max_participants INTEGER,
                text_channel_id BIGINT,
                voice_channel_id BIGINT,
                event_role_id BIGINT,
                announcement_channel_id BIGINT,
                announcement_message_id BIGINT,
                status TEXT NOT NULL DEFAULT 'open',
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )
        cur.execute("ALTER TABLE events ADD COLUMN IF NOT EXISTS event_role_id BIGINT")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS event_participants (
                event_id INTEGER NOT NULL REFERENCES events(id) ON DELETE CASCADE,
                user_id BIGINT NOT NULL,
                joined_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                PRIMARY KEY (event_id, user_id)
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS role_menus (
                id SERIAL PRIMARY KEY,
                guild_id BIGINT NOT NULL,
                name TEXT NOT NULL,
                channel_id BIGINT,
                message_id BIGINT,
                created_by BIGINT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS role_menu_categories (
                id SERIAL PRIMARY KEY,
                menu_id INTEGER NOT NULL REFERENCES role_menus(id) ON DELETE CASCADE,
                label TEXT NOT NULL,
                emoji TEXT,
                exclusive BOOLEAN NOT NULL DEFAULT FALSE,
                position INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS role_menu_roles (
                id SERIAL PRIMARY KEY,
                category_id INTEGER NOT NULL REFERENCES role_menu_categories(id) ON DELETE CASCADE,
                discord_role_id BIGINT NOT NULL,
                label TEXT NOT NULL,
                emoji TEXT,
                description TEXT,
                position INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS role_menu_assignments (
                role_entry_id INTEGER NOT NULL REFERENCES role_menu_roles(id) ON DELETE CASCADE,
                user_id BIGINT NOT NULL,
                assigned_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                PRIMARY KEY (role_entry_id, user_id)
            )
            """
        )


def get_or_create_profile(guild_id: int, user_id: int):
    with get_db() as db, db.cursor() as cur:
        cur.execute(
            "INSERT INTO profiles (guild_id, user_id) VALUES (%s, %s) "
            "ON CONFLICT (guild_id, user_id) DO NOTHING",
            (guild_id, user_id),
        )


# Liste blanche des colonnes modifiables via update_field, pour éviter toute
# injection SQL par le nom de colonne (celui-ci ne peut pas être paramétré
# comme une valeur classique).
_EDITABLE_FIELDS = {"bio", "avatar_url", "profile_color"}


def update_field(guild_id: int, user_id: int, field: str, value):
    if field not in _EDITABLE_FIELDS:
        raise ValueError(f"Champ non autorisé : {field}")
    get_or_create_profile(guild_id, user_id)
    with get_db() as db, db.cursor() as cur:
        cur.execute(
            f"UPDATE profiles SET {field} = %s WHERE guild_id = %s AND user_id = %s",
            (value, guild_id, user_id),
        )


def set_birthday(guild_id: int, user_id: int, day: int, month: int):
    get_or_create_profile(guild_id, user_id)
    with get_db() as db, db.cursor() as cur:
        cur.execute(
            "UPDATE profiles SET birthday_day = %s, birthday_month = %s WHERE guild_id = %s AND user_id = %s",
            (day, month, guild_id, user_id),
        )


def clear_birthday(guild_id: int, user_id: int):
    get_or_create_profile(guild_id, user_id)
    with get_db() as db, db.cursor() as cur:
        cur.execute(
            "UPDATE profiles SET birthday_day = NULL, birthday_month = NULL "
            "WHERE guild_id = %s AND user_id = %s",
            (guild_id, user_id),
        )


def get_birthdays_for_date(day: int, month: int) -> list[tuple[int, int]]:
    """Renvoie (guild_id, user_id) de tous les membres, tous serveurs confondus,
    dont l'anniversaire tombe sur cette date."""
    with get_db() as db, db.cursor() as cur:
        cur.execute(
            "SELECT guild_id, user_id FROM profiles WHERE birthday_day = %s AND birthday_month = %s",
            (day, month),
        )
        return cur.fetchall()


# ---------------------------------------------------------------------------
# Base de données : système d'événements
# ---------------------------------------------------------------------------

def create_event(
    guild_id: int,
    organizer_id: int,
    name: str,
    description: str | None,
    event_datetime_utc: datetime,
    max_participants: int | None,
) -> int:
    with get_db() as db, db.cursor() as cur:
        cur.execute(
            """
            INSERT INTO events (guild_id, organizer_id, name, description, event_datetime, max_participants)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (guild_id, organizer_id, name, description, event_datetime_utc, max_participants),
        )
        event_id = cur.fetchone()[0]
    return event_id


def set_event_channels(event_id: int, text_channel_id: int, voice_channel_id: int):
    with get_db() as db, db.cursor() as cur:
        cur.execute(
            "UPDATE events SET text_channel_id = %s, voice_channel_id = %s WHERE id = %s",
            (text_channel_id, voice_channel_id, event_id),
        )


def set_event_role(event_id: int, role_id: int):
    with get_db() as db, db.cursor() as cur:
        cur.execute("UPDATE events SET event_role_id = %s WHERE id = %s", (role_id, event_id))


def set_event_announcement(event_id: int, channel_id: int, message_id: int):
    with get_db() as db, db.cursor() as cur:
        cur.execute(
            "UPDATE events SET announcement_channel_id = %s, announcement_message_id = %s WHERE id = %s",
            (channel_id, message_id, event_id),
        )


def get_event(event_id: int) -> dict | None:
    with get_db() as db, db.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT * FROM events WHERE id = %s", (event_id,))
        row = cur.fetchone()
    return dict(row) if row else None


def get_open_events(guild_id: int) -> list[dict]:
    with get_db() as db, db.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "SELECT * FROM events WHERE guild_id = %s AND status = 'open' ORDER BY event_datetime ASC",
            (guild_id,),
        )
        return [dict(r) for r in cur.fetchall()]


def get_all_open_events() -> list[dict]:
    """Tous les événements ouverts, tous serveurs confondus (pour réenregistrer
    les boutons persistants au démarrage et pour la tâche de nettoyage auto)."""
    with get_db() as db, db.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT * FROM events WHERE status = 'open'")
        return [dict(r) for r in cur.fetchall()]


def add_participant(event_id: int, user_id: int) -> tuple[bool, str]:
    with get_db() as db, db.cursor() as cur:
        cur.execute("SELECT status, max_participants FROM events WHERE id = %s", (event_id,))
        event_row = cur.fetchone()
        if event_row is None:
            return False, "Cet événement n'existe plus."
        status, max_participants = event_row
        if status != "open":
            return False, "Cet événement est fermé."

        cur.execute(
            "SELECT 1 FROM event_participants WHERE event_id = %s AND user_id = %s",
            (event_id, user_id),
        )
        if cur.fetchone() is not None:
            return False, "Tu participes déjà à cet événement."

        if max_participants is not None:
            cur.execute("SELECT COUNT(*) FROM event_participants WHERE event_id = %s", (event_id,))
            count = cur.fetchone()[0]
            if count >= max_participants:
                return False, "Cet événement est complet."

        cur.execute(
            "INSERT INTO event_participants (event_id, user_id) VALUES (%s, %s)",
            (event_id, user_id),
        )
    return True, "ok"


def remove_participant(event_id: int, user_id: int) -> bool:
    with get_db() as db, db.cursor() as cur:
        cur.execute(
            "DELETE FROM event_participants WHERE event_id = %s AND user_id = %s",
            (event_id, user_id),
        )
        return cur.rowcount > 0


def get_participant_count(event_id: int) -> int:
    with get_db() as db, db.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM event_participants WHERE event_id = %s", (event_id,))
        return cur.fetchone()[0]


def close_event(event_id: int, new_status: str = "completed"):
    with get_db() as db, db.cursor() as cur:
        cur.execute("UPDATE events SET status = %s WHERE id = %s", (new_status, event_id))


# ---------------------------------------------------------------------------
# Base de données : menus de sélection de rôles ("Loadout")
# ---------------------------------------------------------------------------

def create_role_menu(guild_id: int, name: str, created_by: int) -> int:
    with get_db() as db, db.cursor() as cur:
        cur.execute(
            "INSERT INTO role_menus (guild_id, name, created_by) VALUES (%s, %s, %s) RETURNING id",
            (guild_id, name, created_by),
        )
        return cur.fetchone()[0]


def get_role_menu(menu_id: int) -> dict | None:
    with get_db() as db, db.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT * FROM role_menus WHERE id = %s", (menu_id,))
        row = cur.fetchone()
    return dict(row) if row else None


def get_role_menus(guild_id: int) -> list[dict]:
    with get_db() as db, db.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT * FROM role_menus WHERE guild_id = %s ORDER BY id", (guild_id,))
        return [dict(r) for r in cur.fetchall()]


def get_all_published_role_menus() -> list[dict]:
    """Tous les menus déjà publiés (message_id renseigné), tous serveurs confondus —
    pour réenregistrer le bouton persistant au démarrage du bot."""
    with get_db() as db, db.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT * FROM role_menus WHERE message_id IS NOT NULL")
        return [dict(r) for r in cur.fetchall()]


def set_role_menu_message(menu_id: int, channel_id: int, message_id: int):
    with get_db() as db, db.cursor() as cur:
        cur.execute(
            "UPDATE role_menus SET channel_id = %s, message_id = %s WHERE id = %s",
            (channel_id, message_id, menu_id),
        )


def add_role_menu_category(menu_id: int, label: str, emoji: str | None, exclusive: bool) -> int:
    with get_db() as db, db.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM role_menu_categories WHERE menu_id = %s", (menu_id,))
        position = cur.fetchone()[0]
        cur.execute(
            "INSERT INTO role_menu_categories (menu_id, label, emoji, exclusive, position) "
            "VALUES (%s, %s, %s, %s, %s) RETURNING id",
            (menu_id, label, emoji, exclusive, position),
        )
        return cur.fetchone()[0]


def get_role_menu_categories(menu_id: int) -> list[dict]:
    with get_db() as db, db.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT * FROM role_menu_categories WHERE menu_id = %s ORDER BY position", (menu_id,))
        return [dict(r) for r in cur.fetchall()]


def get_role_menu_category(category_id: int) -> dict | None:
    with get_db() as db, db.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT * FROM role_menu_categories WHERE id = %s", (category_id,))
        row = cur.fetchone()
    return dict(row) if row else None


def get_role_menu_categories_for_guild(guild_id: int) -> list[dict]:
    """Toutes les catégories de tous les menus d'un serveur, pour l'autocomplétion."""
    with get_db() as db, db.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT c.* FROM role_menu_categories c
            JOIN role_menus m ON m.id = c.menu_id
            WHERE m.guild_id = %s
            ORDER BY c.menu_id, c.position
            """,
            (guild_id,),
        )
        return [dict(r) for r in cur.fetchall()]


def add_role_menu_role(
    category_id: int, discord_role_id: int, label: str, emoji: str | None, description: str | None
) -> int:
    with get_db() as db, db.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM role_menu_roles WHERE category_id = %s", (category_id,))
        position = cur.fetchone()[0]
        cur.execute(
            "INSERT INTO role_menu_roles (category_id, discord_role_id, label, emoji, description, position) "
            "VALUES (%s, %s, %s, %s, %s, %s) RETURNING id",
            (category_id, discord_role_id, label, emoji, description, position),
        )
        return cur.fetchone()[0]


def get_role_menu_roles(category_id: int) -> list[dict]:
    with get_db() as db, db.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT * FROM role_menu_roles WHERE category_id = %s ORDER BY position", (category_id,))
        return [dict(r) for r in cur.fetchall()]


def get_role_menu_role(role_entry_id: int) -> dict | None:
    with get_db() as db, db.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT * FROM role_menu_roles WHERE id = %s", (role_entry_id,))
        row = cur.fetchone()
    return dict(row) if row else None


def add_role_assignment(role_entry_id: int, user_id: int):
    with get_db() as db, db.cursor() as cur:
        cur.execute(
            "INSERT INTO role_menu_assignments (role_entry_id, user_id) VALUES (%s, %s) "
            "ON CONFLICT (role_entry_id, user_id) DO NOTHING",
            (role_entry_id, user_id),
        )


def remove_role_assignment(role_entry_id: int, user_id: int):
    with get_db() as db, db.cursor() as cur:
        cur.execute(
            "DELETE FROM role_menu_assignments WHERE role_entry_id = %s AND user_id = %s",
            (role_entry_id, user_id),
        )


def get_role_assignment_count(role_entry_id: int) -> int:
    with get_db() as db, db.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM role_menu_assignments WHERE role_entry_id = %s", (role_entry_id,))
        return cur.fetchone()[0]


def fetch_profile(guild_id: int, user_id: int):
    with get_db() as db, db.cursor() as cur:
        cur.execute(
            "SELECT bio, avatar_url, message_count, voice_seconds, profile_color, "
            "birthday_day, birthday_month "
            "FROM profiles WHERE guild_id = %s AND user_id = %s",
            (guild_id, user_id),
        )
        row = cur.fetchone()
        cur.execute(
            "SELECT label, url FROM links WHERE guild_id = %s AND user_id = %s ORDER BY position ASC",
            (guild_id, user_id),
        )
        links = cur.fetchall()
    return row, links


def increment_message_count(guild_id: int, user_id: int):
    get_or_create_profile(guild_id, user_id)
    with get_db() as db, db.cursor() as cur:
        cur.execute(
            "UPDATE profiles SET message_count = message_count + 1 WHERE guild_id = %s AND user_id = %s",
            (guild_id, user_id),
        )


def add_voice_seconds(guild_id: int, user_id: int, seconds: int):
    if seconds <= 0:
        return
    get_or_create_profile(guild_id, user_id)
    with get_db() as db, db.cursor() as cur:
        cur.execute(
            "UPDATE profiles SET voice_seconds = voice_seconds + %s WHERE guild_id = %s AND user_id = %s",
            (seconds, guild_id, user_id),
        )


def format_duration(total_seconds: int) -> str:
    total_seconds = int(total_seconds or 0)
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours > 0:
        return f"{hours}h {minutes}min"
    if minutes > 0:
        return f"{minutes}min {seconds}s"
    return f"{seconds}s"


def add_link(guild_id: int, user_id: int, label: str, url: str) -> tuple[bool, str]:
    get_or_create_profile(guild_id, user_id)
    with get_db() as db, db.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM links WHERE guild_id = %s AND user_id = %s",
            (guild_id, user_id),
        )
        count = cur.fetchone()[0]
        cur.execute(
            "SELECT 1 FROM links WHERE guild_id = %s AND user_id = %s AND label = %s",
            (guild_id, user_id, label),
        )
        exists = cur.fetchone() is not None

        if not exists and count >= MAX_LINKS:
            return False, f"Tu as déjà atteint le maximum de {MAX_LINKS} liens. Supprime-en un d'abord."

        cur.execute(
            """
            INSERT INTO links (guild_id, user_id, label, url, position)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (guild_id, user_id, label) DO UPDATE SET url = EXCLUDED.url
            """,
            (guild_id, user_id, label, url, count),
        )
    return True, "ok"


def remove_link(guild_id: int, user_id: int, label: str) -> bool:
    with get_db() as db, db.cursor() as cur:
        cur.execute(
            "DELETE FROM links WHERE guild_id = %s AND user_id = %s AND label = %s",
            (guild_id, user_id, label),
        )
        return cur.rowcount > 0


def delete_profile(guild_id: int, user_id: int):
    with get_db() as db, db.cursor() as cur:
        cur.execute("DELETE FROM profiles WHERE guild_id = %s AND user_id = %s", (guild_id, user_id))
        cur.execute("DELETE FROM links WHERE guild_id = %s AND user_id = %s", (guild_id, user_id))


# ---------------------------------------------------------------------------
# Bot
# ---------------------------------------------------------------------------

intents = discord.Intents.default()
bot = commands.Bot(command_prefix="!", intents=intents)

profil_group = app_commands.Group(name="profil", description="Gère ton profil sur ce serveur")

# Suivi en mémoire des sessions vocales en cours : {(guild_id, user_id): timestamp_de_connexion}
voice_sessions: dict[tuple[int, int], float] = {}

# Empêche de démarrer plusieurs fois le serveur web si on_ready se déclenche
# plusieurs fois (reconnexions Discord).
_webserver_started = False

# Empêche de réenregistrer plusieurs fois les vues persistantes (événements,
# menus de rôles) si on_ready se déclenche plusieurs fois (reconnexions Discord).
_persistent_views_registered = False


# ---------------------------------------------------------------------------
# Gestion d'erreurs globale
# ---------------------------------------------------------------------------
# Sans ça, une exception survenant APRÈS un interaction.response.defer() (ex:
# la base Postgres qui échoue, un bug quelconque) ne serait jamais rattrapée :
# l'interaction resterait bloquée indéfiniment sur "... réfléchit" côté
# utilisateur, sans aucun message d'erreur ni sur Discord ni dans les logs.

async def _report_interaction_error(interaction: discord.Interaction, error: Exception):
    import traceback
    print(f"Erreur lors du traitement d'une interaction ({interaction.command}) :")
    traceback.print_exception(type(error), error, error.__traceback__)

    message = "⚠️ Une erreur inattendue est survenue. Réessaie, ou préviens un admin si ça persiste."
    try:
        if interaction.response.is_done():
            await interaction.followup.send(message, ephemeral=True)
        else:
            await interaction.response.send_message(message, ephemeral=True)
    except discord.HTTPException:
        pass


class SafeView(discord.ui.View):
    """Vue de base qui rattrape systématiquement les erreurs de ses boutons/menus,
    pour ne jamais laisser une interaction bloquée sur "... réfléchit" en cas de bug."""

    async def on_error(self, interaction: discord.Interaction, error: Exception, item: discord.ui.Item):
        await _report_interaction_error(interaction, error)


class SafeModal(discord.ui.Modal):
    """Modal de base qui rattrape systématiquement les erreurs de soumission,
    pour ne jamais laisser une interaction bloquée en cas de bug."""

    async def on_error(self, interaction: discord.Interaction, error: Exception):
        await _report_interaction_error(interaction, error)



async def _health_handler(request: web.Request) -> web.Response:
    return web.Response(text="Le bot Discord de profil est en ligne.")


async def start_fake_webserver():
    """Démarre un minuscule serveur HTTP.

    Sert uniquement à satisfaire les hébergeurs (comme Render) qui exigent
    qu'un service de type "Web Service" écoute sur un port, alors que le bot
    Discord lui-même n'a besoin d'aucun port : toute sa communication passe
    par une connexion sortante vers l'API Discord (le "gateway").
    """
    app = web.Application()
    app.router.add_get("/", _health_handler)

    runner = web.AppRunner(app)
    await runner.setup()

    port = int(os.getenv("PORT", "10000"))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    print(f"Serveur web factice démarré sur le port {port} (pour satisfaire l'hébergeur).")


def build_profile_embed(member: discord.Member, row, links) -> discord.Embed:
    bio, avatar_url, message_count, voice_seconds, profile_color, birthday_day, birthday_month = (
        row if row else (None, None, 0, 0, None, None, None)
    )
    message_count = message_count or 0
    voice_seconds = voice_seconds or 0

    embed = discord.Embed(
        title=f"Profil de {member.display_name}",
        description=bio if bio else "*Aucune bio définie.*",
        color=profile_color if profile_color else EMBED_COLOR,
    )

    # Photo de profil personnalisée si définie, sinon avatar Discord
    embed.set_thumbnail(url=avatar_url or member.display_avatar.url)

    embed.add_field(name="💬 Messages envoyés", value=str(message_count), inline=True)
    embed.add_field(name="🎙️ Temps en vocal", value=format_duration(voice_seconds), inline=True)

    if birthday_day and birthday_month:
        embed.add_field(
            name="🎂 Anniversaire",
            value=f"{birthday_day} {MONTHS_FR[birthday_month - 1]}",
            inline=True,
        )

    if links:
        liens_txt = "\n".join(f"🔗 [{label}]({url})" for label, url in links)
        embed.add_field(name="Liens utiles", value=liens_txt, inline=False)

    embed.set_footer(text=f"Membre depuis le serveur • {member.guild.name}")
    return embed


@tasks.loop(time=BIRTHDAY_ANNOUNCE_TIME)
async def birthday_announcement_task():
    """S'exécute chaque jour à l'heure définie (heure française, CET/CEST gérée
    automatiquement par ZoneInfo) et annonce les anniversaires du jour."""
    today = datetime.now(PARIS_TZ)
    matches = get_birthdays_for_date(today.day, today.month)

    for guild_id, user_id in matches:
        guild = bot.get_guild(guild_id)
        if guild is None:
            continue
        member = guild.get_member(user_id)
        if member is None or member.bot:
            continue
        channel = guild.system_channel
        if channel is None:
            continue
        try:
            await channel.send(f"🎂 Joyeux anniversaire {member.mention} ! 🎉")
        except discord.Forbidden:
            pass


@birthday_announcement_task.before_loop
async def before_birthday_announcement_task():
    # Évite que la tâche se déclenche avant que le bot ne soit pleinement connecté.
    await bot.wait_until_ready()


@bot.event
async def on_ready():
    init_db()
    try:
        synced = await bot.tree.sync()
        print(f"{bot.user} est connecté. {len(synced)} commandes synchronisées.")
    except Exception as e:
        print(f"Erreur de synchronisation des commandes : {e}")

    # Le serveur factice n'est utile que si l'hébergeur fournit un PORT
    # (c'est le cas de Render en Web Service). En local, on l'ignore.
    global _webserver_started
    if not _webserver_started and os.getenv("PORT"):
        _webserver_started = True
        asyncio.create_task(start_fake_webserver())

    # tasks.loop lève une erreur si on tente de la démarrer alors qu'elle
    # tourne déjà (ex: on_ready se redéclenche après une reconnexion Discord).
    if not birthday_announcement_task.is_running():
        birthday_announcement_task.start()
    if not event_cleanup_task.is_running():
        event_cleanup_task.start()

    # Réenregistre les boutons "Participer"/"Se désister" des événements encore
    # ouverts et "Ouvrir mon inventaire" des menus de rôles publiés, pour qu'ils
    # continuent de fonctionner après un redémarrage ou un redéploiement du bot
    # (les vues persistantes ne survivent pas d'elles-mêmes).
    global _persistent_views_registered
    if not _persistent_views_registered:
        _persistent_views_registered = True
        for event in get_all_open_events():
            bot.add_view(EventView(event["id"]))
        for menu in get_all_published_role_menus():
            bot.add_view(RoleMenuOpenView(menu["id"]))

    # Si le bot redémarre pendant que des membres sont déjà en vocal,
    # on démarre leur chrono maintenant pour ne pas perdre le suivi.
    now = time.monotonic()
    for guild in bot.guilds:
        for voice_channel in guild.voice_channels:
            for member in voice_channel.members:
                if not member.bot:
                    voice_sessions[(guild.id, member.id)] = now


@bot.event
async def on_message(message: discord.Message):
    # On ignore les messages privés et ceux envoyés par des bots (dont ce bot lui-même)
    if message.guild is not None and not message.author.bot:
        increment_message_count(message.guild.id, message.author.id)
    await bot.process_commands(message)


@bot.event
async def on_voice_state_update(
    member: discord.Member, before: discord.VoiceState, after: discord.VoiceState
):
    if member.bot:
        return

    key = (member.guild.id, member.id)

    # Le membre vient de rejoindre un salon vocal (il n'était dans aucun avant)
    if before.channel is None and after.channel is not None:
        voice_sessions[key] = time.monotonic()

    # Le membre vient de quitter le vocal complètement (aucun salon après)
    elif before.channel is not None and after.channel is None:
        start = voice_sessions.pop(key, None)
        if start is not None:
            elapsed = int(time.monotonic() - start)
            add_voice_seconds(member.guild.id, member.id, elapsed)
    # Un simple changement de salon vocal à vocal ne coupe pas le chrono.


# ---------------------------------------------------------------------------
# Système d'événements : aides, salons temporaires et boutons persistants
# ---------------------------------------------------------------------------

def sanitize_channel_name(name: str) -> str:
    """Convertit un nom d'événement en nom de salon Discord valide."""
    name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("ascii")
    name = name.lower().strip()
    name = re.sub(r"\s+", "-", name)
    name = re.sub(r"[^a-z0-9\-_]", "", name)
    name = re.sub(r"-{2,}", "-", name).strip("-")
    return name[:90] or "evenement"


def parse_event_datetime(date_str: str, heure_str: str) -> datetime | None:
    """Parse une date JJ/MM/AAAA et une heure HH:MM (heure française) en datetime
    "aware" dans le fuseau Europe/Paris. Renvoie None si le format ou la date
    (ex: 31/02) est invalide."""
    date_str = date_str.strip()
    heure_str = heure_str.strip()

    m_date = re.fullmatch(r"(\d{1,2})/(\d{1,2})/(\d{4})", date_str)
    m_heure = re.fullmatch(r"(\d{1,2}):(\d{2})", heure_str)
    if not m_date or not m_heure:
        return None

    day, month, year = int(m_date.group(1)), int(m_date.group(2)), int(m_date.group(3))
    hour, minute = int(m_heure.group(1)), int(m_heure.group(2))

    if not is_valid_day_month(day, month):
        return None
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        return None

    try:
        return datetime(year, month, day, hour, minute, tzinfo=PARIS_TZ)
    except ValueError:
        # Cas comme le 29/02 sur une année non bissextile : la date n'existe pas.
        return None


def build_event_embed(
    name: str,
    description: str | None,
    event_dt_paris: datetime,
    max_participants: int | None,
    participant_count: int,
    organizer_id: int,
) -> discord.Embed:
    embed = discord.Embed(
        title=f"📅 {name}",
        description=description or "*Aucune description.*",
        color=EMBED_COLOR,
    )
    embed.add_field(
        name="🗓️ Date",
        value=event_dt_paris.strftime("%d/%m/%Y à %H:%M") + " (heure française)",
        inline=False,
    )
    limite = f"{participant_count}/{max_participants}" if max_participants else f"{participant_count} (illimité)"
    embed.add_field(name="👥 Participants", value=limite, inline=True)
    embed.add_field(name="🎤 Organisateur", value=f"<@{organizer_id}>", inline=True)
    embed.set_footer(text="Clique sur Participer pour rejoindre les salons de l'événement")
    return embed


async def create_event_role_and_channels(
    guild: discord.Guild, event_name: str, organizer: discord.Member
) -> tuple[discord.Role, discord.TextChannel, discord.VoiceChannel]:
    """Crée un rôle Discord temporaire dédié à l'événement (attribué aux participants
    au fil de l'eau), puis les salons temporaires dont l'accès repose sur ce rôle
    plutôt que sur des permissions par membre — plus propre et plus facile à nettoyer."""
    event_role = await guild.create_role(
        name=f"🎉 {event_name}"[:100],
        mentionable=True,
        reason=f"Rôle d'événement créé par {organizer}",
    )

    # On utilise la catégorie fixe configurée (EVENT_CATEGORY_ID) si elle existe
    # bien sur ce serveur ; sinon on retombe sur une recherche/création par nom.
    category = guild.get_channel(EVENT_CATEGORY_ID)
    if not isinstance(category, discord.CategoryChannel):
        category = discord.utils.get(guild.categories, name=EVENT_CATEGORY_NAME)
        if category is None:
            category = await guild.create_category(EVENT_CATEGORY_NAME)

    safe_name = sanitize_channel_name(event_name)

    overwrites = {
        guild.default_role: discord.PermissionOverwrite(view_channel=False),
        guild.me: discord.PermissionOverwrite(view_channel=True, manage_channels=True, connect=True),
        event_role: discord.PermissionOverwrite(view_channel=True, send_messages=True, connect=True, speak=True),
    }

    text_channel = await guild.create_text_channel(
        f"💬-{safe_name}", category=category, overwrites=overwrites,
        reason=f"Événement créé par {organizer}",
    )
    voice_channel = await guild.create_voice_channel(
        f"🔊 {event_name}"[:100], category=category, overwrites=overwrites,
        reason=f"Événement créé par {organizer}",
    )
    return event_role, text_channel, voice_channel


async def cleanup_event_channels(guild: discord.Guild, event: dict):
    for channel_id in (event.get("text_channel_id"), event.get("voice_channel_id")):
        if not channel_id:
            continue
        channel = guild.get_channel(channel_id)
        if channel is not None:
            try:
                await channel.delete(reason="Événement terminé")
            except (discord.Forbidden, discord.NotFound):
                pass

    role_id = event.get("event_role_id")
    if role_id:
        role = guild.get_role(role_id)
        if role is not None:
            try:
                await role.delete(reason="Événement terminé")
            except (discord.Forbidden, discord.NotFound):
                pass


async def refresh_event_announcement(bot_client: discord.Client, event_id: int):
    """Met à jour le compteur de participants sur le message d'annonce d'un événement."""
    event = get_event(event_id)
    if not event or not event["announcement_channel_id"] or not event["announcement_message_id"]:
        return
    channel = bot_client.get_channel(event["announcement_channel_id"])
    if channel is None:
        return
    try:
        message = await channel.fetch_message(event["announcement_message_id"])
    except (discord.NotFound, discord.Forbidden):
        return

    count = get_participant_count(event_id)
    event_dt_paris = event["event_datetime"].astimezone(PARIS_TZ)
    embed = build_event_embed(
        event["name"], event["description"], event_dt_paris,
        event["max_participants"], count, event["organizer_id"],
    )
    try:
        await message.edit(embed=embed)
    except discord.Forbidden:
        pass


async def finalize_event_announcement(bot_client: discord.Client, event_id: int, note: str):
    """Fige l'annonce d'un événement clôturé : couleur neutre, boutons retirés."""
    event = get_event(event_id)
    if not event or not event["announcement_channel_id"] or not event["announcement_message_id"]:
        return
    channel = bot_client.get_channel(event["announcement_channel_id"])
    if channel is None:
        return
    try:
        message = await channel.fetch_message(event["announcement_message_id"])
    except (discord.NotFound, discord.Forbidden):
        return

    count = get_participant_count(event_id)
    event_dt_paris = event["event_datetime"].astimezone(PARIS_TZ)
    embed = build_event_embed(
        event["name"], event["description"], event_dt_paris,
        event["max_participants"], count, event["organizer_id"],
    )
    embed.color = 0x2C2F33
    embed.set_footer(text=note)
    try:
        await message.edit(embed=embed, view=None)
    except discord.Forbidden:
        pass


async def handle_event_join(interaction: discord.Interaction, event_id: int):
    """L'appelant doit avoir déjà déferé la réponse à l'interaction (via
    interaction.response.defer(ephemeral=True)) avant d'appeler cette fonction."""
    event = get_event(event_id)
    if event is None:
        await interaction.followup.send("Cet événement n'existe plus.", ephemeral=True)
        return

    ok, msg = add_participant(event_id, interaction.user.id)
    if not ok:
        await interaction.followup.send(msg, ephemeral=True)
        return

    guild = interaction.guild
    text_channel = guild.get_channel(event["text_channel_id"]) if event["text_channel_id"] else None
    role = guild.get_role(event["event_role_id"]) if event["event_role_id"] else None

    try:
        if role:
            await interaction.user.add_roles(role, reason="Participation à un événement")
    except discord.Forbidden:
        pass

    lien = f" Rendez-vous sur {text_channel.mention}." if text_channel else ""
    await interaction.followup.send(
        f"Tu participes maintenant à **{event['name']}** !{lien}", ephemeral=True
    )
    await refresh_event_announcement(interaction.client, event_id)


async def handle_event_leave(interaction: discord.Interaction, event_id: int):
    """L'appelant doit avoir déjà déferé la réponse à l'interaction (via
    interaction.response.defer(ephemeral=True)) avant d'appeler cette fonction."""
    event = get_event(event_id)
    if event is None:
        await interaction.followup.send("Cet événement n'existe plus.", ephemeral=True)
        return

    removed = remove_participant(event_id, interaction.user.id)
    if not removed:
        await interaction.followup.send("Tu ne participais pas à cet événement.", ephemeral=True)
        return

    guild = interaction.guild
    voice_channel = guild.get_channel(event["voice_channel_id"]) if event["voice_channel_id"] else None
    role = guild.get_role(event["event_role_id"]) if event["event_role_id"] else None

    try:
        if role:
            await interaction.user.remove_roles(role, reason="Désistement d'un événement")
        # Si la personne est déjà connectée au salon vocal de l'événement, on la déconnecte
        # (le retrait du rôle n'éjecte pas automatiquement quelqu'un déjà connecté).
        member = guild.get_member(interaction.user.id)
        if (
            voice_channel and member and member.voice
            and member.voice.channel and member.voice.channel.id == voice_channel.id
        ):
            await member.move_to(None)
    except discord.Forbidden:
        pass

    await interaction.followup.send(f"Tu ne participes plus à **{event['name']}**.", ephemeral=True)
    await refresh_event_announcement(interaction.client, event_id)


class EventView(SafeView):
    """Vue persistante (timeout=None) attachée à un événement précis, via des
    custom_id encodant l'event_id. Réenregistrée à chaque démarrage du bot pour
    que les boutons restent fonctionnels après un redéploiement."""

    def __init__(self, event_id: int):
        super().__init__(timeout=None)
        self.event_id = event_id

        join_button = discord.ui.Button(
            label="Participer", emoji="✅", style=discord.ButtonStyle.success,
            custom_id=f"event_join:{event_id}",
        )
        join_button.callback = self.join_callback
        self.add_item(join_button)

        leave_button = discord.ui.Button(
            label="Se désister", emoji="❌", style=discord.ButtonStyle.secondary,
            custom_id=f"event_leave:{event_id}",
        )
        leave_button.callback = self.leave_callback
        self.add_item(leave_button)

    async def join_callback(self, interaction: discord.Interaction):
        # On accuse réception immédiatement (avant toute requête DB) pour ne
        # jamais risquer de dépasser la fenêtre de 3 secondes imposée par Discord
        # (ex: si la base Postgres met du temps à répondre après une mise en veille).
        await interaction.response.defer(ephemeral=True)
        await handle_event_join(interaction, self.event_id)

    async def leave_callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        await handle_event_leave(interaction, self.event_id)


event_group = app_commands.Group(name="event", description="Créer et gérer des événements avec salons temporaires")


async def event_autocomplete(interaction: discord.Interaction, current: str):
    if interaction.guild_id is None:
        return []
    events = get_open_events(interaction.guild_id)
    choices = []
    for e in events:
        dt_paris = e["event_datetime"].astimezone(PARIS_TZ)
        label = f"{e['name']} — {dt_paris.strftime('%d/%m %H:%M')}"
        if current.lower() in label.lower():
            choices.append(app_commands.Choice(name=label[:100], value=e["id"]))
    return choices[:25]


@event_group.command(name="create", description="Créer un événement avec ses salons temporaires")
@app_commands.describe(
    nom="Nom de l'événement",
    date="Date au format JJ/MM/AAAA",
    heure="Heure au format HH:MM (heure française)",
    description="Description de l'événement (optionnel)",
    max_participants="Nombre maximum de participants (optionnel, illimité si vide)",
)
async def event_create(
    interaction: discord.Interaction,
    nom: str,
    date: str,
    heure: str,
    description: str = None,
    max_participants: int = None,
):
    if interaction.guild is None:
        await interaction.response.send_message("Cette commande doit être utilisée sur un serveur.", ephemeral=True)
        return

    if len(nom) > 80:
        await interaction.response.send_message("Le nom de l'événement est trop long (max 80 caractères).", ephemeral=True)
        return

    event_dt = parse_event_datetime(date, heure)
    if event_dt is None:
        await interaction.response.send_message(
            "Date ou heure invalide. Utilise le format JJ/MM/AAAA pour la date et HH:MM pour l'heure.",
            ephemeral=True,
        )
        return

    if event_dt <= datetime.now(PARIS_TZ):
        await interaction.response.send_message("La date de l'événement doit être dans le futur.", ephemeral=True)
        return

    if max_participants is not None and max_participants < 1:
        await interaction.response.send_message(
            "Le nombre maximum de participants doit être un nombre positif.", ephemeral=True
        )
        return

    # La création des salons peut prendre un court instant.
    await interaction.response.defer(thinking=True)

    try:
        event_role, text_channel, voice_channel = await create_event_role_and_channels(
            interaction.guild, nom, interaction.user
        )
    except discord.Forbidden:
        await interaction.followup.send(
            "Je n'ai pas la permission de créer des rôles/salons sur ce serveur. "
            "Vérifie que j'ai les permissions \"Gérer les salons\" et \"Gérer les rôles\", "
            "et que mon propre rôle est bien placé dans la hiérarchie du serveur."
        )
        return

    event_id = create_event(
        interaction.guild_id,
        interaction.user.id,
        nom,
        description,
        event_dt.astimezone(timezone.utc),
        max_participants,
    )
    set_event_channels(event_id, text_channel.id, voice_channel.id)
    set_event_role(event_id, event_role.id)

    # L'organisateur participe automatiquement à son propre événement.
    add_participant(event_id, interaction.user.id)
    try:
        await interaction.user.add_roles(event_role, reason="Organisateur de l'événement")
    except discord.Forbidden:
        pass

    embed = build_event_embed(nom, description, event_dt, max_participants, 1, interaction.user.id)
    view = EventView(event_id)
    message = await interaction.followup.send(embed=embed, view=view, wait=True)

    set_event_announcement(event_id, message.channel.id, message.id)


@event_group.command(name="list", description="Liste les événements à venir sur ce serveur")
async def event_list(interaction: discord.Interaction):
    if interaction.guild_id is None:
        await interaction.response.send_message("Cette commande doit être utilisée sur un serveur.", ephemeral=True)
        return

    # On accuse réception immédiatement (avant toute requête DB) pour ne jamais
    # risquer de dépasser la fenêtre de 3 secondes imposée par Discord.
    await interaction.response.defer()

    events = get_open_events(interaction.guild_id)
    if not events:
        await interaction.followup.send("Aucun événement à venir sur ce serveur.", ephemeral=True)
        return

    lines = []
    for e in events:
        count = get_participant_count(e["id"])
        limite = f"{count}/{e['max_participants']}" if e["max_participants"] else f"{count}"
        dt_paris = e["event_datetime"].astimezone(PARIS_TZ)
        lines.append(f"📅 **{e['name']}** — {dt_paris.strftime('%d/%m/%Y à %H:%M')} — {limite} participant(s)")

    embed = discord.Embed(title="🗓️ Événements à venir", description="\n".join(lines), color=EMBED_COLOR)
    await interaction.followup.send(embed=embed)


@event_group.command(name="close", description="Clôturer un événement et supprimer ses salons")
@app_commands.describe(evenement="L'événement à clôturer")
@app_commands.autocomplete(evenement=event_autocomplete)
async def event_close(interaction: discord.Interaction, evenement: int):
    if interaction.guild is None:
        await interaction.response.send_message("Cette commande doit être utilisée sur un serveur.", ephemeral=True)
        return

    # On accuse réception immédiatement (avant toute requête DB) pour ne jamais
    # risquer de dépasser la fenêtre de 3 secondes imposée par Discord.
    await interaction.response.defer(ephemeral=True)

    event = get_event(evenement)
    if event is None or event["guild_id"] != interaction.guild_id:
        await interaction.followup.send("Événement introuvable.", ephemeral=True)
        return

    is_organizer = interaction.user.id == event["organizer_id"]
    is_admin = interaction.user.guild_permissions.manage_channels
    if not (is_organizer or is_admin):
        await interaction.followup.send(
            "Seul l'organisateur ou un membre avec la permission \"Gérer les salons\" "
            "peut clôturer cet événement.",
            ephemeral=True,
        )
        return

    close_event(evenement)
    await cleanup_event_channels(interaction.guild, event)
    await finalize_event_announcement(interaction.client, evenement, "Événement clôturé. Salons supprimés.")

    await interaction.followup.send(f"Événement **{event['name']}** clôturé, salons supprimés. ✅", ephemeral=True)


@tasks.loop(minutes=15)
async def event_cleanup_task():
    """Filet de sécurité : nettoie automatiquement les salons d'un événement non
    clôturé manuellement, un certain délai après l'heure de l'événement."""
    now_utc = datetime.now(timezone.utc)
    for event in get_all_open_events():
        if event["event_datetime"] + timedelta(hours=EVENT_AUTO_CLEANUP_HOURS) < now_utc:
            guild = bot.get_guild(event["guild_id"])
            if guild is not None:
                await cleanup_event_channels(guild, event)
            close_event(event["id"])
            await finalize_event_announcement(
                bot, event["id"], "Événement terminé (salons automatiquement supprimés)."
            )


@event_cleanup_task.before_loop
async def before_event_cleanup_task():
    await bot.wait_until_ready()


# ---------------------------------------------------------------------------
# Composants d'interface : menu déroulant, formulaires (modals), boutons
# ---------------------------------------------------------------------------

class BioModal(SafeModal, title="Modifier ma bio"):
    bio_input = discord.ui.TextInput(
        label="Ta bio",
        style=discord.TextStyle.paragraph,
        max_length=MAX_BIO_LEN,
        required=False,
        placeholder="Parle un peu de toi, tes passions, ce que tu fais ici...",
    )

    def __init__(self, guild_id: int, user_id: int):
        super().__init__()
        self.guild_id = guild_id
        self.user_id = user_id
        # Remarque : on ne pré-remplit plus le formulaire avec la bio actuelle,
        # car ça nécessiterait une requête DB avant d'afficher le modal — impossible
        # à différer (Discord exige que le modal soit la toute première réponse à
        # l'interaction) et donc risqué en cas de latence de la base.

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        update_field(self.guild_id, self.user_id, "bio", str(self.bio_input.value))
        await interaction.followup.send("Ta bio a été mise à jour ✅", ephemeral=True)


def is_valid_day_month(day: int, month: int) -> bool:
    if not (1 <= month <= 12):
        return False
    # 29 accepté pour février (année bissextile) même si on ne stocke pas l'année.
    days_in_month = [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    return 1 <= day <= days_in_month[month - 1]


class BirthdayModal(SafeModal, title="Mon anniversaire"):
    date_input = discord.ui.TextInput(
        label="Date (JJ/MM)",
        placeholder="ex: 25/12",
        max_length=5,
        required=False,
    )

    def __init__(self, guild_id: int, user_id: int):
        super().__init__()
        self.guild_id = guild_id
        self.user_id = user_id
        # Remarque : pas de pré-remplissage depuis la DB ici pour la même raison
        # que BioModal (voir plus haut) — on ne peut pas différer avant un modal.
        self.date_input.placeholder = "ex: 25/12 (laisse vide pour retirer)"

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        raw = str(self.date_input.value).strip()

        if not raw:
            clear_birthday(self.guild_id, self.user_id)
            await interaction.followup.send("Anniversaire retiré de ton profil.", ephemeral=True)
            return

        match = re.fullmatch(r"(\d{1,2})\s*/\s*(\d{1,2})", raw)
        if not match:
            await interaction.followup.send(
                "Format invalide. Utilise JJ/MM, par exemple 25/12.", ephemeral=True
            )
            return

        day, month = int(match.group(1)), int(match.group(2))
        if not is_valid_day_month(day, month):
            await interaction.followup.send("Cette date n'existe pas.", ephemeral=True)
            return

        set_birthday(self.guild_id, self.user_id, day, month)
        await interaction.followup.send(
            f"🎂 Anniversaire enregistré : **{day} {MONTHS_FR[month - 1]}**", ephemeral=True
        )


class LinkServiceModal(SafeModal):
    pseudo_input = discord.ui.TextInput(
        label="Pseudo ou URL",
        max_length=200,
        placeholder="ex: tonpseudo ou https://...",
    )

    def __init__(self, guild_id: int, user_id: int, service_key: str):
        service = LINK_SERVICES[service_key]
        super().__init__(title=f"Lien {service['label']}")
        self.guild_id = guild_id
        self.user_id = user_id
        self.service_key = service_key
        self.pseudo_input.label = f"Pseudo ou URL {service['label']}"

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        service = LINK_SERVICES[self.service_key]
        url, error = resolve_link_url(self.service_key, str(self.pseudo_input.value))
        if error:
            await interaction.followup.send(error, ephemeral=True)
            return

        ok, msg = add_link(self.guild_id, self.user_id, service["label"], url)
        if not ok:
            await interaction.followup.send(msg, ephemeral=True)
            return
        await interaction.followup.send(f"Lien **{service['label']}** enregistré ✅", ephemeral=True)


class LinkServiceSelect(discord.ui.Select):
    def __init__(self, guild_id: int, user_id: int):
        options = [
            discord.SelectOption(label=service["label"], value=key, emoji=service["emoji"])
            for key, service in LINK_SERVICES.items()
        ]
        super().__init__(placeholder="Choisis le service", options=options)
        self.guild_id = guild_id
        self.user_id = user_id

    async def callback(self, interaction: discord.Interaction):
        service_key = self.values[0]
        await interaction.response.send_modal(LinkServiceModal(self.guild_id, self.user_id, service_key))


class LinkServiceSelectView(SafeView):
    def __init__(self, guild_id: int, user_id: int):
        super().__init__(timeout=60)
        self.add_item(LinkServiceSelect(guild_id, user_id))


class LinkRemoveSelect(discord.ui.Select):
    def __init__(self, guild_id: int, user_id: int, links: list[tuple[str, str]]):
        options = [
            discord.SelectOption(label=label[:100], description=url[:100])
            for label, url in links
        ]
        super().__init__(placeholder="Choisis le lien à supprimer", options=options)
        self.guild_id = guild_id
        self.user_id = user_id

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer()
        label = self.values[0]
        remove_link(self.guild_id, self.user_id, label)
        await interaction.edit_original_response(content=f"Lien **{label}** supprimé ✅", view=None)


class LinkRemoveView(SafeView):
    def __init__(self, guild_id: int, user_id: int, links: list[tuple[str, str]]):
        super().__init__(timeout=60)
        self.add_item(LinkRemoveSelect(guild_id, user_id, links))


class ConfirmDeleteView(SafeView):
    def __init__(self, guild_id: int, user_id: int):
        super().__init__(timeout=30)
        self.guild_id = guild_id
        self.user_id = user_id

    @discord.ui.button(label="Confirmer la suppression", style=discord.ButtonStyle.danger)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        delete_profile(self.guild_id, self.user_id)
        await interaction.edit_original_response(content="🗑️ Ton profil a été supprimé.", view=None)

    @discord.ui.button(label="Annuler", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.edit_message(content="Suppression annulée.", view=None)


async def wait_for_profile_image(interaction: discord.Interaction, guild_id: int, user_id: int):
    """Attend que l'utilisateur envoie une image dans le salon, puis l'enregistre comme photo de profil."""

    def check(m: discord.Message) -> bool:
        return (
            m.author.id == user_id
            and m.channel.id == interaction.channel_id
            and len(m.attachments) > 0
        )

    try:
        msg = await bot.wait_for("message", check=check, timeout=60)
    except asyncio.TimeoutError:
        await interaction.followup.send("⏱️ Temps écoulé, aucune image reçue.", ephemeral=True)
        return

    attachment = msg.attachments[0]
    if not attachment.content_type or not attachment.content_type.startswith("image/"):
        await interaction.followup.send("Le fichier envoyé n'est pas une image.", ephemeral=True)
        return

    update_field(guild_id, user_id, "avatar_url", attachment.url)
    await interaction.followup.send("Ta photo de profil a été mise à jour ✅", ephemeral=True)

    # On nettoie le message contenant l'image pour ne pas encombrer le salon,
    # si le bot en a la permission.
    try:
        await msg.delete()
    except (discord.Forbidden, discord.NotFound):
        pass


class ColorSelect(discord.ui.Select):
    # (nom affiché, code hexadécimal, emoji d'aperçu)
    COLORS = [
        ("Blurple (par défaut)", "5865F2", "🔵"),
        ("Rouge", "ED4245", "🔴"),
        ("Vert", "57F287", "🟢"),
        ("Jaune", "FEE75C", "🟡"),
        ("Violet", "9B59B6", "🟣"),
        ("Rose", "EB459E", "🌸"),
        ("Orange", "E67E22", "🟠"),
        ("Blanc", "FFFFFF", "⚪"),
        ("Sombre", "2C2F33", "⚫"),
    ]

    def __init__(self, guild_id: int, user_id: int):
        options = [
            discord.SelectOption(label=name, value=hexcode, emoji=emoji)
            for name, hexcode, emoji in self.COLORS
        ]
        super().__init__(placeholder="Choisis une couleur", options=options)
        self.guild_id = guild_id
        self.user_id = user_id

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer()
        hexcode = self.values[0]
        update_field(self.guild_id, self.user_id, "profile_color", int(hexcode, 16))
        await interaction.edit_original_response(content="Couleur de ton profil mise à jour ✅", view=None)


class ColorSelectView(SafeView):
    def __init__(self, guild_id: int, user_id: int):
        super().__init__(timeout=60)
        self.add_item(ColorSelect(guild_id, user_id))


class ProfileEditSelect(discord.ui.Select):
    def __init__(self, guild_id: int, user_id: int):
        options = [
            discord.SelectOption(
                label="Modifier ma bio", value="bio", emoji="📝",
                description="Écrire ou changer le texte de ta bio",
            ),
            discord.SelectOption(
                label="Changer ma photo de profil", value="photo", emoji="🖼️",
                description="Uploader une nouvelle image",
            ),
            discord.SelectOption(
                label="Changer ma couleur de profil", value="couleur", emoji="🎨",
                description="Choisir la couleur de bordure de ton profil",
            ),
            discord.SelectOption(
                label="Définir mon anniversaire", value="anniversaire", emoji="🎂",
                description="Le bot annoncera le jour J (heure française)",
            ),
            discord.SelectOption(
                label="Ajouter/modifier un lien", value="lien_ajouter", emoji="🔗",
                description="Letterboxd ou AniList uniquement (max 2 liens)",
            ),
            discord.SelectOption(
                label="Supprimer un lien", value="lien_supprimer", emoji="✂️",
                description="Retirer un lien existant",
            ),
            discord.SelectOption(
                label="Supprimer mon profil", value="supprimer", emoji="❌",
                description="Effacer toutes tes informations",
            ),
        ]
        super().__init__(placeholder="Que veux-tu modifier ?", options=options, min_values=1, max_values=1)
        self.guild_id = guild_id
        self.user_id = user_id

    async def callback(self, interaction: discord.Interaction):
        choice = self.values[0]

        if choice == "bio":
            await interaction.response.send_modal(BioModal(self.guild_id, self.user_id))

        elif choice == "photo":
            await interaction.response.send_message(
                "📎 Envoie une image dans ce salon (dans les 60 secondes) pour qu'elle devienne ta photo de profil.",
                ephemeral=True,
            )
            await wait_for_profile_image(interaction, self.guild_id, self.user_id)

        elif choice == "couleur":
            view = ColorSelectView(self.guild_id, self.user_id)
            await interaction.response.send_message("Choisis une couleur pour ton profil :", view=view, ephemeral=True)

        elif choice == "anniversaire":
            await interaction.response.send_modal(BirthdayModal(self.guild_id, self.user_id))

        elif choice == "lien_ajouter":
            view = LinkServiceSelectView(self.guild_id, self.user_id)
            await interaction.response.send_message(
                "Choisis le service (ton lien Letterboxd ou AniList sera créé ou remplacé) :",
                view=view,
                ephemeral=True,
            )

        elif choice == "lien_supprimer":
            await interaction.response.defer(ephemeral=True)
            _, links = fetch_profile(self.guild_id, self.user_id)
            if not links:
                await interaction.followup.send("Tu n'as encore aucun lien à supprimer.", ephemeral=True)
                return
            view = LinkRemoveView(self.guild_id, self.user_id, links)
            await interaction.followup.send("Sélectionne le lien à supprimer :", view=view, ephemeral=True)

        elif choice == "supprimer":
            view = ConfirmDeleteView(self.guild_id, self.user_id)
            await interaction.response.send_message(
                "⚠️ Es-tu sûr de vouloir supprimer tout ton profil (bio, photo, couleur, anniversaire, liens, "
                "statistiques) ? Cette action est irréversible.",
                view=view,
                ephemeral=True,
            )


class ProfileEditView(SafeView):
    def __init__(self, guild_id: int, user_id: int):
        super().__init__(timeout=180)
        self.add_item(ProfileEditSelect(guild_id, user_id))


# ---- /profil editor -----------------------------------------------------------

@profil_group.command(name="editor", description="Ouvre un menu pour créer ou modifier ton profil")
async def profil_editor(interaction: discord.Interaction):
    view = ProfileEditView(interaction.guild_id, interaction.user.id)
    await interaction.response.send_message(
        "🛠️ **Édition de ton profil**\nChoisis ce que tu veux modifier dans le menu ci-dessous :",
        view=view,
        ephemeral=True,
    )


# ---- /profil view -----------------------------------------------------------

@profil_group.command(name="view", description="Affiche ton profil ou celui d'un autre membre")
@app_commands.describe(membre="Le membre dont tu veux voir le profil (optionnel)")
async def profil_view(interaction: discord.Interaction, membre: discord.Member = None):
    # On accuse réception immédiatement (avant toute requête DB) pour ne jamais
    # risquer de dépasser la fenêtre de 3 secondes imposée par Discord.
    await interaction.response.defer()

    target = membre or interaction.user
    row, links = fetch_profile(interaction.guild_id, target.id)

    if row is None and not links:
        if target == interaction.user:
            await interaction.edit_original_response(
                content="Tu n'as pas encore de profil. Utilise `/profil editor` pour en créer un !"
            )
        else:
            await interaction.edit_original_response(content=f"{target.display_name} n'a pas encore de profil.")
        return

    embed = build_profile_embed(target, row, links)
    await interaction.edit_original_response(embed=embed)


# ---------------------------------------------------------------------------
# Mini-jeu : Puissance 4
# ---------------------------------------------------------------------------
# Purement en mémoire (pas de sauvegarde en base) : une partie en cours est
# perdue si le bot redémarre, ce qui est un compromis raisonnable pour un jeu
# occasionnel entre deux membres.

CONNECT4_ROWS = 6
CONNECT4_COLS = 7
CONNECT4_EMPTY_EMOJI = "⚪"
CONNECT4_PLAYER_EMOJIS = {1: "🔴", 2: "🟡"}
CONNECT4_COLUMN_EMOJIS = ["1️⃣", "2️⃣", "3️⃣", "4️⃣", "5️⃣", "6️⃣", "7️⃣"]


class Connect4Game:
    def __init__(self, player1: discord.Member, player2: discord.Member):
        self.players = {1: player1, 2: player2}
        self.board = [[0] * CONNECT4_COLS for _ in range(CONNECT4_ROWS)]
        self.current = 1
        self.winner = None  # None en cours, 1 ou 2 si victoire, "draw" si match nul

    def drop(self, col: int) -> int | None:
        """Place un jeton dans la colonne pour le joueur courant.
        Renvoie la ligne où il atterrit, ou None si la colonne est pleine."""
        for row in range(CONNECT4_ROWS - 1, -1, -1):
            if self.board[row][col] == 0:
                self.board[row][col] = self.current
                return row
        return None

    def check_winner_at(self, row: int, col: int) -> bool:
        player = self.board[row][col]
        for dr, dc in ((0, 1), (1, 0), (1, 1), (1, -1)):
            count = 1
            for sign in (1, -1):
                r, c = row + dr * sign, col + dc * sign
                while 0 <= r < CONNECT4_ROWS and 0 <= c < CONNECT4_COLS and self.board[r][c] == player:
                    count += 1
                    r += dr * sign
                    c += dc * sign
            if count >= 4:
                return True
        return False

    def is_full(self) -> bool:
        return all(self.board[0][c] != 0 for c in range(CONNECT4_COLS))

    def render(self) -> str:
        header = "".join(CONNECT4_COLUMN_EMOJIS)
        rows_txt = "\n".join(
            "".join(CONNECT4_PLAYER_EMOJIS.get(cell, CONNECT4_EMPTY_EMOJI) for cell in row)
            for row in self.board
        )
        return f"{header}\n{rows_txt}"


def build_connect4_embed(game: Connect4Game) -> discord.Embed:
    if game.winner == "draw":
        title = "🤝 Match nul !"
        color = 0x99AAB5
    elif game.winner:
        title = f"🏆 {game.players[game.winner].display_name} remporte la partie !"
        color = EMBED_COLOR
    else:
        title = f"Au tour de {game.players[game.current].display_name} ({CONNECT4_PLAYER_EMOJIS[game.current]})"
        color = EMBED_COLOR

    embed = discord.Embed(title=title, description=game.render(), color=color)
    embed.add_field(
        name="Joueurs",
        value=(
            f"{CONNECT4_PLAYER_EMOJIS[1]} {game.players[1].mention}\n"
            f"{CONNECT4_PLAYER_EMOJIS[2]} {game.players[2].mention}"
        ),
        inline=False,
    )
    return embed


class Connect4View(SafeView):
    def __init__(self, game: Connect4Game):
        super().__init__(timeout=600)  # partie abandonnée après 10 min d'inactivité
        self.game = game
        self.message: discord.Message | None = None

        for col in range(CONNECT4_COLS):
            button = discord.ui.Button(
                label=str(col + 1), style=discord.ButtonStyle.primary, row=col // 4
            )
            button.callback = self._make_callback(col)
            self.add_item(button)

    def _make_callback(self, col: int):
        async def callback(interaction: discord.Interaction):
            await self.handle_move(interaction, col)
        return callback

    async def handle_move(self, interaction: discord.Interaction, col: int):
        game = self.game
        player_ids = (game.players[1].id, game.players[2].id)

        if interaction.user.id not in player_ids:
            await interaction.response.send_message("Cette partie ne te concerne pas.", ephemeral=True)
            return

        if interaction.user.id != game.players[game.current].id:
            await interaction.response.send_message("Ce n'est pas ton tour !", ephemeral=True)
            return

        row = game.drop(col)
        if row is None:
            await interaction.response.send_message("Cette colonne est pleine, choisis-en une autre.", ephemeral=True)
            return

        if game.check_winner_at(row, col):
            game.winner = game.current
            self._disable_all()
        elif game.is_full():
            game.winner = "draw"
            self._disable_all()
        else:
            game.current = 2 if game.current == 1 else 1

        embed = build_connect4_embed(game)
        await interaction.response.edit_message(embed=embed, view=self)
        if game.winner:
            self.stop()

    def _disable_all(self):
        for item in self.children:
            item.disabled = True

    async def on_timeout(self):
        self._disable_all()
        if self.message is not None:
            try:
                await self.message.edit(view=self)
            except (discord.NotFound, discord.Forbidden):
                pass


@bot.tree.command(name="puissance4", description="Défier un membre à une partie de Puissance 4")
@app_commands.describe(adversaire="Le membre que tu veux défier")
async def puissance4(interaction: discord.Interaction, adversaire: discord.Member):
    if adversaire.id == interaction.user.id:
        await interaction.response.send_message("Tu ne peux pas te défier toi-même !", ephemeral=True)
        return
    if adversaire.bot:
        await interaction.response.send_message("Tu ne peux pas défier un bot pour l'instant.", ephemeral=True)
        return

    game = Connect4Game(interaction.user, adversaire)
    view = Connect4View(game)
    embed = build_connect4_embed(game)

    await interaction.response.send_message(
        content=f"🔴🟡 {adversaire.mention}, tu es défié à une partie de Puissance 4 par {interaction.user.mention} !",
        embed=embed,
        view=view,
    )
    view.message = await interaction.original_response()


# ---------------------------------------------------------------------------
# Menus de sélection de rôles ("Loadout") : embeds, vues interactives, commandes
# ---------------------------------------------------------------------------

def build_rolemenu_home_embed(menu: dict) -> discord.Embed:
    return discord.Embed(
        title=f"🎯 {menu['name']}",
        description="Choisis une catégorie pour voir les rôles disponibles.",
        color=EMBED_COLOR,
    )


def build_rolemenu_category_embed(
    category: dict, roles: list[dict], member: discord.Member
) -> discord.Embed:
    owned_ids = {r.id for r in member.roles}
    lines = []
    for r in roles:
        has_it = r["discord_role_id"] in owned_ids
        count = get_role_assignment_count(r["id"])
        marker = "✅" if has_it else "⬜"
        emoji = f"{r['emoji']} " if r["emoji"] else ""
        desc = f" — {r['description']}" if r["description"] else ""
        lines.append(f"{marker} {emoji}**{r['label']}**{desc} · {count} membre(s)")

    embed = discord.Embed(
        title=f"{category['emoji'] + ' ' if category['emoji'] else ''}{category['label']}",
        description="\n".join(lines) if lines else "*Aucun rôle configuré dans cette catégorie.*",
        color=EMBED_COLOR,
    )
    mode = "Un seul choix actif à la fois" if category["exclusive"] else "Plusieurs choix cumulables"
    embed.set_footer(text=f"{mode} • Clique sur un bouton pour équiper/retirer un rôle")
    return embed


async def show_rolemenu_home(interaction: discord.Interaction, menu_id: int, first_open: bool = False):
    """Affiche l'accueil du menu (liste des catégories).
    L'appelant doit avoir déjà déferé la réponse à l'interaction avant d'appeler
    cette fonction (via interaction.response.defer()), pour ne jamais risquer de
    dépasser la fenêtre de 3 secondes de Discord avec les requêtes à la base."""
    menu = get_role_menu(menu_id)
    if menu is None:
        content = "Ce menu de rôles n'existe plus."
        if first_open:
            await interaction.followup.send(content, ephemeral=True)
        else:
            await interaction.edit_original_response(content=content, embed=None, view=None)
        return

    categories = get_role_menu_categories(menu_id)
    if not categories:
        content = "Ce menu n'a pas encore de catégories configurées."
        if first_open:
            await interaction.followup.send(content, ephemeral=True)
        else:
            await interaction.edit_original_response(content=content, embed=None, view=None)
        return

    embed = build_rolemenu_home_embed(menu)
    view = RoleMenuCategoryView(menu_id, categories)

    if first_open:
        await interaction.followup.send(embed=embed, view=view, ephemeral=True)
    else:
        await interaction.edit_original_response(content=None, embed=embed, view=view)


async def show_role_category(interaction: discord.Interaction, menu_id: int, category_id: int):
    """L'appelant doit avoir déjà déferé la réponse à l'interaction (voir show_rolemenu_home)."""
    category = get_role_menu_category(category_id)
    if category is None:
        await interaction.edit_original_response(content="Cette catégorie n'existe plus.", embed=None, view=None)
        return

    roles = get_role_menu_roles(category_id)
    embed = build_rolemenu_category_embed(category, roles, interaction.user)
    view = RoleMenuRoleView(menu_id, category_id, roles, interaction.user)
    await interaction.edit_original_response(embed=embed, view=view)


async def handle_role_toggle(interaction: discord.Interaction, menu_id: int, category_id: int, role_entry_id: int):
    """L'appelant doit avoir déjà déferé la réponse à l'interaction (voir show_rolemenu_home)."""
    role_entry = get_role_menu_role(role_entry_id)
    category = get_role_menu_category(category_id)
    if role_entry is None or category is None:
        await interaction.edit_original_response(content="Ce rôle n'est plus configuré.", embed=None, view=None)
        return

    guild = interaction.guild
    discord_role = guild.get_role(role_entry["discord_role_id"])
    if discord_role is None:
        await interaction.edit_original_response(
            content="Le rôle Discord associé n'existe plus, préviens un admin.", embed=None, view=None
        )
        return

    member = interaction.user
    has_it = discord_role in member.roles

    try:
        if has_it:
            await member.remove_roles(discord_role, reason="Retiré via le menu de rôles")
            remove_role_assignment(role_entry_id, member.id)
        else:
            # Dans une catégorie exclusive, un seul rôle peut être actif à la fois :
            # on retire d'abord les autres rôles déjà équipés dans cette catégorie.
            if category["exclusive"]:
                for other in get_role_menu_roles(category_id):
                    if other["id"] == role_entry_id:
                        continue
                    other_role = guild.get_role(other["discord_role_id"])
                    if other_role and other_role in member.roles:
                        await member.remove_roles(other_role, reason="Choix exclusif dans le menu de rôles")
                        remove_role_assignment(other["id"], member.id)

            await member.add_roles(discord_role, reason="Ajouté via le menu de rôles")
            add_role_assignment(role_entry_id, member.id)
    except discord.Forbidden:
        await interaction.edit_original_response(
            content=(
                "Je n'ai pas la permission de gérer ce rôle. Vérifie que mon propre rôle est bien "
                "placé au-dessus dans la hiérarchie des rôles du serveur."
            ),
            embed=None,
            view=None,
        )
        return

    # On redessine le panneau de la catégorie avec les nouveaux états.
    await show_role_category(interaction, menu_id, category_id)


class RoleMenuCategorySelect(discord.ui.Select):
    def __init__(self, menu_id: int, categories: list[dict]):
        options = [
            discord.SelectOption(label=c["label"][:100], value=str(c["id"]), emoji=c["emoji"] or None)
            for c in categories
        ]
        super().__init__(placeholder="Choisis une catégorie...", options=options)
        self.menu_id = menu_id

    async def callback(self, interaction: discord.Interaction):
        # On accuse réception immédiatement (avant toute requête DB) pour ne
        # jamais risquer de dépasser la fenêtre de 3 secondes imposée par Discord.
        await interaction.response.defer()
        await show_role_category(interaction, self.menu_id, int(self.values[0]))


class RoleMenuCategoryView(SafeView):
    def __init__(self, menu_id: int, categories: list[dict]):
        super().__init__(timeout=300)
        self.add_item(RoleMenuCategorySelect(menu_id, categories))


class RoleToggleButton(discord.ui.Button):
    def __init__(self, menu_id: int, category_id: int, role_entry: dict, owned: bool):
        super().__init__(
            label=role_entry["label"][:80],
            emoji=role_entry["emoji"] or None,
            style=discord.ButtonStyle.success if owned else discord.ButtonStyle.secondary,
        )
        self.menu_id = menu_id
        self.category_id = category_id
        self.role_entry_id = role_entry["id"]

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer()
        await handle_role_toggle(interaction, self.menu_id, self.category_id, self.role_entry_id)


class RoleMenuBackButton(discord.ui.Button):
    def __init__(self, menu_id: int):
        super().__init__(label="◀ Retour aux catégories", style=discord.ButtonStyle.secondary, row=4)
        self.menu_id = menu_id

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer()
        await show_rolemenu_home(interaction, self.menu_id)


class RoleMenuRoleView(SafeView):
    def __init__(self, menu_id: int, category_id: int, roles: list[dict], member: discord.Member):
        super().__init__(timeout=300)
        owned_ids = {r.id for r in member.roles}
        for role_entry in roles:
            owned = role_entry["discord_role_id"] in owned_ids
            self.add_item(RoleToggleButton(menu_id, category_id, role_entry, owned))
        self.add_item(RoleMenuBackButton(menu_id))


class RoleMenuOpenView(SafeView):
    """Vue persistante (timeout=None) attachée au message public d'un menu de rôles.
    Réenregistrée à chaque démarrage du bot pour tous les menus déjà publiés."""

    def __init__(self, menu_id: int):
        super().__init__(timeout=None)
        button = discord.ui.Button(
            label="Ouvrir mon inventaire", emoji="🎯", style=discord.ButtonStyle.primary,
            custom_id=f"rolemenu_open:{menu_id}",
        )
        button.callback = self.open_callback
        self.add_item(button)
        self.menu_id = menu_id

    async def open_callback(self, interaction: discord.Interaction):
        # On accuse réception immédiatement (avant toute requête DB) pour ne
        # jamais risquer de dépasser la fenêtre de 3 secondes imposée par Discord
        # (ex: si la base Postgres met du temps à répondre après une mise en veille).
        await interaction.response.defer(ephemeral=True, thinking=True)
        await show_rolemenu_home(interaction, self.menu_id, first_open=True)


role_menu_group = app_commands.Group(
    name="rolemenu", description="Configurer des menus de sélection de rôles interactifs"
)


async def rolemenu_autocomplete(interaction: discord.Interaction, current: str):
    if interaction.guild_id is None:
        return []
    choices = []
    for m in get_role_menus(interaction.guild_id):
        if current.lower() in m["name"].lower():
            choices.append(app_commands.Choice(name=m["name"][:100], value=m["id"]))
    return choices[:25]


async def rolemenu_category_autocomplete(interaction: discord.Interaction, current: str):
    if interaction.guild_id is None:
        return []
    choices = []
    for c in get_role_menu_categories_for_guild(interaction.guild_id):
        label = f"{c['label']} (menu #{c['menu_id']})"
        if current.lower() in label.lower():
            choices.append(app_commands.Choice(name=label[:100], value=c["id"]))
    return choices[:25]


@role_menu_group.command(name="creer", description="Créer un nouveau menu de sélection de rôles")
@app_commands.describe(nom="Nom du menu (affiché en titre)")
async def rolemenu_creer(interaction: discord.Interaction, nom: str):
    if interaction.guild is None:
        await interaction.response.send_message("Cette commande doit être utilisée sur un serveur.", ephemeral=True)
        return
    if not interaction.user.guild_permissions.manage_roles:
        await interaction.response.send_message(
            "Il te faut la permission \"Gérer les rôles\" pour configurer un menu.", ephemeral=True
        )
        return

    # On accuse réception immédiatement (avant toute requête DB) pour ne jamais
    # risquer de dépasser la fenêtre de 3 secondes imposée par Discord.
    await interaction.response.defer(ephemeral=True)

    menu_id = create_role_menu(interaction.guild_id, nom, interaction.user.id)
    await interaction.followup.send(
        f"Menu **{nom}** créé (id `{menu_id}`). Ajoute des catégories avec `/rolemenu categorie-ajouter`.",
        ephemeral=True,
    )


@role_menu_group.command(name="categorie-ajouter", description="Ajouter une catégorie à un menu de rôles")
@app_commands.describe(
    menu="Le menu auquel ajouter la catégorie",
    nom="Nom de la catégorie",
    exclusif="Un seul rôle actif à la fois dans cette catégorie (ex: langue) ?",
    emoji="Emoji affiché pour cette catégorie (optionnel)",
)
@app_commands.autocomplete(menu=rolemenu_autocomplete)
async def rolemenu_categorie_ajouter(
    interaction: discord.Interaction, menu: int, nom: str, exclusif: bool, emoji: str = None
):
    if not interaction.user.guild_permissions.manage_roles:
        await interaction.response.send_message(
            "Il te faut la permission \"Gérer les rôles\" pour configurer un menu.", ephemeral=True
        )
        return

    await interaction.response.defer(ephemeral=True)

    menu_row = get_role_menu(menu)
    if menu_row is None or menu_row["guild_id"] != interaction.guild_id:
        await interaction.followup.send("Menu introuvable.", ephemeral=True)
        return

    category_id = add_role_menu_category(menu, nom, emoji, exclusif)
    await interaction.followup.send(
        f"Catégorie **{nom}** ajoutée (id `{category_id}`). Ajoute des rôles avec `/rolemenu role-ajouter`.",
        ephemeral=True,
    )


@role_menu_group.command(name="role-ajouter", description="Ajouter un rôle Discord à une catégorie d'un menu")
@app_commands.describe(
    categorie="La catégorie à laquelle ajouter ce rôle",
    role="Le rôle Discord à distribuer",
    emoji="Emoji affiché pour ce rôle (optionnel)",
    description="Courte description affichée à côté du rôle (optionnel)",
)
@app_commands.autocomplete(categorie=rolemenu_category_autocomplete)
async def rolemenu_role_ajouter(
    interaction: discord.Interaction,
    categorie: int,
    role: discord.Role,
    emoji: str = None,
    description: str = None,
):
    if not interaction.user.guild_permissions.manage_roles:
        await interaction.response.send_message(
            "Il te faut la permission \"Gérer les rôles\" pour configurer un menu.", ephemeral=True
        )
        return

    await interaction.response.defer(ephemeral=True)

    category_row = get_role_menu_category(categorie)
    if category_row is None:
        await interaction.followup.send("Catégorie introuvable.", ephemeral=True)
        return

    warning = ""
    if role >= interaction.guild.me.top_role:
        warning = (
            "\n⚠️ Ce rôle est placé au-dessus (ou au même niveau que) mon propre rôle dans la "
            "hiérarchie du serveur : je ne pourrai pas l'attribuer tant que ce ne sera pas corrigé "
            "(Paramètres du serveur → Rôles, fais glisser mon rôle au-dessus)."
        )

    role_entry_id = add_role_menu_role(categorie, role.id, role.name, emoji, description)
    await interaction.followup.send(
        f"Rôle **{role.name}** ajouté à la catégorie (id `{role_entry_id}`).{warning}", ephemeral=True
    )


@role_menu_group.command(name="apercu", description="Aperçu de la structure d'un menu avant publication")
@app_commands.describe(menu="Le menu à prévisualiser")
@app_commands.autocomplete(menu=rolemenu_autocomplete)
async def rolemenu_apercu(interaction: discord.Interaction, menu: int):
    await interaction.response.defer(ephemeral=True)

    menu_row = get_role_menu(menu)
    if menu_row is None or menu_row["guild_id"] != interaction.guild_id:
        await interaction.followup.send("Menu introuvable.", ephemeral=True)
        return

    categories = get_role_menu_categories(menu)
    if not categories:
        await interaction.followup.send(
            f"Le menu **{menu_row['name']}** n'a encore aucune catégorie.", ephemeral=True
        )
        return

    lines = []
    for c in categories:
        mode = "exclusif" if c["exclusive"] else "multiple"
        lines.append(f"\n**{(c['emoji'] + ' ') if c['emoji'] else ''}{c['label']}** ({mode})")
        roles = get_role_menu_roles(c["id"])
        if not roles:
            lines.append("　_Aucun rôle configuré._")
        for r in roles:
            desc = f" — {r['description']}" if r["description"] else ""
            lines.append(f"　{(r['emoji'] + ' ') if r['emoji'] else ''}{r['label']}{desc}")

    embed = discord.Embed(title=f"Aperçu — {menu_row['name']}", description="\n".join(lines), color=EMBED_COLOR)
    await interaction.followup.send(embed=embed, ephemeral=True)


@role_menu_group.command(name="publier", description="Publier le menu de rôles dans ce salon")
@app_commands.describe(menu="Le menu à publier")
@app_commands.autocomplete(menu=rolemenu_autocomplete)
async def rolemenu_publier(interaction: discord.Interaction, menu: int):
    if not interaction.user.guild_permissions.manage_roles:
        await interaction.response.send_message(
            "Il te faut la permission \"Gérer les rôles\" pour publier un menu.", ephemeral=True
        )
        return

    await interaction.response.defer(ephemeral=True)

    menu_row = get_role_menu(menu)
    if menu_row is None or menu_row["guild_id"] != interaction.guild_id:
        await interaction.followup.send("Menu introuvable.", ephemeral=True)
        return

    categories = get_role_menu_categories(menu)
    if not categories:
        await interaction.followup.send("Ajoute au moins une catégorie avant de publier ce menu.", ephemeral=True)
        return

    embed = discord.Embed(
        title=f"🎯 {menu_row['name']}",
        description=(
            "Clique sur le bouton ci-dessous pour ouvrir ton inventaire de rôles personnel.\n"
            "Personne d'autre ne verra ton panneau — choisis librement !"
        ),
        color=EMBED_COLOR,
    )
    view = RoleMenuOpenView(menu)
    message = await interaction.channel.send(embed=embed, view=view)
    set_role_menu_message(menu, interaction.channel.id, message.id)

    await interaction.followup.send("Menu publié ✅", ephemeral=True)


bot.tree.add_command(profil_group)
bot.tree.add_command(event_group)
bot.tree.add_command(role_menu_group)


@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    # Filet de sécurité final pour toutes les commandes slash (/profil, /event,
    # /rolemenu, /puissance4) : si une exception survient après un defer(), elle
    # est rattrapée ici plutôt que de laisser l'interaction bloquée indéfiniment.
    await _report_interaction_error(interaction, error)


if __name__ == "__main__":
    if not TOKEN:
        raise SystemExit(
            "Erreur : la variable d'environnement DISCORD_TOKEN n'est pas définie.\n"
            "Copie .env.example en .env et renseigne ton token de bot."
        )
    if not DATABASE_URL:
        raise SystemExit(
            "Erreur : la variable d'environnement DATABASE_URL n'est pas définie.\n"
            "Crée une base Postgres gratuite (ex: Neon.tech) et colle son URL de connexion "
            "dans le fichier .env. Voir le README pour le guide pas à pas."
        )
    bot.run(TOKEN)
