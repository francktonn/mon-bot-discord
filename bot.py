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
                announcement_channel_id BIGINT,
                announcement_message_id BIGINT,
                status TEXT NOT NULL DEFAULT 'open',
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )
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

# Empêche de réenregistrer plusieurs fois les vues persistantes des événements
# si on_ready se déclenche plusieurs fois (reconnexions Discord).
_event_views_registered = False


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

    # Réenregistre les boutons "Participer"/"Se désister" de tous les événements
    # encore ouverts, pour qu'ils continuent de fonctionner après un redémarrage
    # ou un redéploiement du bot (les vues persistantes ne survivent pas d'elles-mêmes).
    global _event_views_registered
    if not _event_views_registered:
        _event_views_registered = True
        for event in get_all_open_events():
            bot.add_view(EventView(event["id"]))

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


async def create_event_channels(
    guild: discord.Guild, event_name: str, organizer: discord.Member
) -> tuple[discord.TextChannel, discord.VoiceChannel]:
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
        organizer: discord.PermissionOverwrite(view_channel=True, send_messages=True, connect=True, speak=True),
    }

    text_channel = await guild.create_text_channel(
        f"💬-{safe_name}", category=category, overwrites=overwrites,
        reason=f"Événement créé par {organizer}",
    )
    voice_channel = await guild.create_voice_channel(
        f"🔊 {event_name}"[:100], category=category, overwrites=overwrites,
        reason=f"Événement créé par {organizer}",
    )
    return text_channel, voice_channel


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
    event = get_event(event_id)
    if event is None:
        await interaction.response.send_message("Cet événement n'existe plus.", ephemeral=True)
        return

    ok, msg = add_participant(event_id, interaction.user.id)
    if not ok:
        await interaction.response.send_message(msg, ephemeral=True)
        return

    guild = interaction.guild
    text_channel = guild.get_channel(event["text_channel_id"]) if event["text_channel_id"] else None
    voice_channel = guild.get_channel(event["voice_channel_id"]) if event["voice_channel_id"] else None

    try:
        if text_channel:
            await text_channel.set_permissions(interaction.user, view_channel=True, send_messages=True)
        if voice_channel:
            await voice_channel.set_permissions(interaction.user, view_channel=True, connect=True, speak=True)
    except discord.Forbidden:
        pass

    lien = f" Rendez-vous sur {text_channel.mention}." if text_channel else ""
    await interaction.response.send_message(
        f"Tu participes maintenant à **{event['name']}** !{lien}", ephemeral=True
    )
    await refresh_event_announcement(interaction.client, event_id)


async def handle_event_leave(interaction: discord.Interaction, event_id: int):
    event = get_event(event_id)
    if event is None:
        await interaction.response.send_message("Cet événement n'existe plus.", ephemeral=True)
        return

    removed = remove_participant(event_id, interaction.user.id)
    if not removed:
        await interaction.response.send_message("Tu ne participais pas à cet événement.", ephemeral=True)
        return

    guild = interaction.guild
    text_channel = guild.get_channel(event["text_channel_id"]) if event["text_channel_id"] else None
    voice_channel = guild.get_channel(event["voice_channel_id"]) if event["voice_channel_id"] else None

    try:
        if text_channel:
            await text_channel.set_permissions(interaction.user, overwrite=None)
        if voice_channel:
            await voice_channel.set_permissions(interaction.user, overwrite=None)
            # Si la personne est déjà connectée au salon vocal de l'événement, on la déconnecte
            # (le retrait de la permission n'éjecte pas automatiquement quelqu'un déjà connecté).
            member = guild.get_member(interaction.user.id)
            if member and member.voice and member.voice.channel and member.voice.channel.id == voice_channel.id:
                await member.move_to(None)
    except discord.Forbidden:
        pass

    await interaction.response.send_message(f"Tu ne participes plus à **{event['name']}**.", ephemeral=True)
    await refresh_event_announcement(interaction.client, event_id)


class EventView(discord.ui.View):
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
        await handle_event_join(interaction, self.event_id)

    async def leave_callback(self, interaction: discord.Interaction):
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
        text_channel, voice_channel = await create_event_channels(interaction.guild, nom, interaction.user)
    except discord.Forbidden:
        await interaction.followup.send(
            "Je n'ai pas la permission de créer des salons sur ce serveur. "
            "Vérifie que j'ai la permission \"Gérer les salons\"."
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

    # L'organisateur participe automatiquement à son propre événement.
    add_participant(event_id, interaction.user.id)

    embed = build_event_embed(nom, description, event_dt, max_participants, 1, interaction.user.id)
    view = EventView(event_id)
    message = await interaction.followup.send(embed=embed, view=view, wait=True)

    set_event_announcement(event_id, message.channel.id, message.id)


@event_group.command(name="list", description="Liste les événements à venir sur ce serveur")
async def event_list(interaction: discord.Interaction):
    if interaction.guild_id is None:
        await interaction.response.send_message("Cette commande doit être utilisée sur un serveur.", ephemeral=True)
        return

    events = get_open_events(interaction.guild_id)
    if not events:
        await interaction.response.send_message("Aucun événement à venir sur ce serveur.", ephemeral=True)
        return

    lines = []
    for e in events:
        count = get_participant_count(e["id"])
        limite = f"{count}/{e['max_participants']}" if e["max_participants"] else f"{count}"
        dt_paris = e["event_datetime"].astimezone(PARIS_TZ)
        lines.append(f"📅 **{e['name']}** — {dt_paris.strftime('%d/%m/%Y à %H:%M')} — {limite} participant(s)")

    embed = discord.Embed(title="🗓️ Événements à venir", description="\n".join(lines), color=EMBED_COLOR)
    await interaction.response.send_message(embed=embed)


@event_group.command(name="close", description="Clôturer un événement et supprimer ses salons")
@app_commands.describe(evenement="L'événement à clôturer")
@app_commands.autocomplete(evenement=event_autocomplete)
async def event_close(interaction: discord.Interaction, evenement: int):
    if interaction.guild is None:
        await interaction.response.send_message("Cette commande doit être utilisée sur un serveur.", ephemeral=True)
        return

    event = get_event(evenement)
    if event is None or event["guild_id"] != interaction.guild_id:
        await interaction.response.send_message("Événement introuvable.", ephemeral=True)
        return

    is_organizer = interaction.user.id == event["organizer_id"]
    is_admin = interaction.user.guild_permissions.manage_channels
    if not (is_organizer or is_admin):
        await interaction.response.send_message(
            "Seul l'organisateur ou un membre avec la permission \"Gérer les salons\" "
            "peut clôturer cet événement.",
            ephemeral=True,
        )
        return

    await interaction.response.defer(ephemeral=True)

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

class BioModal(discord.ui.Modal, title="Modifier ma bio"):
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
        # Pré-remplit le formulaire avec la bio actuelle si elle existe
        row, _ = fetch_profile(guild_id, user_id)
        if row and row[0]:
            self.bio_input.default = row[0]

    async def on_submit(self, interaction: discord.Interaction):
        update_field(self.guild_id, self.user_id, "bio", str(self.bio_input.value))
        await interaction.response.send_message("Ta bio a été mise à jour ✅", ephemeral=True)


def is_valid_day_month(day: int, month: int) -> bool:
    if not (1 <= month <= 12):
        return False
    # 29 accepté pour février (année bissextile) même si on ne stocke pas l'année.
    days_in_month = [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    return 1 <= day <= days_in_month[month - 1]


class BirthdayModal(discord.ui.Modal, title="Mon anniversaire"):
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
        row, _ = fetch_profile(guild_id, user_id)
        if row and row[5] and row[6]:
            self.date_input.default = f"{row[5]:02d}/{row[6]:02d}"
        self.date_input.placeholder = "ex: 25/12 (laisse vide pour retirer)"

    async def on_submit(self, interaction: discord.Interaction):
        raw = str(self.date_input.value).strip()

        if not raw:
            clear_birthday(self.guild_id, self.user_id)
            await interaction.response.send_message("Anniversaire retiré de ton profil.", ephemeral=True)
            return

        match = re.fullmatch(r"(\d{1,2})\s*/\s*(\d{1,2})", raw)
        if not match:
            await interaction.response.send_message(
                "Format invalide. Utilise JJ/MM, par exemple 25/12.", ephemeral=True
            )
            return

        day, month = int(match.group(1)), int(match.group(2))
        if not is_valid_day_month(day, month):
            await interaction.response.send_message("Cette date n'existe pas.", ephemeral=True)
            return

        set_birthday(self.guild_id, self.user_id, day, month)
        await interaction.response.send_message(
            f"🎂 Anniversaire enregistré : **{day} {MONTHS_FR[month - 1]}**", ephemeral=True
        )


class LinkServiceModal(discord.ui.Modal):
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
        service = LINK_SERVICES[self.service_key]
        url, error = resolve_link_url(self.service_key, str(self.pseudo_input.value))
        if error:
            await interaction.response.send_message(error, ephemeral=True)
            return

        ok, msg = add_link(self.guild_id, self.user_id, service["label"], url)
        if not ok:
            await interaction.response.send_message(msg, ephemeral=True)
            return
        await interaction.response.send_message(f"Lien **{service['label']}** enregistré ✅", ephemeral=True)


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


class LinkServiceSelectView(discord.ui.View):
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
        label = self.values[0]
        remove_link(self.guild_id, self.user_id, label)
        await interaction.response.edit_message(content=f"Lien **{label}** supprimé ✅", view=None)


class LinkRemoveView(discord.ui.View):
    def __init__(self, guild_id: int, user_id: int, links: list[tuple[str, str]]):
        super().__init__(timeout=60)
        self.add_item(LinkRemoveSelect(guild_id, user_id, links))


class ConfirmDeleteView(discord.ui.View):
    def __init__(self, guild_id: int, user_id: int):
        super().__init__(timeout=30)
        self.guild_id = guild_id
        self.user_id = user_id

    @discord.ui.button(label="Confirmer la suppression", style=discord.ButtonStyle.danger)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        delete_profile(self.guild_id, self.user_id)
        await interaction.response.edit_message(content="🗑️ Ton profil a été supprimé.", view=None)

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
        hexcode = self.values[0]
        update_field(self.guild_id, self.user_id, "profile_color", int(hexcode, 16))
        await interaction.response.edit_message(content="Couleur de ton profil mise à jour ✅", view=None)


class ColorSelectView(discord.ui.View):
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
            _, links = fetch_profile(self.guild_id, self.user_id)
            if not links:
                await interaction.response.send_message("Tu n'as encore aucun lien à supprimer.", ephemeral=True)
                return
            view = LinkRemoveView(self.guild_id, self.user_id, links)
            await interaction.response.send_message("Sélectionne le lien à supprimer :", view=view, ephemeral=True)

        elif choice == "supprimer":
            view = ConfirmDeleteView(self.guild_id, self.user_id)
            await interaction.response.send_message(
                "⚠️ Es-tu sûr de vouloir supprimer tout ton profil (bio, photo, couleur, anniversaire, liens, "
                "statistiques) ? Cette action est irréversible.",
                view=view,
                ephemeral=True,
            )


class ProfileEditView(discord.ui.View):
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
    target = membre or interaction.user
    row, links = fetch_profile(interaction.guild_id, target.id)

    if row is None and not links:
        if target == interaction.user:
            await interaction.response.send_message(
                "Tu n'as pas encore de profil. Utilise `/profil editor` pour en créer un !",
                ephemeral=True,
            )
        else:
            await interaction.response.send_message(
                f"{target.display_name} n'a pas encore de profil.", ephemeral=True
            )
        return

    embed = build_profile_embed(target, row, links)
    await interaction.response.send_message(embed=embed)


bot.tree.add_command(profil_group)
bot.tree.add_command(event_group)


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
