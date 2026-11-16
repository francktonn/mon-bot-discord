"""
Bot Discord de Bienvenue
========================

Poste un message de bienvenue personnalisable quand un nouveau membre
rejoint le serveur, avec un bouton "🎉 Bienvenue fami" : au premier clic,
un autocollant (choisi au hasard parmi ceux configurés par un admin) est
envoyé dans un nouveau message qui mentionne le nouveau membre.

Commandes slash (/) :
    /bienvenue salon             -> définit le salon où poster les messages de bienvenue
    /bienvenue message           -> personnalise le texte (utilise {membre} et {serveur})
    /bienvenue sticker-ajouter   -> ajoute un autocollant du serveur au tirage
    /bienvenue sticker-retirer   -> retire un autocollant du tirage
    /bienvenue apercu            -> prévisualise la configuration actuelle

Annonce du Cinédimanche (jour et heure au choix, heure de Paris ; dimanche 12h par défaut) :
    /cine salon                  -> salon de l'annonce + jour et heure (optionnels)
    /cine film                   -> définit le film prévu ce soir
    /cine voir                   -> affiche le film prévu et la configuration
    /cine effacer                -> efface le film prévu
    /cine annoncer               -> envoie l'annonce tout de suite (test)

Les données sont stockées dans une base PostgreSQL externe (Neon, Supabase, ou
tout autre Postgres compatible), configurée via la variable d'environnement
DATABASE_URL. Contrairement à un fichier SQLite local, cette base survit aux
redéploiements et redémarrages, même sur les hébergeurs sans disque persistant
(comme Render en plan gratuit). Chaque serveur a sa propre configuration.
"""

import os
import re
import json
import hmac
import sys
import random
import asyncio
import traceback
from contextlib import contextmanager
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import aiohttp
import discord
import psycopg2
import psycopg2.extras
from discord import app_commands
from discord.ext import commands, tasks
from dotenv import load_dotenv
from aiohttp import web  # (aiohttp est aussi importé en entier ci-dessus pour TMDB)

load_dotenv()

# Sans ça, les print() peuvent rester bloqués dans un tampon interne quand la
# sortie standard n'est pas un vrai terminal (c'est le cas sur Render et la
# plupart des hébergeurs) : le code s'exécute normalement, mais les logs
# n'apparaissent pas tout de suite (voire pas du tout avant un redémarrage).
sys.stdout.reconfigure(line_buffering=True)

TOKEN = os.getenv("DISCORD_TOKEN")
# URL de connexion à la base Postgres (fournie par Neon, Supabase, Render Postgres...).
DATABASE_URL = os.getenv("DATABASE_URL")
EMBED_COLOR = 0x5865F2  # blurple

DEFAULT_WELCOME_TEMPLATE = "Bienvenue {membre} sur **{serveur}** ! 🎉"

# Annonce du Cinédimanche : le jour et l'heure se règlent avec /cine salon
# (heure de Paris). Par défaut : dimanche 12h00. Si le bot était éteint ou en
# veille à l'heure pile, l'annonce part dès son retour, pendant CINE_WINDOW_HOURS.
PARIS = ZoneInfo("Europe/Paris")
CINE_DEFAULT_DAY = 6      # 0 = lundi ... 6 = dimanche
CINE_DEFAULT_HOUR = 12
CINE_DEFAULT_MINUTE = 0
CINE_WINDOW_HOURS = 6
JOURS = ["lundi", "mardi", "mercredi", "jeudi", "vendredi", "samedi", "dimanche"]
TMDB_API_KEY = os.getenv("TMDB_API_KEY")  # facultatif : affiche du film
# Mot de passe partagé avec la page de la roue : elle l'envoie avec le film choisi
# au vote (POST /cine). Sans cette variable, l'envoi depuis la page est désactivé.
CINE_WEBHOOK_SECRET = os.getenv("CINE_WEBHOOK_SECRET")


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
            CREATE TABLE IF NOT EXISTS welcome_config (
                guild_id BIGINT PRIMARY KEY,
                channel_id BIGINT,
                message_template TEXT,
                draftbot_id BIGINT,
                draftbot_channel_id BIGINT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS welcome_stickers (
                guild_id BIGINT NOT NULL,
                sticker_id BIGINT NOT NULL,
                sticker_name TEXT NOT NULL,
                PRIMARY KEY (guild_id, sticker_id)
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS known_members (
                guild_id BIGINT NOT NULL,
                member_id BIGINT NOT NULL,
                PRIMARY KEY (guild_id, member_id)
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS cine_config (
                guild_id BIGINT PRIMARY KEY,
                channel_id BIGINT,
                role_id BIGINT,
                event_time TEXT,
                film TEXT,
                film_by TEXT,
                film_num INTEGER,
                film_poster TEXT,
                last_announced DATE
            )
            """
        )
        # Si la table existait déjà avant l'ajout de ces colonnes (mise à
        # jour du bot), on les ajoute proprement sans tout recréer.
        cur.execute(
            "ALTER TABLE welcome_config ADD COLUMN IF NOT EXISTS draftbot_id BIGINT"
        )
        cur.execute(
            "ALTER TABLE welcome_config ADD COLUMN IF NOT EXISTS draftbot_channel_id BIGINT"
        )
        cur.execute("ALTER TABLE cine_config ADD COLUMN IF NOT EXISTS film_poster TEXT")
        cur.execute("ALTER TABLE cine_config ADD COLUMN IF NOT EXISTS announce_day INTEGER")
        cur.execute("ALTER TABLE cine_config ADD COLUMN IF NOT EXISTS announce_hour INTEGER")
        cur.execute("ALTER TABLE cine_config ADD COLUMN IF NOT EXISTS announce_minute INTEGER")


def is_known_member(guild_id: int, member_id: int) -> bool:
    with get_db() as db, db.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM known_members WHERE guild_id = %s AND member_id = %s",
            (guild_id, member_id),
        )
        return cur.fetchone() is not None


def mark_known_member(guild_id: int, member_id: int):
    with get_db() as db, db.cursor() as cur:
        cur.execute(
            "INSERT INTO known_members (guild_id, member_id) VALUES (%s, %s) "
            "ON CONFLICT (guild_id, member_id) DO NOTHING",
            (guild_id, member_id),
        )


def forget_member(guild_id: int, member_id: int):
    """Appelé quand un membre quitte : on l'oublie, pour qu'un futur retour
    (leave + rejoin) soit à nouveau traité comme une vraie arrivée."""
    with get_db() as db, db.cursor() as cur:
        cur.execute(
            "DELETE FROM known_members WHERE guild_id = %s AND member_id = %s",
            (guild_id, member_id),
        )


def get_welcome_config(guild_id: int) -> dict | None:
    with get_db() as db, db.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT * FROM welcome_config WHERE guild_id = %s", (guild_id,))
        row = cur.fetchone()
    return dict(row) if row else None


def set_welcome_channel(guild_id: int, channel_id: int | None):
    with get_db() as db, db.cursor() as cur:
        cur.execute(
            """
            INSERT INTO welcome_config (guild_id, channel_id) VALUES (%s, %s)
            ON CONFLICT (guild_id) DO UPDATE SET channel_id = EXCLUDED.channel_id
            """,
            (guild_id, channel_id),
        )


def set_welcome_message(guild_id: int, template: str):
    with get_db() as db, db.cursor() as cur:
        cur.execute(
            """
            INSERT INTO welcome_config (guild_id, message_template) VALUES (%s, %s)
            ON CONFLICT (guild_id) DO UPDATE SET message_template = EXCLUDED.message_template
            """,
            (guild_id, template),
        )


def set_draftbot(guild_id: int, bot_id: int | None, channel_id: int | None):
    with get_db() as db, db.cursor() as cur:
        cur.execute(
            """
            INSERT INTO welcome_config (guild_id, draftbot_id, draftbot_channel_id) VALUES (%s, %s, %s)
            ON CONFLICT (guild_id) DO UPDATE SET
                draftbot_id = EXCLUDED.draftbot_id,
                draftbot_channel_id = EXCLUDED.draftbot_channel_id
            """,
            (guild_id, bot_id, channel_id),
        )


def add_welcome_sticker(guild_id: int, sticker_id: int, sticker_name: str):
    with get_db() as db, db.cursor() as cur:
        cur.execute(
            "INSERT INTO welcome_stickers (guild_id, sticker_id, sticker_name) VALUES (%s, %s, %s) "
            "ON CONFLICT (guild_id, sticker_id) DO NOTHING",
            (guild_id, sticker_id, sticker_name),
        )


def remove_welcome_sticker(guild_id: int, sticker_id: int) -> bool:
    with get_db() as db, db.cursor() as cur:
        cur.execute(
            "DELETE FROM welcome_stickers WHERE guild_id = %s AND sticker_id = %s",
            (guild_id, sticker_id),
        )
        return cur.rowcount > 0


def get_welcome_stickers(guild_id: int) -> list[dict]:
    with get_db() as db, db.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "SELECT * FROM welcome_stickers WHERE guild_id = %s ORDER BY sticker_name", (guild_id,)
        )
        return [dict(r) for r in cur.fetchall()]


CINE_COLUMNS = {"channel_id", "role_id", "event_time", "film", "film_by", "film_num", "film_poster", "last_announced",
                "announce_day", "announce_hour", "announce_minute"}


def get_cine_config(guild_id: int) -> dict | None:
    with get_db() as db, db.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT * FROM cine_config WHERE guild_id = %s", (guild_id,))
        row = cur.fetchone()
    return dict(row) if row else None


def set_cine(guild_id: int, **fields):
    """Met à jour uniquement les colonnes données (liste blanche CINE_COLUMNS)."""
    cols = [c for c in fields if c in CINE_COLUMNS]
    if not cols:
        return
    names = ", ".join(cols)
    placeholders = ", ".join(["%s"] * len(cols))
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols)
    with get_db() as db, db.cursor() as cur:
        cur.execute(
            f"INSERT INTO cine_config (guild_id, {names}) VALUES (%s, {placeholders}) "
            f"ON CONFLICT (guild_id) DO UPDATE SET {updates}",
            (guild_id, *[fields[c] for c in cols]),
        )


def cine_schedule_of(cfg: dict | None) -> tuple[int, int, int]:
    """(jour, heure, minute) de l'annonce, avec les valeurs par défaut si non réglé."""
    cfg = cfg or {}
    day = cfg.get("announce_day")
    hour = cfg.get("announce_hour")
    minute = cfg.get("announce_minute")
    return (
        CINE_DEFAULT_DAY if day is None else day,
        CINE_DEFAULT_HOUR if hour is None else hour,
        CINE_DEFAULT_MINUTE if minute is None else minute,
    )


def format_cine_schedule(cfg: dict | None) -> str:
    day, hour, minute = cine_schedule_of(cfg)
    return f"chaque {JOURS[day]} à {hour}h{minute:02d}"


# Planning en mémoire {guild_id: (jour, heure, minute)} pour les serveurs qui ont
# un salon défini. Il permet à la boucle de ne contacter la base que lorsqu'une
# annonce est vraiment due (sinon Neon ne pourrait jamais se mettre en veille).
CINE_SCHEDULES: dict[int, tuple[int, int, int]] = {}


def load_cine_schedules():
    with get_db() as db, db.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT * FROM cine_config WHERE channel_id IS NOT NULL")
        CINE_SCHEDULES.clear()
        for row in cur.fetchall():
            CINE_SCHEDULES[row["guild_id"]] = cine_schedule_of(dict(row))


def refresh_cine_schedule(guild_id: int):
    cfg = get_cine_config(guild_id)
    if cfg and cfg.get("channel_id"):
        CINE_SCHEDULES[guild_id] = cine_schedule_of(cfg)
    else:
        CINE_SCHEDULES.pop(guild_id, None)


def current_cine_slot(now: datetime, schedule: tuple[int, int, int]):
    """Si `now` est dans la fenêtre d'annonce de ce planning, renvoie la date du
    créneau (utile si la fenêtre passe minuit), sinon None."""
    day, hour, minute = schedule
    window = timedelta(hours=CINE_WINDOW_HOURS)
    for back in (0, 1):
        slot = (now - timedelta(days=back)).replace(hour=hour, minute=minute, second=0, microsecond=0)
        if slot.weekday() == day and slot <= now < slot + window:
            return slot.date()
    return None


def next_cine_slot(now: datetime, schedule: tuple[int, int, int]) -> datetime:
    """Prochain moment d'annonce (maintenant inclus si on est dans la fenêtre)."""
    day, hour, minute = schedule
    for ahead in range(0, 8):
        slot = (now + timedelta(days=ahead)).replace(hour=hour, minute=minute, second=0, microsecond=0)
        if slot.weekday() == day and slot + timedelta(hours=CINE_WINDOW_HOURS) > now:
            return slot
    return now


def get_due_cine(guild_ids: list[int]) -> list[dict]:
    """Serveurs (parmi guild_ids) avec salon et film définis : le film est effacé après
    chaque annonce, donc il n'y a pas de limite d'annonces par semaine."""
    if not guild_ids:
        return []
    with get_db() as db, db.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "SELECT * FROM cine_config WHERE channel_id IS NOT NULL AND film IS NOT NULL "
            "AND guild_id = ANY(%s)",
            (list(guild_ids),),
        )
        return [dict(r) for r in cur.fetchall()]


# ---------------------------------------------------------------------------
# Bot
# ---------------------------------------------------------------------------

intents = discord.Intents.default()
# Intent privilégié nécessaire pour détecter l'arrivée de nouveaux membres.
# Doit AUSSI être activé sur le portail développeur Discord (onglet Bot →
# Server Members Intent), sinon on_member_join ne se déclenche jamais,
# silencieusement.
intents.members = True
bot = commands.Bot(command_prefix="!", intents=intents)

# Messages de bienvenue avec bouton actif : {message_id: {"guild_id": int, "channel_id": int,
# "member_mention": str, "clicked_by": set[int]}}
# Le bouton reste actif indéfiniment (il n'est jamais désactivé) : chaque
# personne du serveur peut l'utiliser, mais une seule fois chacune (son ID est
# ajouté à "clicked_by" après son premier clic). Perdre ce dict au redémarrage
# du bot n'est pas grave pour les données (le bouton lui-même reste
# fonctionnel grâce à bot.add_view() dans on_ready), mais tout le monde pourra
# alors re-cliquer une fois après un redémarrage.
pending_welcome_buttons: dict[int, dict] = {}

# Empêche de démarrer plusieurs fois le serveur web ou de ré-enregistrer les
# vues persistantes si on_ready se déclenche plusieurs fois (reconnexions Discord).
_webserver_started = False
_persistent_views_registered = False


# ---------------------------------------------------------------------------
# Gestion d'erreurs globale
# ---------------------------------------------------------------------------
# Sans ça, une exception survenant APRÈS un interaction.response.defer() (ex:
# la base Postgres qui échoue, un bug quelconque) ne serait jamais rattrapée :
# l'interaction resterait bloquée indéfiniment sur "... réfléchit" côté
# utilisateur, sans aucun message d'erreur ni sur Discord ni dans les logs.

async def _report_interaction_error(interaction: discord.Interaction, error: Exception):
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


async def _health_handler(request: web.Request) -> web.Response:
    return web.Response(text="Le bot Discord de bienvenue est en ligne.")


async def start_fake_webserver():
    """Démarre un minuscule serveur HTTP.

    Sert uniquement à satisfaire les hébergeurs (comme Render) qui exigent
    qu'un service de type "Web Service" écoute sur un port, alors que le bot
    Discord lui-même n'a besoin d'aucun port : toute sa communication passe
    par une connexion sortante vers l'API Discord (le "gateway").
    """
    app = web.Application(client_max_size=8 * 1024)
    app.router.add_get("/", _health_handler)
    app.router.add_post("/cine", _cine_post)
    app.router.add_route("OPTIONS", "/cine", _cine_options)

    runner = web.AppRunner(app)
    await runner.setup()

    port = int(os.getenv("PORT", "10000"))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    print(f"Serveur web factice démarré sur le port {port} (pour satisfaire l'hébergeur).")


# ---------------------------------------------------------------------------
# Message de bienvenue personnalisé avec autocollant choisi par bouton
# ---------------------------------------------------------------------------

class WelcomeStickerView(discord.ui.View):
    """Bouton "Bienvenue fami" attaché au message de bienvenue. Persistant
    (timeout=None + custom_id fixe) pour continuer à fonctionner même après
    un redémarrage du bot, tant qu'il est ré-enregistré via bot.add_view()
    dans on_ready."""

    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(
        label="🎉 Bienvenue fami",
        style=discord.ButtonStyle.secondary,
        custom_id="welcome_sticker_button",
    )
    async def welcome_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        entry = pending_welcome_buttons.get(interaction.message.id)
        if entry is None:
            await interaction.response.send_message(
                "Ce bouton n'est plus actif (le bot a probablement redémarré depuis).", ephemeral=True
            )
            return

        if interaction.user.id in entry["clicked_by"]:
            await interaction.response.send_message(
                "Tu as déjà souhaité la bienvenue ici !", ephemeral=True
            )
            return

        # On marque tout de suite ce clic comme traité (avant tout await),
        # pour qu'un double-clic rapide de la même personne ne déclenche pas
        # deux envois.
        entry["clicked_by"].add(interaction.user.id)

        configured_stickers = get_welcome_stickers(entry["guild_id"])
        if not configured_stickers:
            await interaction.response.send_message("Aucun autocollant configuré pour la bienvenue.", ephemeral=True)
            return
        choice = random.choice(configured_stickers)
        sticker = discord.utils.get(interaction.guild.stickers, id=choice["sticker_id"])
        if sticker is None:
            await interaction.response.send_message("Autocollant introuvable.", ephemeral=True)
            return

        channel = interaction.guild.get_channel(entry["channel_id"])
        if channel is None:
            await interaction.response.send_message("Salon introuvable.", ephemeral=True)
            return

        content = f"{interaction.user.mention} te souhaite la bienvenue {entry['member_mention']} !"
        await interaction.response.defer()
        try:
            await channel.send(content=content, stickers=[sticker])
        except discord.Forbidden:
            pass


async def send_welcome_message(member: discord.Member):
    """Poste le message de bienvenue (+ bouton à sticker) pour ce membre, une seule fois."""
    if is_known_member(member.guild.id, member.id):
        print(f"[bienvenue] {member} a déjà été accueilli sur {member.guild.name} — on ignore.")
        return

    print(f"[bienvenue] send_welcome_message appelé pour {member} (id={member.id}, pending={member.pending}) sur {member.guild.name}")

    config = get_welcome_config(member.guild.id)
    if config is None or not config.get("channel_id"):
        print(f"[bienvenue] Aucun salon configuré pour {member.guild.name} (config={config}) — abandon.")
        return  # Système de bienvenue non configuré sur ce serveur

    channel = member.guild.get_channel(config["channel_id"])
    if channel is None:
        print(f"[bienvenue] Salon {config['channel_id']} introuvable (supprimé ?) sur {member.guild.name} — abandon.")
        return

    template = config.get("message_template") or DEFAULT_WELCOME_TEMPLATE
    content = template.replace("{membre}", member.mention).replace("{serveur}", member.guild.name)

    # Le bouton (et donc le sticker) n'a d'intérêt que s'il y a des stickers
    # configurés pour ce serveur ; sinon on envoie juste le message texte.
    has_stickers = bool(get_welcome_stickers(member.guild.id))
    view = WelcomeStickerView() if has_stickers else None

    # On marque le membre comme accueilli AVANT d'envoyer, pour éviter tout
    # doublon si plusieurs déclencheurs (join, changement de rôle, synchro
    # périodique) se déclenchaient pour la même personne.
    mark_known_member(member.guild.id, member.id)

    try:
        welcome_message = await channel.send(content=content, view=view)
        print(f"[bienvenue] Message envoyé avec succès dans #{channel.name} pour {member}.")
    except discord.Forbidden:
        print(f"[bienvenue] Permission refusée pour envoyer dans #{channel.name} sur {member.guild.name}.")
        return
    except Exception as e:
        print(f"[bienvenue] Erreur inattendue lors de l'envoi : {e!r}")
        raise

    if has_stickers:
        pending_welcome_buttons[welcome_message.id] = {
            "guild_id": member.guild.id,
            "channel_id": channel.id,
            "member_mention": member.mention,
            "clicked_by": set(),
        }


@bot.event
async def on_member_join(member: discord.Member):
    print(f"[bienvenue] on_member_join déclenché pour {member} (id={member.id}, bot={member.bot}, pending={member.pending}) sur {member.guild.name}")
    if member.bot:
        return

    await send_welcome_message(member)


@bot.event
async def on_member_remove(member: discord.Member):
    # On oublie ce membre : s'il revient plus tard (leave + rejoin), ce sera
    # traité comme une nouvelle arrivée normale (par on_member_join ou par
    # le message de DraftBot, voir on_message ci-dessous).
    forget_member(member.guild.id, member.id)


@bot.event
async def on_message(message: discord.Message):
    # Filet de sécurité pour les arrivées que Discord ne signale jamais à
    # notre bot (bug connu avec les comptes liés à une intégration comme
    # Twitch : https://github.com/Rapptz/discord.py/discussions/10366).
    # DraftBot, lui, arrive à détecter ces arrivées et poste un message de
    # bienvenue — on se base donc dessus comme second déclencheur.
    if message.guild is not None and message.author.bot:
        config = get_welcome_config(message.guild.id)
        if (
            config is not None
            and config.get("draftbot_id")
            and message.author.id == config["draftbot_id"]
            and config.get("draftbot_channel_id")
            and message.channel.id == config["draftbot_channel_id"]
        ):
            # `message.mentions` reste disponible même sans l'intent
            # privilégié de contenu des messages (contrairement à
            # `message.content`) : c'est une liste structurée à part,
            # fournie par Discord indépendamment du texte.
            real_mentions = [m for m in message.mentions if not m.bot]
            if real_mentions:
                member = real_mentions[0]
                print(f"[bienvenue] Message de DraftBot détecté, mentionnant {member} sur {message.guild.name}.")
                await send_welcome_message(member)

    # Nécessaire puisqu'on surcharge on_message : sans ça, les éventuelles
    # commandes textuelles ("!...") cesseraient discrètement de fonctionner.
    await bot.process_commands(message)


welcome_group = app_commands.Group(
    name="bienvenue", description="Configurer le message de bienvenue et ses autocollants"
)


async def welcome_sticker_autocomplete(interaction: discord.Interaction, current: str):
    """Liste les autocollants existants du serveur (pour /bienvenue sticker-ajouter)."""
    if interaction.guild is None:
        return []
    choices = []
    for sticker in interaction.guild.stickers:
        if current.lower() in sticker.name.lower():
            choices.append(app_commands.Choice(name=sticker.name[:100], value=str(sticker.id)))
    return choices[:25]


async def configured_welcome_sticker_autocomplete(interaction: discord.Interaction, current: str):
    """Liste les autocollants déjà configurés pour la bienvenue (pour /bienvenue sticker-retirer)."""
    if interaction.guild_id is None:
        return []
    choices = []
    for s in get_welcome_stickers(interaction.guild_id):
        if current.lower() in s["sticker_name"].lower():
            choices.append(app_commands.Choice(name=s["sticker_name"][:100], value=str(s["sticker_id"])))
    return choices[:25]


@welcome_group.command(name="salon", description="Définir le salon où poster les messages de bienvenue")
@app_commands.describe(salon="Le salon textuel à utiliser (laisse vide pour désactiver)")
async def bienvenue_salon(interaction: discord.Interaction, salon: discord.TextChannel = None):
    if interaction.guild is None:
        await interaction.response.send_message("Cette commande doit être utilisée sur un serveur.", ephemeral=True)
        return
    if not interaction.user.guild_permissions.manage_guild:
        await interaction.response.send_message(
            "Il te faut la permission \"Gérer le serveur\" pour configurer la bienvenue.", ephemeral=True
        )
        return

    await interaction.response.defer(ephemeral=True)
    set_welcome_channel(interaction.guild_id, salon.id if salon else None)

    if salon:
        await interaction.followup.send(f"Les messages de bienvenue seront postés dans {salon.mention} ✅", ephemeral=True)
    else:
        await interaction.followup.send("Système de bienvenue désactivé.", ephemeral=True)


@welcome_group.command(name="message", description="Personnaliser le texte du message de bienvenue")
@app_commands.describe(texte="Utilise {membre} pour la mention et {serveur} pour le nom du serveur")
async def bienvenue_message(interaction: discord.Interaction, texte: str):
    if interaction.guild is None:
        await interaction.response.send_message("Cette commande doit être utilisée sur un serveur.", ephemeral=True)
        return
    if not interaction.user.guild_permissions.manage_guild:
        await interaction.response.send_message(
            "Il te faut la permission \"Gérer le serveur\" pour configurer la bienvenue.", ephemeral=True
        )
        return
    if "{membre}" not in texte:
        await interaction.response.send_message(
            "Ton message doit contenir `{membre}` pour mentionner la personne qui arrive.", ephemeral=True
        )
        return

    await interaction.response.defer(ephemeral=True)
    set_welcome_message(interaction.guild_id, texte)
    exemple = texte.replace("{membre}", interaction.user.mention).replace("{serveur}", interaction.guild.name)
    await interaction.followup.send(f"Message mis à jour ✅\n\nAperçu : {exemple}", ephemeral=True)


@welcome_group.command(
    name="draftbot",
    description="Se caler sur les messages de bienvenue d'un autre bot (ex: DraftBot) comme filet de sécurité",
)
@app_commands.describe(
    bot="Le bot dont les messages de bienvenue doivent aussi déclencher le nôtre (laisse vide pour désactiver)",
    salon="Le salon où CE bot poste ses messages de bienvenue (peut être différent de celui réglé avec /bienvenue salon)",
)
async def bienvenue_draftbot(
    interaction: discord.Interaction,
    bot: discord.User = None,
    salon: discord.TextChannel = None,
):
    if interaction.guild is None:
        await interaction.response.send_message("Cette commande doit être utilisée sur un serveur.", ephemeral=True)
        return
    if not interaction.user.guild_permissions.manage_guild:
        await interaction.response.send_message(
            "Il te faut la permission \"Gérer le serveur\" pour configurer la bienvenue.", ephemeral=True
        )
        return
    if bot is not None and not bot.bot:
        await interaction.response.send_message(
            "Ce n'est pas un bot — choisis le compte du bot qui poste les messages de bienvenue (ex: DraftBot).",
            ephemeral=True,
        )
        return
    if bot is not None and salon is None:
        await interaction.response.send_message(
            "Précise aussi dans quel salon ce bot poste ses messages de bienvenue (paramètre `salon`).",
            ephemeral=True,
        )
        return

    await interaction.response.defer(ephemeral=True)
    set_draftbot(interaction.guild_id, bot.id if bot else None, salon.id if salon else None)

    if bot:
        await interaction.followup.send(
            f"Les messages de bienvenue de **{bot.name}** dans {salon.mention} déclencheront aussi le "
            f"message et le sticker de ce bot ✅\n\n"
            f"⚠️ Ça ne marche que si {bot.name} mentionne bien le nouveau membre dans son message.",
            ephemeral=True,
        )
    else:
        await interaction.followup.send("Filet de sécurité désactivé.", ephemeral=True)


@welcome_group.command(name="sticker-ajouter", description="Ajouter un autocollant du serveur au tirage de bienvenue")
@app_commands.describe(sticker="L'autocollant du serveur à ajouter")
@app_commands.autocomplete(sticker=welcome_sticker_autocomplete)
async def bienvenue_sticker_ajouter(interaction: discord.Interaction, sticker: str):
    if interaction.guild is None:
        await interaction.response.send_message("Cette commande doit être utilisée sur un serveur.", ephemeral=True)
        return
    if not interaction.user.guild_permissions.manage_guild:
        await interaction.response.send_message(
            "Il te faut la permission \"Gérer le serveur\" pour configurer la bienvenue.", ephemeral=True
        )
        return

    await interaction.response.defer(ephemeral=True)

    guild_sticker = discord.utils.get(interaction.guild.stickers, id=int(sticker))
    if guild_sticker is None:
        await interaction.followup.send(
            "Autocollant introuvable. Choisis-en un dans la liste proposée par l'autocomplétion.",
            ephemeral=True,
        )
        return

    add_welcome_sticker(interaction.guild_id, guild_sticker.id, guild_sticker.name)
    await interaction.followup.send(
        f"Autocollant **{guild_sticker.name}** ajouté au tirage de bienvenue ✅", ephemeral=True
    )


@welcome_group.command(name="sticker-retirer", description="Retirer un autocollant du tirage de bienvenue")
@app_commands.describe(sticker="L'autocollant à retirer du tirage")
@app_commands.autocomplete(sticker=configured_welcome_sticker_autocomplete)
async def bienvenue_sticker_retirer(interaction: discord.Interaction, sticker: str):
    if interaction.guild is None:
        await interaction.response.send_message("Cette commande doit être utilisée sur un serveur.", ephemeral=True)
        return
    if not interaction.user.guild_permissions.manage_guild:
        await interaction.response.send_message(
            "Il te faut la permission \"Gérer le serveur\" pour configurer la bienvenue.", ephemeral=True
        )
        return

    await interaction.response.defer(ephemeral=True)
    removed = remove_welcome_sticker(interaction.guild_id, int(sticker))
    if removed:
        await interaction.followup.send("Autocollant retiré du tirage de bienvenue ✅", ephemeral=True)
    else:
        await interaction.followup.send("Cet autocollant n'était pas dans le tirage.", ephemeral=True)


@welcome_group.command(name="apercu", description="Prévisualiser le message de bienvenue actuel")
async def bienvenue_apercu(interaction: discord.Interaction):
    if interaction.guild is None:
        await interaction.response.send_message("Cette commande doit être utilisée sur un serveur.", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)

    config = get_welcome_config(interaction.guild_id)
    channel_id = config.get("channel_id") if config else None
    template = (config.get("message_template") if config else None) or DEFAULT_WELCOME_TEMPLATE
    stickers = get_welcome_stickers(interaction.guild_id)
    draftbot_id = config.get("draftbot_id") if config else None
    draftbot_channel_id = config.get("draftbot_channel_id") if config else None

    exemple = template.replace("{membre}", interaction.user.mention).replace("{serveur}", interaction.guild.name)
    salon_txt = f"<#{channel_id}>" if channel_id else "*non configuré (le système est désactivé)*"
    stickers_txt = ", ".join(s["sticker_name"] for s in stickers) if stickers else "*aucun configuré*"
    if draftbot_id and draftbot_channel_id:
        draftbot_txt = f"<@{draftbot_id}> dans <#{draftbot_channel_id}>"
    else:
        draftbot_txt = "*aucun (désactivé)*"

    embed = discord.Embed(title="Aperçu du message de bienvenue", description=exemple, color=EMBED_COLOR)
    embed.add_field(name="Salon", value=salon_txt, inline=False)
    embed.add_field(name="Autocollants dans le tirage", value=stickers_txt, inline=False)
    embed.add_field(name="Filet de sécurité (bot de référence)", value=draftbot_txt, inline=False)
    await interaction.followup.send(embed=embed, ephemeral=True)


bot.tree.add_command(welcome_group)


# ---------------------------------------------------------------------------
# Annonce du Cinédimanche
# ---------------------------------------------------------------------------

async def fetch_poster(title: str) -> str | None:
    """Cherche l'affiche sur TMDB (facultatif : ignoré sans TMDB_API_KEY)."""
    if not TMDB_API_KEY:
        return None
    try:
        m = re.match(r"^(.*?)[\s(\[]*((?:19|20)\d\d)[)\]]?\s*$", title)
        query, year = (m.group(1).strip(), m.group(2)) if m and m.group(1).strip() else (title.strip(), "")
        params = {"include_adult": "false", "language": "fr-FR", "query": query}
        if year:
            params["year"] = year
        headers = {}
        if len(TMDB_API_KEY) > 40:
            headers["Authorization"] = f"Bearer {TMDB_API_KEY}"
        else:
            params["api_key"] = TMDB_API_KEY
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=8)) as session:
            async with session.get("https://api.themoviedb.org/3/search/movie", params=params, headers=headers) as r:
                if r.status != 200:
                    return None
                results = (await r.json()).get("results") or []
        hit = next((x for x in results if x.get("poster_path")), None)
        return f"https://image.tmdb.org/t/p/w500{hit['poster_path']}" if hit else None
    except Exception as e:
        print(f"[cine] Affiche TMDB introuvable : {e!r}")
        return None


async def send_cine_announcement(cfg: dict) -> bool:
    """Poste l'annonce du film prévu, puis efface le film (pour ne pas le re-annoncer)."""
    channel = bot.get_channel(cfg["channel_id"])
    if channel is None:
        print(f"[cine] Salon {cfg['channel_id']} introuvable (serveur {cfg['guild_id']}) — abandon.")
        return False

    film = cfg["film"]
    num = cfg.get("film_num")
    numero = f" {num}" if num else ""
    texte = (
        "Ce soir, comme tous les dimanches, c'est ciné dimanche @everyone !\n\n"
        "Qu'est-ce que le cinédimanche ? On regarde le film que **VOUS** avez choisi dimanche dernier "
        "et à la fin du film, **VOUS** choisissez le film de dimanche prochain\n\n"
        f"Ce soir le cinédimanche{numero} : **{film}**"
    )
    # L'affiche (et « proposé par ») dans un embed sous le message
    poster = cfg.get("film_poster") or await fetch_poster(film)
    embed = None
    if poster or cfg.get("film_by"):
        embed = discord.Embed(color=EMBED_COLOR)
        if poster:
            embed.set_image(url=poster)
        if cfg.get("film_by"):
            embed.set_footer(text=f"Proposé par {cfg['film_by']}")

    try:
        await channel.send(
            content=texte,
            embed=embed,
            allowed_mentions=discord.AllowedMentions(everyone=True),
        )
    except discord.Forbidden:
        print(f"[cine] Permission refusée pour envoyer dans #{channel.name}.")
        return False

    set_cine(cfg["guild_id"], film=None, film_by=None, film_num=None, film_poster=None)
    print(f"[cine] Annonce envoyée dans #{channel.name} : {film}")
    return True


# Envois en échec (serveur, créneau, film), pour ne pas réessayer toutes les minutes.
_cine_failed: set = set()


@tasks.loop(minutes=1)
async def cine_announce_loop():
    """Vérifie chaque minute si un serveur est dans sa fenêtre d'annonce (jour et heure
    choisis avec /cine salon, heure de Paris). Hors fenêtre, aucune requête à la base."""
    try:
        now = datetime.now(PARIS)
        slot_dates = {}
        for gid, schedule in list(CINE_SCHEDULES.items()):
            slot = current_cine_slot(now, schedule)
            if slot is not None:
                slot_dates[gid] = slot
        if not slot_dates:
            return
        for cfg in await asyncio.to_thread(get_due_cine, list(slot_dates)):
            key = (cfg["guild_id"], slot_dates[cfg["guild_id"]], cfg["film"])
            if key in _cine_failed:
                continue  # envoi déjà échoué pour ce film et ce créneau : pas de retry chaque minute
            if not await send_cine_announcement(cfg):
                _cine_failed.add(key)
    except Exception:
        traceback.print_exc()


@cine_announce_loop.before_loop
async def _before_cine_loop():
    await bot.wait_until_ready()


def _cors(resp: web.Response) -> web.Response:
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
    resp.headers["Access-Control-Allow-Methods"] = "POST, OPTIONS"
    return resp


def _reply(status: int, ok: bool, error: str = "") -> web.Response:
    return _cors(web.json_response({"ok": ok, "error": error}, status=status))


async def _cine_options(request: web.Request) -> web.Response:
    return _cors(web.Response(status=204))


async def _cine_post(request: web.Request) -> web.Response:
    """Reçoit le film choisi au vote depuis la page de la roue.
    Corps JSON (envoyé en text/plain pour éviter une requête de pré-vérification CORS) :
    {secret, film, by, num, poster, guild_id}"""
    if not CINE_WEBHOOK_SECRET:
        return _reply(404, False, "Envoi depuis la page désactivé (CINE_WEBHOOK_SECRET absent).")
    try:
        data = json.loads(await request.text())
    except ValueError:
        return _reply(400, False, "Requête illisible.")
    if not isinstance(data, dict) or not hmac.compare_digest(
        str(data.get("secret", "")).encode(), CINE_WEBHOOK_SECRET.encode()
    ):
        return _reply(401, False, "Mot de passe incorrect.")

    film = str(data.get("film") or "").strip()[:100]
    if not film:
        return _reply(400, False, "Film manquant.")
    by = str(data.get("by") or "").strip()[:40] or None
    try:
        num = int(data.get("num") or 0)
    except (TypeError, ValueError):
        num = 0
    num = num if 1 <= num <= 9999 else None
    poster = str(data.get("poster") or "")
    if not poster.startswith("https://image.tmdb.org/t/p/") or len(poster) > 300:
        poster = None

    gid = str(data.get("guild_id") or "").strip()
    if gid.isdigit():
        guild = bot.get_guild(int(gid))
    else:
        guild = bot.guilds[0] if len(bot.guilds) == 1 else None
    if guild is None:
        return _reply(400, False, "Serveur introuvable : renseigne l'ID du serveur Discord dans la page.")

    await asyncio.to_thread(set_cine, guild.id, film=film, film_by=by, film_num=num, film_poster=poster)
    print(f"[cine] Film reçu depuis la page : {film} (n°{num}, par {by}) pour {guild.name}")
    return _reply(200, True)


cine_group = app_commands.Group(name="cine", description="Annonce du Cinédimanche")


async def _require_cine_admin(interaction: discord.Interaction) -> bool:
    if interaction.guild is None:
        await interaction.response.send_message("Cette commande doit être utilisée sur un serveur.", ephemeral=True)
        return False
    if not interaction.user.guild_permissions.manage_guild:
        await interaction.response.send_message(
            "Il te faut la permission \"Gérer le serveur\" pour gérer le Cinédimanche.", ephemeral=True
        )
        return False
    return True


def parse_heure(texte: str) -> tuple[int, int] | None:
    """Accepte 12, 12h, 12h30, 12:30, 8h05... Renvoie (heure, minute) ou None."""
    m = re.fullmatch(r"\s*(\d{1,2})\s*(?:[h:]\s*(\d{1,2})?)?\s*", texte.lower())
    if not m:
        return None
    hour, minute = int(m.group(1)), int(m.group(2) or 0)
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        return None
    return hour, minute


@cine_group.command(name="salon", description="Définir le salon de l'annonce, ainsi que le jour et l'heure")
@app_commands.describe(
    salon="Le salon des annonces (laisse tout vide pour désactiver l'annonce)",
    jour="Jour de l'annonce (par défaut : dimanche)",
    heure="Heure de l'annonce, heure de Paris : 12h, 18h30, 20:15... (par défaut : 12h)",
)
@app_commands.choices(jour=[app_commands.Choice(name=j.capitalize(), value=i) for i, j in enumerate(JOURS)])
async def cine_salon(
    interaction: discord.Interaction,
    salon: discord.TextChannel = None,
    jour: app_commands.Choice[int] = None,
    heure: str = None,
):
    if not await _require_cine_admin(interaction):
        return

    # Aucun argument : on désactive l'annonce.
    if salon is None and jour is None and heure is None:
        await interaction.response.defer(ephemeral=True)
        set_cine(interaction.guild_id, channel_id=None)
        refresh_cine_schedule(interaction.guild_id)
        await interaction.followup.send("Annonce du Cinédimanche désactivée.", ephemeral=True)
        return

    parsed = None
    if heure is not None:
        parsed = parse_heure(heure)
        if parsed is None:
            await interaction.response.send_message(
                "Heure invalide. Exemples : `12h`, `18h30`, `20:15`.", ephemeral=True
            )
            return

    await interaction.response.defer(ephemeral=True)
    cfg = get_cine_config(interaction.guild_id) or {}
    fields = {}
    if salon is not None:
        fields["channel_id"] = salon.id
    if jour is not None:
        fields["announce_day"] = jour.value
    if parsed is not None:
        fields["announce_hour"], fields["announce_minute"] = parsed
    # Si on ne change que le jour OU que l'heure, on fixe aussi l'autre valeur
    # (par défaut ou déjà réglée) pour que le planning soit toujours complet.
    day, hour, minute = cine_schedule_of({**cfg, **fields})
    fields.update(announce_day=day, announce_hour=hour, announce_minute=minute)
    set_cine(interaction.guild_id, **fields)
    refresh_cine_schedule(interaction.guild_id)

    cfg = get_cine_config(interaction.guild_id) or {}
    if not cfg.get("channel_id"):
        await interaction.followup.send(
            f"Planning enregistré : {format_cine_schedule(cfg)}.\n"
            "⚠️ Aucun salon défini : relance `/cine salon` avec le paramètre `salon`.",
            ephemeral=True,
        )
        return
    await interaction.followup.send(
        f"Le film sera annoncé dans <#{cfg['channel_id']}> {format_cine_schedule(cfg)} (heure de Paris) ✅",
        ephemeral=True,
    )


@cine_group.command(name="film", description="Définir le film prévu (annoncé à l'heure réglée avec /cine salon)")
@app_commands.describe(
    titre="Titre du film (tu peux ajouter l'année : Alien (1979))",
    propose_par="Pseudo du viewer qui l'a proposé",
    numero="Numéro du Cinédimanche",
)
async def cine_film(
    interaction: discord.Interaction,
    titre: app_commands.Range[str, 1, 100],
    propose_par: app_commands.Range[str, 1, 40] = None,
    numero: app_commands.Range[int, 1, 9999] = None,
):
    if not await _require_cine_admin(interaction):
        return
    await interaction.response.defer(ephemeral=True)
    set_cine(interaction.guild_id, film=titre.strip(), film_by=propose_par, film_num=numero, film_poster=None)
    cfg = get_cine_config(interaction.guild_id)
    suite = "" if cfg and cfg.get("channel_id") else "\n⚠️ Aucun salon défini : utilise `/cine salon`."
    await interaction.followup.send(f"Film enregistré : **{titre.strip()}** ✅{suite}", ephemeral=True)


@cine_group.command(name="voir", description="Voir le film prévu et la configuration de l'annonce")
async def cine_voir(interaction: discord.Interaction):
    if not await _require_cine_admin(interaction):
        return
    await interaction.response.defer(ephemeral=True)
    cfg = get_cine_config(interaction.guild_id) or {}
    film = cfg.get("film")
    embed = discord.Embed(title="Cinédimanche", color=EMBED_COLOR)
    embed.add_field(
        name="Film prévu",
        value=(f"**{film}**" + (f" (proposé par {cfg['film_by']})" if cfg.get("film_by") else "")) if film else "*aucun*",
        inline=False,
    )
    embed.add_field(name="Numéro", value=str(cfg["film_num"]) if cfg.get("film_num") else "*non défini*")
    embed.add_field(name="Salon", value=f"<#{cfg['channel_id']}>" if cfg.get("channel_id") else "*non configuré (désactivé)*")
    now = datetime.now(PARIS)
    schedule = cine_schedule_of(cfg)
    prochaine = next_cine_slot(now, schedule)
    etat = []
    if not cfg.get("channel_id"):
        etat.append("⚠️ aucun salon défini")
    if not film:
        etat.append("⚠️ aucun film défini")
    embed.add_field(
        name="Annonce",
        value=(
            f"{format_cine_schedule(cfg)} (heure de Paris)\n"
            f"Prochain créneau : {prochaine.strftime('%d/%m à %Hh%M')}\n"
            f"Heure actuelle du bot : {now.strftime('%d/%m %Hh%M')}\n"
            + ("\n".join(etat) if etat else "✅ tout est prêt")
        ),
        inline=False,
    )
    await interaction.followup.send(embed=embed, ephemeral=True)


@cine_group.command(name="effacer", description="Effacer le film prévu")
async def cine_effacer(interaction: discord.Interaction):
    if not await _require_cine_admin(interaction):
        return
    await interaction.response.defer(ephemeral=True)
    set_cine(interaction.guild_id, film=None, film_by=None, film_num=None, film_poster=None)
    await interaction.followup.send("Film prévu effacé.", ephemeral=True)


@cine_group.command(name="annoncer", description="Envoyer l'annonce tout de suite (test)")
async def cine_annoncer(interaction: discord.Interaction):
    if not await _require_cine_admin(interaction):
        return
    await interaction.response.defer(ephemeral=True)
    cfg = get_cine_config(interaction.guild_id)
    if not cfg or not cfg.get("channel_id"):
        await interaction.followup.send("Aucun salon défini : utilise d'abord `/cine salon`.", ephemeral=True)
        return
    if not cfg.get("film"):
        await interaction.followup.send("Aucun film défini : utilise d'abord `/cine film`.", ephemeral=True)
        return
    ok = await send_cine_announcement(cfg)
    await interaction.followup.send(
        "Annonce envoyée ✅ (le film prévu a été effacé)." if ok else "Envoi impossible (salon introuvable ou permission manquante).",
        ephemeral=True,
    )


bot.tree.add_command(cine_group)


@bot.event
async def on_ready():
    init_db()
    try:
        synced = await bot.tree.sync()
        print(f"{bot.user} est connecté. {len(synced)} commandes synchronisées.")
    except Exception as e:
        print(f"Erreur de synchronisation des commandes : {e}")

    # Boucle qui annonce le film du Cinédimanche au jour et à l'heure choisis (Paris).
    try:
        load_cine_schedules()
        print(f"[cine] Planning chargé : {CINE_SCHEDULES}")
    except Exception:
        traceback.print_exc()
    if not cine_announce_loop.is_running():
        cine_announce_loop.start()

    # Le serveur factice n'est utile que si l'hébergeur fournit un PORT
    # (c'est le cas de Render en Web Service). En local, on l'ignore.
    global _webserver_started
    if not _webserver_started and (os.getenv("PORT") or CINE_WEBHOOK_SECRET):
        _webserver_started = True
        asyncio.create_task(start_fake_webserver())

    # Réenregistre le bouton "Bienvenue fami" pour qu'il continue de
    # fonctionner après un redémarrage ou un redéploiement du bot (les vues
    # persistantes ne survivent pas d'elles-mêmes).
    global _persistent_views_registered
    if not _persistent_views_registered:
        _persistent_views_registered = True
        bot.add_view(WelcomeStickerView())


@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    # Filet de sécurité final pour /bienvenue : si une exception survient après
    # un defer(), elle est rattrapée ici plutôt que de laisser l'interaction
    # bloquée indéfiniment.
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
