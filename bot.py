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

Les données sont stockées dans une base PostgreSQL externe (Neon, Supabase, ou
tout autre Postgres compatible), configurée via la variable d'environnement
DATABASE_URL. Contrairement à un fichier SQLite local, cette base survit aux
redéploiements et redémarrages, même sur les hébergeurs sans disque persistant
(comme Render en plan gratuit). Chaque serveur a sa propre configuration.
"""

import os
import sys
import random
import asyncio
import traceback
from contextlib import contextmanager
from datetime import timedelta

import discord
import psycopg2
import psycopg2.extras
from discord import app_commands
from discord.ext import commands
from dotenv import load_dotenv
from aiohttp import web

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
                message_template TEXT
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

# Messages de bienvenue en attente d'un clic sur le bouton pour déclencher
# l'envoi du sticker : {message_id: {"guild_id": int, "channel_id": int, "member_mention": str}}
# Une entrée est retirée dès que le sticker a été envoyé (déclenchement unique).
# Perdre ce dict au redémarrage du bot n'est pas grave pour les données (le
# bouton lui-même reste fonctionnel grâce à bot.add_view() dans on_ready),
# mais un clic pendant cette fenêtre affichera juste "bouton déjà utilisé".
pending_welcome_buttons: dict[int, dict] = {}

# Suivi en mémoire (pas en base) des membres déjà accueillis pendant que le
# bot tourne, pour éviter un double message si on_member_join ET le filet de
# sécurité on_member_update se déclenchaient tous les deux pour la même
# personne. Remis à zéro à chaque redémarrage/redéploiement — volontaire,
# pour pouvoir retester facilement en redéployant.
welcomed_members: set[tuple[int, int]] = set()

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
    app = web.Application()
    app.router.add_get("/", _health_handler)

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
        # On retire l'entrée immédiatement (avant tout await) pour garantir un
        # déclenchement unique même si plusieurs personnes cliquent en même temps.
        entry = pending_welcome_buttons.pop(interaction.message.id, None)
        if entry is None:
            await interaction.response.send_message(
                "Ce bouton a déjà été utilisé.", ephemeral=True
            )
            return

        # On désactive le bouton sur le message d'origine pour que personne
        # d'autre ne puisse re-cliquer dessus.
        button.disabled = True
        button.label = "🎉 Bienvenue envoyée !"
        try:
            await interaction.response.edit_message(view=self)
        except discord.HTTPException:
            pass

        configured_stickers = get_welcome_stickers(entry["guild_id"])
        if not configured_stickers:
            return
        choice = random.choice(configured_stickers)
        sticker = discord.utils.get(interaction.guild.stickers, id=choice["sticker_id"])
        if sticker is None:
            return

        channel = interaction.guild.get_channel(entry["channel_id"])
        if channel is None:
            return

        content = f"{interaction.user.mention} te souhaite la bienvenue {entry['member_mention']} !"
        try:
            await channel.send(content=content, stickers=[sticker])
        except discord.Forbidden:
            pass


async def send_welcome_message(member: discord.Member):
    """Poste le message de bienvenue (+ bouton à sticker) pour ce membre, une seule fois."""
    key = (member.guild.id, member.id)
    if key in welcomed_members:
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
    # doublon si on_member_join et le filet de sécurité (on_member_update)
    # se déclenchaient tous les deux pour la même personne.
    welcomed_members.add(key)

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
        }


@bot.event
async def on_member_join(member: discord.Member):
    print(f"[bienvenue] on_member_join déclenché pour {member} (id={member.id}, bot={member.bot}, pending={member.pending}) sur {member.guild.name}")
    if member.bot:
        return

    await send_welcome_message(member)


@bot.event
async def on_member_update(before: discord.Member, after: discord.Member):
    # Filet de sécurité pour un bug connu de Discord : quand un membre reçoit
    # automatiquement un rôle synchronisé depuis une intégration (ex :
    # abonné Twitch) au moment où il rejoint, on_member_join ne se déclenche
    # parfois jamais (voir https://github.com/Rapptz/discord.py/discussions/10366).
    # On détecte donc aussi l'arrivée via l'apparition de son tout premier
    # rôle, tant que ce membre a rejoint très récemment et n'a pas déjà été
    # accueilli — pour ne pas redéclencher le message à chaque changement de
    # rôle d'un membre présent depuis longtemps.
    if after.bot:
        return
    if before.roles == after.roles:
        return
    if (after.guild.id, after.id) in welcomed_members:
        return
    if after.joined_at is None:
        return
    if discord.utils.utcnow() - after.joined_at > timedelta(minutes=10):
        return

    print(f"[bienvenue] on_member_update (rôle changé, arrivée récente) pour {after} sur {after.guild.name} — déclenchement du filet de sécurité.")
    await send_welcome_message(after)


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

    exemple = template.replace("{membre}", interaction.user.mention).replace("{serveur}", interaction.guild.name)
    salon_txt = f"<#{channel_id}>" if channel_id else "*non configuré (le système est désactivé)*"
    stickers_txt = ", ".join(s["sticker_name"] for s in stickers) if stickers else "*aucun configuré*"

    embed = discord.Embed(title="Aperçu du message de bienvenue", description=exemple, color=EMBED_COLOR)
    embed.add_field(name="Salon", value=salon_txt, inline=False)
    embed.add_field(name="Autocollants dans le tirage", value=stickers_txt, inline=False)
    await interaction.followup.send(embed=embed, ephemeral=True)


bot.tree.add_command(welcome_group)


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
