"""
Bot Discord de "Profil"
========================

Permet à chaque membre d'un serveur de se créer une petite fiche de profil :
- une photo de profil (image)
- une couleur de profil personnalisée
- une bio (texte)
- des liens utiles (ex: Twitter, portfolio, twitch...)
- un compteur du nombre de messages envoyés sur le serveur
- un temps total passé en vocal sur le serveur
- un système de niveaux/XP basé sur l'activité (messages + vocal)

Commandes slash (/) :
    /profil editor     -> ouvre un menu déroulant pour créer/modifier son profil
                           (bio, photo, couleur, liens, suppression) via des
                           sous-menus et des formulaires (modals), sans avoir
                           à mémoriser de commandes séparées.
    /profil view [membre]      -> afficher le profil (le sien ou celui d'un autre membre)
    /profil classement [critere] -> afficher le top 10 du serveur (XP, messages ou vocal)

Le compteur de messages s'incrémente automatiquement à chaque message envoyé
par un membre sur le serveur (les messages des bots ne sont pas comptés).

Le temps de vocal s'accumule automatiquement dès qu'un membre rejoint un salon
vocal jusqu'à ce qu'il en reparte (les changements de salon vocal à vocal ne
réinitialisent pas le chrono, seule une déconnexion complète l'arrête).

L'XP est calculée automatiquement à partir des messages et du temps de vocal
(pas de colonne séparée à maintenir) ; un niveau est débloqué tous les paliers
d'XP, visible sur `/profil view` et `/profil classement` (aucune annonce
publique n'est envoyée lors d'un passage de niveau).

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
import math
from contextlib import contextmanager

import discord
import psycopg2
from discord import app_commands
from discord.ext import commands
from dotenv import load_dotenv
from aiohttp import web

load_dotenv()

TOKEN = os.getenv("DISCORD_TOKEN")
# URL de connexion à la base Postgres (fournie par Neon, Supabase, Render Postgres...).
# Contrairement à un fichier SQLite local, une base externe survit aux redémarrages
# et redéploiements, même sur les plans gratuits sans disque persistant.
DATABASE_URL = os.getenv("DATABASE_URL")
MAX_LINKS = 5
MAX_BIO_LEN = 500
EMBED_COLOR = 0x5865F2  # blurple (couleur par défaut si aucune n'est choisie)

# Système de niveaux / XP : l'XP est dérivée des statistiques déjà suivies,
# pas besoin de colonne supplémentaire pour la stocker.
XP_PER_MESSAGE = 10
XP_PER_VOICE_MINUTE = 5

URL_REGEX = re.compile(r"^https?://[^\s]+$", re.IGNORECASE)


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


def fetch_profile(guild_id: int, user_id: int):
    with get_db() as db, db.cursor() as cur:
        cur.execute(
            "SELECT bio, avatar_url, message_count, voice_seconds, profile_color "
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


def fetch_all_profiles(guild_id: int) -> list[tuple[int, int, int]]:
    """Renvoie (user_id, message_count, voice_seconds) pour tous les profils d'un serveur."""
    with get_db() as db, db.cursor() as cur:
        cur.execute(
            "SELECT user_id, message_count, voice_seconds FROM profiles WHERE guild_id = %s",
            (guild_id,),
        )
        return cur.fetchall()


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


# ---------------------------------------------------------------------------
# Système de niveaux / XP
# ---------------------------------------------------------------------------

def compute_xp(message_count: int, voice_seconds: int) -> int:
    return message_count * XP_PER_MESSAGE + (voice_seconds // 60) * XP_PER_VOICE_MINUTE


def xp_threshold(level: int) -> int:
    """XP totale nécessaire pour atteindre ce niveau (paliers croissants)."""
    return 50 * level * (level + 1)


def compute_level(xp: int) -> int:
    if xp <= 0:
        return 0
    # Résolution approchée de la formule quadratique, puis ajustement exact
    # pour compenser les éventuelles imprécisions de virgule flottante.
    level = int((-1 + math.sqrt(1 + (4 * xp) / 50)) / 2)
    level = max(level, 0)
    while xp_threshold(level + 1) <= xp:
        level += 1
    while level > 0 and xp_threshold(level) > xp:
        level -= 1
    return level


def level_progress(xp: int) -> tuple[int, int, int]:
    """Renvoie (niveau, xp_dans_le_niveau, xp_necessaire_pour_le_niveau)."""
    level = compute_level(xp)
    floor_xp = xp_threshold(level)
    next_xp = xp_threshold(level + 1)
    return level, xp - floor_xp, next_xp - floor_xp


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
    bio, avatar_url, message_count, voice_seconds, profile_color = row if row else (None, None, 0, 0, None)
    message_count = message_count or 0
    voice_seconds = voice_seconds or 0

    xp = compute_xp(message_count, voice_seconds)
    level, xp_into_level, xp_needed = level_progress(xp)

    embed = discord.Embed(
        title=f"Profil de {member.display_name}",
        description=bio if bio else "*Aucune bio définie.*",
        color=profile_color if profile_color else EMBED_COLOR,
    )

    # Photo de profil personnalisée si définie, sinon avatar Discord
    embed.set_thumbnail(url=avatar_url or member.display_avatar.url)

    embed.add_field(
        name="🏆 Niveau",
        value=f"**Niveau {level}**\n{xp_into_level}/{xp_needed} XP",
        inline=True,
    )
    embed.add_field(name="💬 Messages envoyés", value=str(message_count), inline=True)
    embed.add_field(name="🎙️ Temps en vocal", value=format_duration(voice_seconds), inline=True)

    if links:
        liens_txt = "\n".join(f"🔗 [{label}]({url})" for label, url in links)
        embed.add_field(name="Liens utiles", value=liens_txt, inline=False)

    embed.set_footer(text=f"Membre depuis le serveur • {member.guild.name}")
    return embed


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


class LinkAddModal(discord.ui.Modal, title="Ajouter un lien"):
    nom_input = discord.ui.TextInput(
        label="Nom du lien", max_length=50, placeholder="ex: Twitter, Portfolio, Twitch..."
    )
    url_input = discord.ui.TextInput(
        label="URL", max_length=200, placeholder="https://..."
    )

    def __init__(self, guild_id: int, user_id: int):
        super().__init__()
        self.guild_id = guild_id
        self.user_id = user_id

    async def on_submit(self, interaction: discord.Interaction):
        nom = str(self.nom_input.value).strip()
        url = str(self.url_input.value).strip()

        if not URL_REGEX.match(url):
            await interaction.response.send_message(
                "L'URL doit commencer par http:// ou https://", ephemeral=True
            )
            return

        ok, msg = add_link(self.guild_id, self.user_id, nom, url)
        if not ok:
            await interaction.response.send_message(msg, ephemeral=True)
            return
        await interaction.response.send_message(f"Lien **{nom}** ajouté ✅", ephemeral=True)


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
                label="Ajouter un lien", value="lien_ajouter", emoji="🔗",
                description=f"Max {MAX_LINKS} liens (Twitter, portfolio...)",
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

        elif choice == "lien_ajouter":
            await interaction.response.send_modal(LinkAddModal(self.guild_id, self.user_id))

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
                "⚠️ Es-tu sûr de vouloir supprimer tout ton profil (bio, photo, couleur, liens, statistiques) ? "
                "Cette action est irréversible.",
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


# ---- /profil classement -------------------------------------------------------

@profil_group.command(name="classement", description="Affiche le top 10 du serveur")
@app_commands.describe(critere="Le critère de classement (par défaut : niveau/XP)")
@app_commands.choices(
    critere=[
        app_commands.Choice(name="Niveau / XP", value="xp"),
        app_commands.Choice(name="Messages envoyés", value="messages"),
        app_commands.Choice(name="Temps en vocal", value="vocal"),
    ]
)
async def profil_classement(
    interaction: discord.Interaction, critere: app_commands.Choice[str] = None
):
    key = critere.value if critere else "xp"
    rows = fetch_all_profiles(interaction.guild_id)

    if not rows:
        await interaction.response.send_message(
            "Aucun profil n'a encore été créé sur ce serveur.", ephemeral=True
        )
        return

    enriched = []
    for user_id, message_count, voice_seconds in rows:
        message_count = message_count or 0
        voice_seconds = voice_seconds or 0
        xp = compute_xp(message_count, voice_seconds)
        level = compute_level(xp)
        enriched.append((user_id, message_count, voice_seconds, xp, level))

    sort_key = {"messages": 1, "vocal": 2, "xp": 3}[key]
    enriched.sort(key=lambda r: r[sort_key], reverse=True)
    top = enriched[:10]

    medals = ["🥇", "🥈", "🥉"]
    lines = []
    for i, (user_id, message_count, voice_seconds, xp, level) in enumerate(top):
        member = interaction.guild.get_member(user_id)
        name = member.display_name if member else f"Utilisateur inconnu ({user_id})"
        rank_icon = medals[i] if i < len(medals) else f"`#{i + 1}`"

        if key == "messages":
            value = f"{message_count} messages"
        elif key == "vocal":
            value = format_duration(voice_seconds)
        else:
            value = f"Niveau {level} — {xp} XP"

        lines.append(f"{rank_icon} **{name}** — {value}")

    titles = {
        "xp": "🏆 Classement du serveur — Niveau / XP",
        "messages": "💬 Classement du serveur — Messages",
        "vocal": "🎙️ Classement du serveur — Temps en vocal",
    }
    embed = discord.Embed(title=titles[key], description="\n".join(lines), color=EMBED_COLOR)
    await interaction.response.send_message(embed=embed)


bot.tree.add_command(profil_group)


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
