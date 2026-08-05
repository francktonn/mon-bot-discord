"""
Bot Discord de "Profil"
========================

Permet à chaque membre d'un serveur de se créer une petite fiche de profil :
- une photo de profil (image)
- une bio (texte)
- des liens utiles (ex: Twitter, portfolio, twitch...)
- un compteur du nombre de messages envoyés sur le serveur

Commandes slash (/) :
    /profil bio <texte>              -> définir/modifier sa bio
    /profil photo <image>            -> définir sa photo de profil
    /profil lien-ajouter <nom> <url> -> ajouter un lien (max 5)
    /profil lien-supprimer <nom>     -> supprimer un lien
    /profil voir [membre]            -> afficher le profil (le sien ou celui d'un autre membre)
    /profil supprimer                -> supprimer entièrement son profil

Le compteur de messages s'incrémente automatiquement à chaque message envoyé
par un membre sur le serveur (les messages des bots ne sont pas comptés).

Les données sont stockées dans une base SQLite locale (profiles.db),
un profil par (serveur, utilisateur) donc chaque serveur a ses propres profils.
"""

import os
import sqlite3
import re
from contextlib import closing

import discord
from discord import app_commands
from discord.ext import commands
from dotenv import load_dotenv

load_dotenv()

TOKEN = os.getenv("DISCORD_TOKEN")
DB_PATH = os.path.join(os.path.dirname(__file__), "profiles.db")
MAX_LINKS = 5
MAX_BIO_LEN = 500
EMBED_COLOR = 0x5865F2  # blurple

URL_REGEX = re.compile(r"^https?://[^\s]+$", re.IGNORECASE)


# ---------------------------------------------------------------------------
# Base de données
# ---------------------------------------------------------------------------

def init_db():
    with closing(sqlite3.connect(DB_PATH)) as db:
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS profiles (
                guild_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                bio TEXT,
                avatar_url TEXT,
                message_count INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (guild_id, user_id)
            )
            """
        )
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS links (
                guild_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                label TEXT NOT NULL,
                url TEXT NOT NULL,
                position INTEGER NOT NULL,
                PRIMARY KEY (guild_id, user_id, label)
            )
            """
        )
        # Migration légère pour les bases créées avec une version antérieure du bot
        # (ajoute la colonne message_count si elle n'existe pas encore).
        existing_columns = {row[1] for row in db.execute("PRAGMA table_info(profiles)")}
        if "message_count" not in existing_columns:
            db.execute("ALTER TABLE profiles ADD COLUMN message_count INTEGER NOT NULL DEFAULT 0")
        db.commit()


def get_or_create_profile(guild_id: int, user_id: int):
    with closing(sqlite3.connect(DB_PATH)) as db:
        db.execute(
            "INSERT OR IGNORE INTO profiles (guild_id, user_id) VALUES (?, ?)",
            (guild_id, user_id),
        )
        db.commit()


def update_field(guild_id: int, user_id: int, field: str, value: str):
    get_or_create_profile(guild_id, user_id)
    with closing(sqlite3.connect(DB_PATH)) as db:
        db.execute(
            f"UPDATE profiles SET {field} = ? WHERE guild_id = ? AND user_id = ?",
            (value, guild_id, user_id),
        )
        db.commit()


def fetch_profile(guild_id: int, user_id: int):
    with closing(sqlite3.connect(DB_PATH)) as db:
        cur = db.execute(
            "SELECT bio, avatar_url, message_count FROM profiles WHERE guild_id = ? AND user_id = ?",
            (guild_id, user_id),
        )
        row = cur.fetchone()
        cur = db.execute(
            "SELECT label, url FROM links WHERE guild_id = ? AND user_id = ? ORDER BY position ASC",
            (guild_id, user_id),
        )
        links = cur.fetchall()
    return row, links


def increment_message_count(guild_id: int, user_id: int):
    get_or_create_profile(guild_id, user_id)
    with closing(sqlite3.connect(DB_PATH)) as db:
        db.execute(
            "UPDATE profiles SET message_count = message_count + 1 WHERE guild_id = ? AND user_id = ?",
            (guild_id, user_id),
        )
        db.commit()


def add_link(guild_id: int, user_id: int, label: str, url: str) -> tuple[bool, str]:
    get_or_create_profile(guild_id, user_id)
    with closing(sqlite3.connect(DB_PATH)) as db:
        cur = db.execute(
            "SELECT COUNT(*) FROM links WHERE guild_id = ? AND user_id = ?",
            (guild_id, user_id),
        )
        count = cur.fetchone()[0]
        cur = db.execute(
            "SELECT 1 FROM links WHERE guild_id = ? AND user_id = ? AND label = ?",
            (guild_id, user_id, label),
        )
        exists = cur.fetchone() is not None

        if not exists and count >= MAX_LINKS:
            return False, f"Tu as déjà atteint le maximum de {MAX_LINKS} liens. Supprime-en un d'abord."

        db.execute(
            """
            INSERT INTO links (guild_id, user_id, label, url, position)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(guild_id, user_id, label) DO UPDATE SET url = excluded.url
            """,
            (guild_id, user_id, label, url, count),
        )
        db.commit()
    return True, "ok"


def remove_link(guild_id: int, user_id: int, label: str) -> bool:
    with closing(sqlite3.connect(DB_PATH)) as db:
        cur = db.execute(
            "DELETE FROM links WHERE guild_id = ? AND user_id = ? AND label = ?",
            (guild_id, user_id, label),
        )
        db.commit()
        return cur.rowcount > 0


def delete_profile(guild_id: int, user_id: int):
    with closing(sqlite3.connect(DB_PATH)) as db:
        db.execute("DELETE FROM profiles WHERE guild_id = ? AND user_id = ?", (guild_id, user_id))
        db.execute("DELETE FROM links WHERE guild_id = ? AND user_id = ?", (guild_id, user_id))
        db.commit()


# ---------------------------------------------------------------------------
# Bot
# ---------------------------------------------------------------------------

intents = discord.Intents.default()
bot = commands.Bot(command_prefix="!", intents=intents)

profil_group = app_commands.Group(name="profil", description="Gère ton profil sur ce serveur")


def build_profile_embed(member: discord.Member, row, links) -> discord.Embed:
    bio, avatar_url, message_count = row if row else (None, None, 0)

    embed = discord.Embed(
        title=f"Profil de {member.display_name}",
        description=bio if bio else "*Aucune bio définie.*",
        color=EMBED_COLOR,
    )

    # Photo de profil personnalisée si définie, sinon avatar Discord
    embed.set_thumbnail(url=avatar_url or member.display_avatar.url)

    embed.add_field(name="💬 Messages envoyés", value=str(message_count or 0), inline=False)

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


@bot.event
async def on_message(message: discord.Message):
    # On ignore les messages privés et ceux envoyés par des bots (dont ce bot lui-même)
    if message.guild is not None and not message.author.bot:
        increment_message_count(message.guild.id, message.author.id)
    await bot.process_commands(message)


# ---- /profil bio ----------------------------------------------------------

@profil_group.command(name="bio", description="Définit ou modifie ta bio")
@app_commands.describe(texte="Le texte de ta bio (max 500 caractères)")
async def profil_bio(interaction: discord.Interaction, texte: str):
    if len(texte) > MAX_BIO_LEN:
        await interaction.response.send_message(
            f"Ta bio est trop longue ({len(texte)}/{MAX_BIO_LEN} caractères).", ephemeral=True
        )
        return
    update_field(interaction.guild_id, interaction.user.id, "bio", texte)
    await interaction.response.send_message("Ta bio a été mise à jour ✅", ephemeral=True)


# ---- /profil photo ---------------------------------------------------------

@profil_group.command(name="photo", description="Définit ta photo de profil")
@app_commands.describe(image="L'image à utiliser comme photo de profil")
async def profil_photo(interaction: discord.Interaction, image: discord.Attachment):
    if not image.content_type or not image.content_type.startswith("image/"):
        await interaction.response.send_message("Le fichier envoyé n'est pas une image.", ephemeral=True)
        return
    update_field(interaction.guild_id, interaction.user.id, "avatar_url", image.url)
    await interaction.response.send_message("Ta photo de profil a été mise à jour ✅", ephemeral=True)


# ---- /profil lien-ajouter ---------------------------------------------------

@profil_group.command(name="lien-ajouter", description="Ajoute un lien à ton profil (max 5)")
@app_commands.describe(nom="Le nom affiché du lien (ex: Twitter)", url="L'URL complète (https://...)")
async def profil_lien_ajouter(interaction: discord.Interaction, nom: str, url: str):
    if not URL_REGEX.match(url):
        await interaction.response.send_message(
            "L'URL doit commencer par http:// ou https://", ephemeral=True
        )
        return
    if len(nom) > 50:
        await interaction.response.send_message("Le nom du lien est trop long (max 50 caractères).", ephemeral=True)
        return

    ok, msg = add_link(interaction.guild_id, interaction.user.id, nom, url)
    if not ok:
        await interaction.response.send_message(msg, ephemeral=True)
        return
    await interaction.response.send_message(f"Lien **{nom}** ajouté ✅", ephemeral=True)


# ---- /profil lien-supprimer -------------------------------------------------

@profil_group.command(name="lien-supprimer", description="Supprime un lien de ton profil")
@app_commands.describe(nom="Le nom exact du lien à supprimer")
async def profil_lien_supprimer(interaction: discord.Interaction, nom: str):
    removed = remove_link(interaction.guild_id, interaction.user.id, nom)
    if removed:
        await interaction.response.send_message(f"Lien **{nom}** supprimé ✅", ephemeral=True)
    else:
        await interaction.response.send_message(f"Aucun lien nommé **{nom}** trouvé.", ephemeral=True)


# ---- /profil voir -----------------------------------------------------------

@profil_group.command(name="voir", description="Affiche ton profil ou celui d'un autre membre")
@app_commands.describe(membre="Le membre dont tu veux voir le profil (optionnel)")
async def profil_voir(interaction: discord.Interaction, membre: discord.Member = None):
    target = membre or interaction.user
    row, links = fetch_profile(interaction.guild_id, target.id)

    if row is None and not links:
        if target == interaction.user:
            await interaction.response.send_message(
                "Tu n'as pas encore de profil. Utilise `/profil bio`, `/profil photo` etc. pour en créer un !",
                ephemeral=True,
            )
        else:
            await interaction.response.send_message(
                f"{target.display_name} n'a pas encore de profil.", ephemeral=True
            )
        return

    embed = build_profile_embed(target, row, links)
    await interaction.response.send_message(embed=embed)


# ---- /profil supprimer -------------------------------------------------------

@profil_group.command(name="supprimer", description="Supprime entièrement ton profil")
async def profil_supprimer(interaction: discord.Interaction):
    delete_profile(interaction.guild_id, interaction.user.id)
    await interaction.response.send_message("Ton profil a été supprimé.", ephemeral=True)


bot.tree.add_command(profil_group)


if __name__ == "__main__":
    if not TOKEN:
        raise SystemExit(
            "Erreur : la variable d'environnement DISCORD_TOKEN n'est pas définie.\n"
            "Copie .env.example en .env et renseigne ton token de bot."
        )
    bot.run(TOKEN)
