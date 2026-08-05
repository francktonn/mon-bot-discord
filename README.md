# Bot Discord de Profil

Un bot permettant à chaque membre de créer une fiche de profil sur ton serveur :
photo de profil, bannière, bio et liens utiles (Twitter, Twitch, portfolio...).

## 1. Créer l'application Discord

1. Va sur https://discord.com/developers/applications et clique sur **New Application**.
2. Donne-lui un nom, puis va dans l'onglet **Bot** (menu de gauche).
3. Clique sur **Reset Token** (ou **Add Bot**) pour générer un token, et copie-le.
   ⚠️ Ne partage jamais ce token publiquement.
4. Toujours dans l'onglet **Bot**, aucune intent privilégiée n'est nécessaire pour ce bot
   (il n'utilise ni le contenu des messages, ni la liste des membres).

## 2. Inviter le bot sur ton serveur

1. Va dans l'onglet **OAuth2 > URL Generator**.
2. Coche les scopes `bot` et `applications.commands`.
3. Dans les permissions du bot, coche :
   - `Send Messages`
   - `Embed Links`
   - `Attach Files` (facultatif, utile pour l'avenir)
4. Copie l'URL générée en bas de page, ouvre-la dans ton navigateur, et invite le bot
   sur ton serveur.

## 3. Installer et lancer le bot

```bash
# Se placer dans le dossier du projet
cd profile_bot

# (Optionnel mais recommandé) créer un environnement virtuel
python -m venv venv
source venv/bin/activate        # sous Windows : venv\Scripts\activate

# Installer les dépendances
pip install -r requirements.txt

# Configurer le token
cp .env.example .env
# puis ouvre .env et colle ton token à la place de "colle_ton_token_ici"

# Lancer le bot
python bot.py
```

Si tout fonctionne, tu verras dans le terminal :
```
TonBot#1234 est connecté. X commandes synchronisées.
```

Note : la première synchronisation des commandes slash peut prendre jusqu'à
une heure pour apparaître partout si tu as beaucoup de serveurs ; en pratique,
sur un seul serveur ça apparaît généralement en quelques secondes à quelques minutes.

## 4. Commandes disponibles

Toutes les commandes sont des commandes slash, tape `/` dans Discord pour les voir.

| Commande | Description |
|---|---|
| `/profil bio <texte>` | Définit ou modifie ta bio (max 500 caractères) |
| `/profil photo <image>` | Définit ta photo de profil (upload d'une image) |
| `/profil banniere <image>` | Définit ta bannière (upload d'une image) |
| `/profil lien-ajouter <nom> <url>` | Ajoute un lien utile (max 5 liens) |
| `/profil lien-supprimer <nom>` | Supprime un lien par son nom |
| `/profil voir [membre]` | Affiche ton profil, ou celui d'un membre mentionné |
| `/profil supprimer` | Supprime entièrement ton profil |

### Exemple d'utilisation

```
/profil bio texte: Développeur passionné 🚀, j'adore le pixel art et le café.
/profil photo image: (upload d'une image)
/profil banniere image: (upload d'une image)
/profil lien-ajouter nom: Twitter url: https://twitter.com/moncompte
/profil lien-ajouter nom: Portfolio url: https://monsite.com
/profil voir
```

D'autres membres peuvent voir ton profil avec `/profil voir membre: @toi`.

## 5. Stockage des données

Les profils sont stockés dans un fichier SQLite local `profiles.db`, créé
automatiquement au premier lancement, dans le même dossier que `bot.py`.
Chaque profil est lié à un couple (serveur, utilisateur) : un même utilisateur
peut donc avoir un profil différent sur chaque serveur où le bot est présent.

⚠️ Pense à sauvegarder ce fichier si tu héberges le bot durablement (ou migre
vers une vraie base de données comme PostgreSQL si tu veux un hébergement plus
robuste — je peux t'aider à faire cette migration si besoin).

## 6. Hébergement

Pour que le bot reste en ligne 24/7, tu peux l'héberger sur :
- un petit VPS (ex : OVH, Hetzner) avec un service `systemd` ou `screen`/`tmux`,
- une plateforme comme Railway, Render ou Fly.io (souvent avec un plan gratuit limité).

## 7. Idées d'amélioration possibles

- Ajouter des couleurs de profil personnalisables.
- Ajouter un système de badges/rôles affichés sur le profil.
- Permettre l'édition via un menu déroulant / boutons plutôt que des commandes séparées.
- Ajouter une commande `/profil export` pour exporter son profil en JSON.

N'hésite pas à me demander si tu veux que j'ajoute une de ces fonctionnalités !
