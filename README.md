# jellysync

Share your Jellyfin library with friends' Jellyfin servers — without copying files, without giving anyone your server login, and without exposing anything but one small port.

## What does it actually do?

Say you and a friend each run your own Jellyfin server. Normally, if you want to watch something that's only on their server, you're stuck asking them to copy the file over or share their whole Jellyfin login.

jellysync is a small helper that runs quietly next to your Jellyfin. It talks to your friend's helper, and the two of them swap lists of "here's what I have." For anything you don't already have yourself, jellysync drops a tiny pointer file into your Jellyfin library — so the movie or show just *shows up*, like it was always there. When you hit play, jellysync fetches the video from your friend's server in the background and hands it straight to your Jellyfin. You never notice the difference.

If you already have your own copy of something, your copy always wins — jellysync never overrides or duplicates what's already on your server.

**What jellysync is not:** it doesn't touch your Jellyfin login or API key on anyone else's machine, it doesn't open your whole server to the internet, and it doesn't require you to manually copy a single file.

## How it fits together

```
Your Jellyfin  <──>  your jellysync  <── (private network) ──>  friend's jellysync  <──>  friend's Jellyfin
```

- Your jellysync only ever talks to *your* Jellyfin using *your* API key. That key never leaves your machine.
- Your jellysync only talks to jellysync nodes you've explicitly added as peers — never to their Jellyfin directly.
- Everything runs in Docker, one container per Jellyfin instance.

## Before you start

- **Docker and Docker Compose** installed.
- **A Jellyfin server** — either one you already run, or use the one included in the compose file to set up a fresh one.
- **A private network to your peers.** jellysync assumes you and your friends are already reachable on some shared network — a VPN like Tailscale, WireGuard, or ZeroTier works great. jellysync does **not** do NAT traversal or port forwarding for you; if your peer's address isn't reachable, nothing will sync.

## Quick start

### 1. Get the project

```sh
git clone <this repo> jellysync && cd jellysync
```

Open `docker-compose.yml` and give your node a name — anything unique works, e.g. your name:

```yaml
- NODE_ID=stefano
```

### 2. Start it up

```sh
docker compose up -d
```

This starts both Jellyfin and jellysync. (Already running your own Jellyfin elsewhere? Remove the `jellyfin` block from `docker-compose.yml` and point `JELLYFIN_URL` at your existing server instead.)

jellysync will exit immediately with `JELLYFIN_API_KEY is required` — expected at this point, since you haven't done step 3 yet.

### 3. Get a Jellyfin API key

1. Open your Jellyfin, sign in as an admin.
2. Go to the **Dashboard** (gear icon, top right) → **Advanced** → **API Keys**.
3. Click **+**, name it "jellysync", confirm.
4. Copy the key it gives you.

Paste it into `docker-compose.yml`:

```yaml
- JELLYFIN_API_KEY=paste-it-here
```

Then restart jellysync so it picks up the change:

```sh
docker compose up -d
```

### 4. Open the dashboard

Visit `http://<this-machine>:8080` in a browser. You'll see your node's status, an empty peers list, and an empty catalog — that's expected, you haven't added anyone yet.

### 5. Add a friend

On the dashboard, use the **Peers** box: type an id (anything, e.g. their name) and their jellysync address — something like `http://their-vpn-address:8080` — and hit **add**.

Do the same on their side, pointing back at you. Once both sides have added each other, give it about a minute: jellysync syncs catalogs on a 60-second cycle. Anything they have that you don't will show up in your Jellyfin library automatically, ready to play.

## Configuration reference

Everything is configured as environment variables on the `jellysync` service in `docker-compose.yml` — there's no separate config file. After changing any of these, run `docker compose up -d` to apply them.

| Variable | What it means |
|---|---|
| `NODE_ID` | A name for this node. Must be unique among your peers. |
| `JELLYFIN_URL` | Address of *your* Jellyfin, as seen from inside the jellysync container. |
| `JELLYFIN_API_KEY` | The API key from step 3 above. Never shared with peers. |
| `DB_PATH` | Where jellysync stores its database inside the container. Defaults to `./data/jellysync.db`; the included compose file sets it to `/data/jellysync.db`, which lands in the mounted `./jellysync/data` folder. |
| `OUTPUT_DIR` | Folder jellysync writes pointer files into. Must be a folder your Jellyfin also has mounted, as a library. |
| `BASE_URL` | The address *your own* Jellyfin uses to reach jellysync. If you used the included `docker-compose.yml` as-is, leave this at `http://jellysync:8080`. |
| `PEERS` | Optional — a starting list of peers as `id=url,id=url`, in case you'd rather edit this than use the dashboard. The dashboard's Peers list is the same data either way, and additions there persist across restarts even if they're not set here. |

jellysync always listens on `:8080` inside its container — to use a different port on your host, change the host side of the `ports` mapping (e.g. `"9000:8080"`), not the container side.

## Good to know (v1 limitations)

- **No built-in authentication between peers yet.** jellysync trusts anything reachable at a peer's configured address — this is why a private network (VPN) between you and your peers isn't optional, it's the security boundary for now.
- **Peers must be added directly on both sides** — there's no relaying through a peer's peers yet. If you and two friends want to share with each other, all three of you add each other directly.
- **If a peer's copy stops mid-stream, playback just fails** rather than automatically switching to another copy (only relevant if the same movie exists on more than one peer).

## Troubleshooting

- **Nothing shows up after adding a peer.** Give it up to 60 seconds (the sync interval). If it's still empty, check the peer's status in the dashboard — it should say `ONLINE`. If it says `OFFLINE`, double check the address you entered and that your VPN connection to them is actually up.
- **jellysync won't start.** Check `docker compose logs jellysync` — it fails fast with a clear message if it can't reach your Jellyfin or if a required environment variable is missing.
