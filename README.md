# JerBot-Discord-Bot

Proudly vibe-coded.

## Building and running

To run this app you must have a docker engine installed on your system.

To run the bot you must have a .env file created in the folder containing docker-compose.yml file.
In the .env file you should provide the app tokens and server/channel id in the same format as in .env.example file.

To build and start the container run the command:
```
docker compose up -d --build
```
On subsequent starts (no code changes) you can omit `--build`:
```
docker compose up -d
```

## Commands

The bot only responds to commands in the channel set as `ALLOWED_CHANNEL_ID`.

| Command | Description |
|---|---|
| `/play <url>` | Add a YouTube link to the queue and start playing |
| `/skip` | Skip the current song and play the next in queue |
| `/stop` | Stop playback, clear the queue and disconnect |
| `/surprise <url>` | Queue a song without revealing the title to the channel |
