require('dotenv').config();

const { Client, GatewayIntentBits, REST, Routes, SlashCommandBuilder, MessageFlags } = require('discord.js');
const {
  joinVoiceChannel,
  createAudioPlayer,
  createAudioResource,
  AudioPlayerStatus,
  entersState,
  VoiceConnectionStatus,
  StreamType,
} = require('@discordjs/voice');
const { Readable } = require('stream');

const { DISCORD_TOKEN, CLIENT_ID, GUILD_ID, ALLOWED_CHANNEL_ID } = process.env;

// youtubei.js/bgutils-js are ESM-only; loaded via dynamic import in main() below.
let yt;
let YTNodes;
let webPoMinter;
let userAgent;

const client = new Client({
  intents: [GatewayIntentBits.Guilds, GatewayIntentBits.GuildVoiceStates],
});

const commands = [
  new SlashCommandBuilder()
    .setName('play')
    .setDescription('Add a YouTube link or search query to the queue and start playing')
    .addStringOption(o => o.setName('query').setDescription('YouTube URL or search query').setRequired(true))
    .toJSON(),
  new SlashCommandBuilder()
    .setName('skip')
    .setDescription('Skip the current song')
    .toJSON(),
  new SlashCommandBuilder()
    .setName('pause')
    .setDescription('Pause the current song')
    .toJSON(),
  new SlashCommandBuilder()
    .setName('resume')
    .setDescription('Resume the paused song')
    .toJSON(),
  new SlashCommandBuilder()
    .setName('spierdalaj')
    .setDescription('Skips the current song and apologizes for playing it')
    .toJSON(),
  new SlashCommandBuilder()
    .setName('stop')
    .setDescription('Stop playback, clear queue and disconnect')
    .toJSON(),
  new SlashCommandBuilder()
    .setName('surprise')
    .setDescription('Play a song without revealing what it is')
    .addStringOption(o => o.setName('query').setDescription('YouTube URL or search query').setRequired(true))
    .toJSON(),
];

// guildId -> { player, connection, queue, stream, currentTrack, textChannel }
const sessions = new Map();
// guildId -> in-flight getOrCreateSession() promise, so concurrent calls don't create duplicate connections
const pendingSessionCreations = new Map();

async function resolveVideoId(input) {
  const trimmed = input.trim();

  if (trimmed.startsWith('http://') || trimmed.startsWith('https://')) {
    let parsed;
    try {
      parsed = new URL(trimmed);
    } catch {
      throw new Error('Invalid URL.');
    }
    if (parsed.hostname === 'youtu.be') {
      return parsed.pathname.slice(1);
    }
    if (parsed.hostname.includes('youtube.com')) {
      if (parsed.searchParams.has('v')) {
        return parsed.searchParams.get('v');
      }
      const shortsMatch = parsed.pathname.match(/^\/shorts\/([^/]+)/);
      if (shortsMatch) {
        return shortsMatch[1];
      }
    }
    throw new Error('Unsupported YouTube URL.');
  }

  const search = await yt.search(trimmed, { type: 'video' });
  const video = search.results.firstOfType(YTNodes.Video);
  if (!video) {
    throw new Error('No results found for that search.');
  }
  return video.video_id;
}

function fmtDuration(secs) {
  if (!secs) return '?:??';
  const m = Math.floor(secs / 60);
  const s = secs % 60;
  return `${m}:${s.toString().padStart(2, '0')}`;
}

const IDLE_TIMEOUT_MS = 5 * 60 * 1000;

function scheduleIdleDisconnect(guildId) {
  const session = sessions.get(guildId);
  if (!session) return;

  // Clear any existing timer
  clearTimeout(session.idleTimer);

  // Set new timer only if player is idle and queue is empty
  if (session.player.state.status === AudioPlayerStatus.Idle && session.queue.length === 0) {
    session.idleTimer = setTimeout(() => {
      const s = sessions.get(guildId);
      if (s) {
        // Double-check the conditions before disconnecting
        const isPlayerIdle = s.player.state.status === AudioPlayerStatus.Idle;
        const isQueueEmpty = s.queue.length === 0;

        if (isPlayerIdle && isQueueEmpty) {
          s.textChannel.send('No songs played for 5 minutes, disconnecting.').catch(() => {});
          s.stream?.destroy();
          s.player.stop(true);
          s.connection.destroy();
          sessions.delete(guildId);
        }
      }
    }, IDLE_TIMEOUT_MS);
  }
}

// YTMUSIC only exposes streaming formats for content YouTube Music actually carries;
// non-music videos (movie clips, VODs, etc.) come back with no streaming data at all.
// Fall back to IOS, which serves formats for regular videos too.
const STREAM_CLIENTS = ['YTMUSIC', 'IOS'];

async function chooseFormatWithFallback(videoId) {
  let lastErr;
  for (const client of STREAM_CLIENTS) {
    try {
      const info = await yt.getBasicInfo(videoId, { client });
      return info.chooseFormat({ quality: 'best', type: 'audio' });
    } catch (err) {
      lastErr = err;
      if (err?.info?.error_type !== 'NO_STREAMING_DATA') throw err;
    }
  }
  throw lastErr;
}

async function getAudioStream(videoId) {
  if (!webPoMinter) {
    throw new Error('PO token minter is not ready.');
  }

  const contentPoToken = await webPoMinter.mintAsWebsafeString(videoId);
  const format = await chooseFormatWithFallback(videoId);
  const decipheredUrl = await format.decipher(yt.session.player);
  const baseUrl = `${decipheredUrl}&pot=${contentPoToken}`;
  const totalBytes = format.content_length;

  // youtubei.js's own internal downloader never does a single plain fetch for
  // audio-only formats - it always pages through the file in chunks via a
  // "&range=start-end" query param (not an HTTP Range header). A single unranged
  // fetch only gets the CDN's default truncated initial chunk, not the full track.
  const CHUNK_SIZE = 1048576 * 10; // 10MB, matches youtubei.js's own chunk size
  const chunks = [];
  let start = 0;

  const MAX_CHUNK_ATTEMPTS = 3;

  while (!totalBytes || start < totalBytes) {
    const end = start + CHUNK_SIZE;
    let chunk;

    for (let attempt = 1; ; attempt++) {
      // A stalled connection (as opposed to a cleanly closed one) would otherwise hang
      // this forever with no error - abort it so a stall fails loudly instead of
      // silently jamming the guild's playback.
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(new Error('Timed out downloading audio.')), 30_000);

      try {
        const res = await fetch(`${baseUrl}&range=${start}-${end}`, {
          headers: { 'user-agent': userAgent },
          signal: controller.signal,
        });
        if (!res.ok) {
          throw new Error(`Stream fetch failed with status ${res.status}`);
        }
        chunk = Buffer.from(await res.arrayBuffer());
        break;
      } catch (err) {
        // Transient stalls/network blips are common on long tracks; retry a few
        // times before giving up on the whole download.
        if (attempt >= MAX_CHUNK_ATTEMPTS) throw err;
      } finally {
        clearTimeout(timeout);
      }
    }

    if (chunk.length === 0) break;
    chunks.push(chunk);
    start = end + 1;
    // No known total size: a short read means the server has nothing left to send.
    if (!totalBytes && chunk.length < CHUNK_SIZE) break;
  }

  return Readable.from(Buffer.concat(chunks));
}

async function playNext(guildId) {
  const session = sessions.get(guildId);
  if (!session) return;

  // Check if we should skip playing because there are no tracks or player is already playing
  if (session.queue.length === 0) {
    // If the player is not idle, don't schedule disconnection yet
    if (session.player.state.status !== AudioPlayerStatus.Idle) {
      return;
    }
    scheduleIdleDisconnect(guildId);
    return;
  }

  clearTimeout(session.idleTimer);
  const track = session.queue.shift();
  session.stream?.destroy();

  let stream;
  try {
    stream = await getAudioStream(track.videoId);
  } catch (err) {
    console.error(`Failed to get audio stream for ${track.videoId}:`, err.message);
    session.textChannel.send(`Failed to stream **${track.title}**, skipping.`).catch(() => {});
    return playNext(guildId);
  }

  stream.on('error', err => {
    console.error(`Audio stream error for ${track.videoId}:`, err.message);
    const s = sessions.get(guildId);
    if (s && s.stream === stream) {
      s.textChannel.send('Failed to stream that track, skipping.').catch(() => {});
      playNext(guildId);
    }
  });

  session.stream = stream;
  session.currentTrack = track;
  session.player.play(createAudioResource(stream, { inputType: StreamType.Arbitrary }));

  const msg = track.surprise
    ? 'Surprise song incoming! What could it be...'
    : `Now playing: **${track.title}** [${fmtDuration(track.duration)}]`;
  session.textChannel.send(msg).catch(() => {});
}

async function getOrCreateSession(interaction, voiceChannel) {
  const existing = sessions.get(interaction.guildId);
  if (existing) return existing;

  // If another call is already creating a session for this guild, wait for
  // it instead of racing to open a second voice connection.
  const pending = pendingSessionCreations.get(interaction.guildId);
  if (pending) return pending;

  const creation = (async () => {
    const connection = joinVoiceChannel({
      channelId: voiceChannel.id,
      guildId: interaction.guildId,
      adapterCreator: interaction.guild.voiceAdapterCreator,
    });

    await entersState(connection, VoiceConnectionStatus.Ready, 15_000).catch(() => {
      connection.destroy();
      throw new Error('Could not connect to voice channel.');
    });

    const player = createAudioPlayer();
    player.on(AudioPlayerStatus.Idle, () => {
      // Ensure we only call playNext when the session still exists
      const session = sessions.get(interaction.guildId);
      if (session) {
        playNext(interaction.guildId);
      }
    });
    player.on('error', err => {
      console.error('Player error:', err.message);
      // Ensure we only call playNext when the session still exists
      const session = sessions.get(interaction.guildId);
      if (session) {
        playNext(interaction.guildId);
      }
    });
    connection.subscribe(player);

    const session = {
      player,
      connection,
      queue: [],
      stream: null,
      currentTrack: null,
      textChannel: interaction.channel,
      idleTimer: null
    };
    sessions.set(interaction.guildId, session);
    return session;
  })();

  pendingSessionCreations.set(interaction.guildId, creation);
  try {
    return await creation;
  } finally {
    pendingSessionCreations.delete(interaction.guildId);
  }
}

async function handleQueue(interaction, surprise) {
  const input = interaction.options.getString('query');
  const voiceChannel = interaction.member?.voice?.channel;

  if (!voiceChannel) {
    return interaction.reply({ content: 'You need to join a voice channel first!', flags: MessageFlags.Ephemeral });
  }

  await interaction.deferReply();

  try {
    const videoId = await resolveVideoId(input);
    const info = await yt.getBasicInfo(videoId);
    const track = {
      videoId,
      title: info.basic_info.title ?? 'Unknown title',
      duration: info.basic_info.duration,
      surprise,
    };
    const session = await getOrCreateSession(interaction, voiceChannel);
    const isIdle = session.player.state.status === AudioPlayerStatus.Idle;
    session.queue.push(track);

    // Clear any existing idle timer when adding new tracks
    clearTimeout(session.idleTimer);

    if (isIdle) {
      await playNext(interaction.guildId);
      await interaction.editReply(
        surprise
          ? 'Surprise song incoming! What could it be...'
          : `Now playing: **${track.title}** [${fmtDuration(track.duration)}]`
      );
    } else {
      await interaction.editReply(
        surprise
          ? 'Surprise song added to the queue!'
          : `Added to queue (#${session.queue.length}): **${track.title}** [${fmtDuration(track.duration)}]`
      );
    }
  } catch (err) {
    console.error('Queue error:', err);
    await interaction.editReply('Failed to play that video. Make sure the link is public and try again.');
  }
}

async function registerCommands() {
  const rest = new REST({ version: '10' }).setToken(DISCORD_TOKEN);
  await rest.put(Routes.applicationGuildCommands(CLIENT_ID, GUILD_ID), { body: commands });
  console.log('Slash commands registered.');
}

client.once('clientReady', async () => {
  console.log(`Logged in as ${client.user.tag}`);
  await registerCommands().catch(err => {
    console.error('Failed to register commands:', err.message);
    console.error('Make sure the bot was invited with the applications.commands scope.');
  });
});

client.on('interactionCreate', async interaction => {
  if (!interaction.isChatInputCommand()) return;

  if (interaction.channelId !== ALLOWED_CHANNEL_ID) {
    return interaction.reply({
      content: 'This command can only be used in the designated music channel.',
      flags: MessageFlags.Ephemeral,
    });
  }

  const args = interaction.options.data.map(o => `${o.name}=${o.value}`).join(' ');
  console.log(`[${new Date().toISOString()}] ${interaction.user.tag} used /${interaction.commandName}${args ? ` ${args}` : ''}`);

  switch (interaction.commandName) {
    case 'play':
      return handleQueue(interaction, false);

    case 'surprise':
      return handleQueue(interaction, true);

    case 'spierdalaj': {
      const session = sessions.get(interaction.guildId);
      if (!session || session.player.state.status === AudioPlayerStatus.Idle) {
        return interaction.reply({ content: 'Nothing is currently playing.', flags: MessageFlags.Ephemeral });
      }
      session.stream?.destroy();
      session.player.stop(true);
      return interaction.reply(`I'm very sorry :(((`);
    }

    case 'skip': {
      const session = sessions.get(interaction.guildId);
      if (!session || session.player.state.status === AudioPlayerStatus.Idle) {
        return interaction.reply({ content: 'Nothing is currently playing.', flags: MessageFlags.Ephemeral });
      }
      const label = session.currentTrack?.surprise
        ? 'the surprise song'
        : `**${session.currentTrack?.title ?? 'current track'}**`;
      session.stream?.destroy();
      session.player.stop(true);
      const suffix = session.queue.length > 0 ? '' : ' Queue is empty, disconnecting.';
      return interaction.reply(`Skipped ${label}.${suffix}`);
    }

    case 'pause': {
      const session = sessions.get(interaction.guildId);
      const status = session?.player.state.status;
      if (!session || status === AudioPlayerStatus.Idle) {
        return interaction.reply({ content: 'Nothing is currently playing.', flags: MessageFlags.Ephemeral });
      }
      if (status === AudioPlayerStatus.Paused) {
        return interaction.reply({ content: 'Playback is already paused.', flags: MessageFlags.Ephemeral });
      }
      if (!session.player.pause()) {
        return interaction.reply({ content: 'Could not pause right now, try again in a moment.', flags: MessageFlags.Ephemeral });
      }
      return interaction.reply('Paused playback.');
    }

    case 'resume': {
      const session = sessions.get(interaction.guildId);
      const status = session?.player.state.status;
      if (!session || status === AudioPlayerStatus.Idle) {
        return interaction.reply({ content: 'Nothing is currently playing.', flags: MessageFlags.Ephemeral });
      }
      if (status !== AudioPlayerStatus.Paused) {
        return interaction.reply({ content: 'Playback is already playing.', flags: MessageFlags.Ephemeral });
      }
      if (!session.player.unpause()) {
        return interaction.reply({ content: 'Could not resume right now, try again in a moment.', flags: MessageFlags.Ephemeral });
      }
      return interaction.reply('Resumed playback.');
    }

    case 'stop': {
      const session = sessions.get(interaction.guildId);
      if (!session) {
        return interaction.reply({ content: 'Nothing is currently playing.', flags: MessageFlags.Ephemeral });
      }
      clearTimeout(session.idleTimer);
      session.queue.length = 0;
      session.player.stop(true);
      session.stream?.destroy();
      session.connection.destroy();
      sessions.delete(interaction.guildId);
      return interaction.reply('Stopped playback and disconnected.');
    }
  }
});

	client.on('voiceStateUpdate', (oldState, newState) => {
	  // Check if the bot was disconnected from a voice channel
	  if (oldState.id === client.user.id && oldState.channelId && !newState.channelId) {
	    const guildId = oldState.guild.id;
	    const session = sessions.get(guildId);
	    if (session) {
	      clearTimeout(session.idleTimer);
	      session.stream?.destroy();
	      session.player.stop(true);
	      session.connection.destroy();
	      sessions.delete(guildId);
	    }
	  }
	});

// YouTube requires a PO token to authorize actual media downloads (not just metadata).
// This solves the BotGuard attestation challenge and mints one, mirroring bgutils-js's
// own reference integration: https://github.com/LuanRT/BgUtils/blob/main/examples/index-innertube.ts
async function initPoTokenMinter() {
  try {
    const { BotGuardClient } = await import('bgutils-js/botguard');
    const { parseLooseJSON, buildURL, getHeaders, USER_AGENT } = await import('bgutils-js/utils');
    const { WebPoMinter } = await import('bgutils-js/webpo');
    const { JSDOM } = await import('jsdom');

    userAgent = USER_AGENT;

    const dom = new JSDOM('<!DOCTYPE html><html lang="en"><head><title></title></head><body></body></html>', {
      url: 'https://www.youtube.com',
      referrer: 'https://www.youtube.com/',
      userAgent: USER_AGENT,
    });

    const pageHtml = await (await fetch('https://www.youtube.com', {
      headers: {
        accept: '*/*',
        'accept-language': 'en-US,en;q=0.7',
        'user-agent': USER_AGENT,
      },
    })).text();

    const ytConfig = pageHtml.match(/ytcfg\.set\(({.+?})\);/s)?.[1];
    if (!ytConfig) {
      throw new Error('Could not find ytcfg in YouTube homepage HTML.');
    }
    dom.window.yt = { config_: JSON.parse(ytConfig) };

    Object.assign(globalThis, {
      yt: dom.window.yt,
      window: dom.window,
      document: dom.window.document,
      location: dom.window.location,
      origin: dom.window.origin,
    });
    if (!('navigator' in globalThis)) {
      Object.defineProperty(globalThis, 'navigator', { value: dom.window.navigator });
    }

    const initialAttestationData = pageHtml.match(/window\.ytAtN\(\s*({[\s\S]*?})\s*\)/);
    if (!initialAttestationData) {
      throw new Error('Could not find BotGuard challenge in YouTube homepage HTML.');
    }
    const challengeResponse = parseLooseJSON(initialAttestationData[1]).R;
    if (!challengeResponse?.bgChallenge) {
      throw new Error('Could not get BotGuard challenge.');
    }

    const interpreterUrl = challengeResponse.bgChallenge.interpreterUrl.privateDoNotAccessOrElseTrustedResourceUrlWrappedValue;
    const interpreterJavascript = await (await fetch(`https:${interpreterUrl}`)).text();
    if (!interpreterJavascript) {
      throw new Error('Could not load BotGuard VM.');
    }
    new Function(interpreterJavascript)();

    const botGuardClient = await BotGuardClient.create({
      program: challengeResponse.bgChallenge.program,
      globalName: challengeResponse.bgChallenge.globalName,
      globalObject: globalThis,
    });

    const requestKey = 'O43z0dpjhgX20SCx4KAo';
    const webPoSignalOutput = [];
    const botguardResponse = await botGuardClient.snapshot({ webPoSignalOutput });

    const integrityTokenResponse = await fetch(buildURL('GenerateIT', true), {
      method: 'POST',
      headers: getHeaders(),
      body: JSON.stringify([requestKey, botguardResponse]),
    });
    const [integrityToken, estimatedTtlSecs, mintRefreshThreshold, websafeFallbackToken] = await integrityTokenResponse.json();

    webPoMinter = await WebPoMinter.create(
      { integrityToken, estimatedTtlSecs, mintRefreshThreshold, websafeFallbackToken },
      webPoSignalOutput
    );

    console.log('PO token minter ready.');
  } catch (err) {
    console.error('Failed to initialize PO token minter (streaming will fail until this recovers):', err.message);
  }
}

async function main() {
  const youtubei = await import('youtubei.js');
  // youtubei.js ships no JS interpreter of its own; this is needed to decipher signed stream URLs.
  youtubei.Platform.shim.eval = async data => new Function(data.output)();
  yt = await youtubei.Innertube.create({ generate_session_locally: true });
  YTNodes = youtubei.YTNodes;

  await initPoTokenMinter();
  // The integrity token expires (~12h); refresh well before that so long-running uptime doesn't degrade.
  setInterval(() => initPoTokenMinter(), 6 * 60 * 60 * 1000);

  client.login(DISCORD_TOKEN);
}

main().catch(err => {
  console.error('Failed to initialize YouTube client:', err);
  process.exit(1);
});
