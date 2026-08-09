require('dotenv').config();

const { Client, GatewayIntentBits, REST, Routes, SlashCommandBuilder } = require('discord.js');
const {
  joinVoiceChannel,
  createAudioPlayer,
  createAudioResource,
  AudioPlayerStatus,
  entersState,
  VoiceConnectionStatus,
  StreamType,
} = require('@discordjs/voice');
const { create: createYtDlp } = require('yt-dlp-exec');
const { spawn } = require('child_process');

const ytDlp = createYtDlp('yt-dlp');
const { DISCORD_TOKEN, CLIENT_ID, GUILD_ID, ALLOWED_CHANNEL_ID } = process.env;

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

// guildId -> { player, connection, queue, ytProc, currentTrack, textChannel }
const sessions = new Map();
// guildId -> in-flight getOrCreateSession() promise, so concurrent calls don't create duplicate connections
const pendingSessionCreations = new Map();

async function getVideoInfo(target) {
  const result = await ytDlp(target, {
    dumpSingleJson: true,
    noWarnings: true,
    noCheckCertificates: true,
    preferFreeFormats: true,
    noFlatPlaylist: true,
    addHeader: ['referer:youtube.com', 'user-agent:googlebot'],
  });
  return result.entries?.[0] ?? result;
}

function createYtDlpStream(url) {
  const proc = spawn('yt-dlp', [url, '-f', 'bestaudio', '--no-playlist', '-o', '-', '--quiet']);
  proc.on('error', err => {
    console.error('yt-dlp process error:', err.message);
  });
  return proc;
}

function resolveQuery(input) {
  const trimmed = input.trim();
  if (!trimmed.startsWith('http://') && !trimmed.startsWith('https://')) {
    return `ytsearch1:${trimmed}`;
  }
  try {
    const parsed = new URL(trimmed);
    if (parsed.hostname === 'youtu.be') {
      return `https://www.youtube.com/watch?v=${parsed.pathname.slice(1)}`;
    }
    if (parsed.hostname.includes('youtube.com') && parsed.searchParams.has('v')) {
      return `https://www.youtube.com/watch?v=${parsed.searchParams.get('v')}`;
    }
  } catch {}
  return trimmed;
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
          s.ytProc?.kill();
          s.player.stop(true);
          s.connection.destroy();
          sessions.delete(guildId);
        }
      }
    }, IDLE_TIMEOUT_MS);
  }
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
  session.ytProc?.kill();

  const ytProc = createYtDlpStream(track.url);
  session.ytProc = ytProc;
  session.currentTrack = track;
  session.player.play(createAudioResource(ytProc.stdout, { inputType: StreamType.Arbitrary }));

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
      ytProc: null,
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
    return interaction.reply({ content: 'You need to join a voice channel first!', ephemeral: true });
  }

  await interaction.deferReply();

  try {
    const target = resolveQuery(input);
    const info = await getVideoInfo(target);
    const track = { url: info.webpage_url, title: info.title, duration: info.duration, surprise };
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
      ephemeral: true,
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
        return interaction.reply({ content: 'Nothing is currently playing.', ephemeral: true });
      }
      session.ytProc?.kill();
      session.player.stop(true);
      return interaction.reply(`I'm very sorry :(((`);
    }

    case 'skip': {
      const session = sessions.get(interaction.guildId);
      if (!session || session.player.state.status === AudioPlayerStatus.Idle) {
        return interaction.reply({ content: 'Nothing is currently playing.', ephemeral: true });
      }
      const label = session.currentTrack?.surprise
        ? 'the surprise song'
        : `**${session.currentTrack?.title ?? 'current track'}**`;
      session.ytProc?.kill();
      session.player.stop(true);
      const suffix = session.queue.length > 0 ? '' : ' Queue is empty, disconnecting.';
      return interaction.reply(`Skipped ${label}.${suffix}`);
    }

    case 'pause': {
      const session = sessions.get(interaction.guildId);
      const status = session?.player.state.status;
      if (!session || status === AudioPlayerStatus.Idle) {
        return interaction.reply({ content: 'Nothing is currently playing.', ephemeral: true });
      }
      if (status === AudioPlayerStatus.Paused) {
        return interaction.reply({ content: 'Playback is already paused.', ephemeral: true });
      }
      session.player.pause();
      return interaction.reply('Paused playback.');
    }

    case 'resume': {
      const session = sessions.get(interaction.guildId);
      const status = session?.player.state.status;
      if (!session || status === AudioPlayerStatus.Idle) {
        return interaction.reply({ content: 'Nothing is currently playing.', ephemeral: true });
      }
      if (status !== AudioPlayerStatus.Paused) {
        return interaction.reply({ content: 'Playback is already playing.', ephemeral: true });
      }
      session.player.unpause();
      return interaction.reply('Resumed playback.');
    }

    case 'stop': {
      const session = sessions.get(interaction.guildId);
      if (!session) {
        return interaction.reply({ content: 'Nothing is currently playing.', ephemeral: true });
      }
      clearTimeout(session.idleTimer);
      session.queue.length = 0;
      session.player.stop(true);
      session.ytProc?.kill();
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
	      session.ytProc?.kill();
	      session.player.stop(true);
	      session.connection.destroy();
	      sessions.delete(guildId);
	    }
	  }
	});

	client.login(DISCORD_TOKEN);
