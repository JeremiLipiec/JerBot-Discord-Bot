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
  return spawn('yt-dlp', [url, '-f', 'bestaudio', '--no-playlist', '-o', '-', '--quiet']);
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
  clearTimeout(session.idleTimer);
  session.idleTimer = setTimeout(() => {
    const s = sessions.get(guildId);
    if (s) {
      s.textChannel.send('No songs played for 5 minutes, disconnecting.').catch(() => {});
      s.ytProc?.kill();
      s.player.stop(true);
      s.connection.destroy();
      sessions.delete(guildId);
    }
  }, IDLE_TIMEOUT_MS);
}

async function playNext(guildId) {
  const session = sessions.get(guildId);
  if (!session) return;

  if (session.queue.length === 0) {
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
  player.on(AudioPlayerStatus.Idle, () => playNext(interaction.guildId));
  player.on('error', err => {
    console.error('Player error:', err.message);
    playNext(interaction.guildId);
  });
  connection.subscribe(player);

  const session = {
    player,
    connection,
    queue: [],
    ytProc: null,
    currentTrack: null,
    textChannel: interaction.channel,
  };
  sessions.set(interaction.guildId, session);
  return session;
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

client.login(DISCORD_TOKEN);
