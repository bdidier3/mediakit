// Chargement du fichier de stats (généré chaque jour par la GitHub Action)
// et calculs par parc. Le fichier n'est téléchargé qu'une fois par page.

const STATS_URL = 'stats/stats_compact.json';
const US_TAG = '#etatsunis🇺🇸';

let statsPromise = null;

// Fichier complet : vidéos normalisées + chiffres des comptes (abonnés, vues).
export function loadStats() {
  if (!statsPromise) {
    statsPromise = fetch(STATS_URL)
      .then((response) => {
        if (!response.ok) {
          throw new Error(`HTTP ${response.status}`);
        }
        return response.json();
      })
      .then((raw) => ({
        videos: Array.isArray(raw?.videos) ? raw.videos.map(normalizeVideo) : [],
        channels: raw?.channels && typeof raw.channels === 'object' ? raw.channels : {}
      }))
      .catch((error) => {
        statsPromise = null;
        throw error;
      });
  }
  return statsPromise;
}

export function loadVideos() {
  return loadStats().then(({ videos }) => videos);
}

function extractHashtags(text) {
  const seen = new Set();
  const tags = [];
  for (const tag of text.match(/#[^\s#]+/g) || []) {
    const key = tag.toLowerCase();
    if (!seen.has(key)) {
      seen.add(key);
      tags.push(tag);
    }
  }
  return tags;
}

function normalizeVideo(item) {
  const platform = String(item.platform || 'tiktok').toLowerCase();
  const id = String(item.id || '');
  const title = String(item.title || item.description || '');
  const description = String(item.description || '');
  let url = String(item.url || '').trim();

  if (!url && id) {
    url = platform === 'youtube'
      ? `https://www.youtube.com/watch?v=${id}`
      : `https://www.tiktok.com/@clip2ep.fan/video/${id}`;
  }

  const hashtags = extractHashtags(`${title} ${description}`);

  return {
    platform,
    id,
    url,
    title,
    description,
    hashtags,
    tagSet: new Set(hashtags.map((tag) => tag.toLowerCase())),
    searchText: `${title} ${description}`.toLowerCase(),
    views: Number(item.views) || 0,
    likes: Number(item.likes) || 0,
    comments: Number(item.comments) || 0
  };
}

function canonicalUrl(video) {
  return video.platform === 'tiktok' ? video.url.split('?')[0] : video.url;
}

// Une vidéo = une plateforme + un identifiant (une même vidéo YouTube peut
// apparaître sous /shorts/ID et watch?v=ID dans le fichier de stats).
function videoKey(video) {
  return video.id ? `${video.platform}:${video.id}` : canonicalUrl(video);
}

function matchesPark(video, park) {
  return park.hashtags.some((tag) => video.tagSet.has(tag))
    || park.keywords.some((keyword) => video.searchText.includes(keyword));
}

// Vidéos d'un parc : rattachées par hashtag ou mot-clé, dédoublonnées
// (on garde la version la plus vue), triées par vues décroissantes.
export function videosForPark(allVideos, park) {
  const byKey = new Map();
  const add = (video) => {
    const key = videoKey(video) || `${video.platform}:${video.title}`;
    const existing = byKey.get(key);
    if (!existing || video.views > existing.views) {
      byKey.set(key, video);
    }
  };

  allVideos.filter((video) => matchesPark(video, park)).forEach(add);

  for (const extraUrl of park.extraVideos || []) {
    const extra = allVideos.find((video) => canonicalUrl(video) === extraUrl);
    if (extra) {
      add(extra);
    }
  }

  let videos = [...byKey.values()];
  if (park.usTag) {
    videos = videos.map((video) => (video.tagSet.has(US_TAG)
      ? video
      : { ...video, hashtags: [...video.hashtags, US_TAG] }));
  }

  return videos.sort((a, b) => b.views - a.views);
}

export function sumStats(videos) {
  return videos.reduce(
    (acc, video) => {
      acc.views += video.views;
      acc.likes += video.likes;
      acc.comments += video.comments;
      return acc;
    },
    { videos: videos.length, views: 0, likes: 0, comments: 0 }
  );
}
