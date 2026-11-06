import { formatNumber, onLanguageChange, t } from './i18n.js';
import { findPark } from './parks.js';
import { loadVideos, sumStats, videosForPark } from './stats.js';
import { renderFlap } from './effects.js';
import { initRide } from './ride.js';
import { ICONS, escapeHtml, initPage } from './ui.js';

const BATCH_SIZE = window.matchMedia('(max-width: 760px)').matches ? 8 : 12;
const LAYOUTS = ['stack', 'duo', 'compact'];
const LAYOUT_ICONS = {
  stack: '<svg viewBox="0 0 24 24" fill="currentColor" aria-hidden="true"><path d="M4 6h16v3H4zm0 5h16v3H4zm0 5h16v3H4z"/></svg>',
  duo: '<svg viewBox="0 0 24 24" fill="currentColor" aria-hidden="true"><path d="M3 5h8v14H3zm10 0h8v14h-8z"/></svg>',
  compact: '<svg viewBox="0 0 24 24" fill="currentColor" aria-hidden="true"><path d="M3 4h5v7H3zm6.5 0h5v7h-5zM16 4h5v7h-5zM3 13h5v7H3zm6.5 0h5v7h-5zm6.5 0h5v7h-5z"/></svg>'
};

const content = document.getElementById('content');
const state = {
  park: findPark(new URLSearchParams(window.location.search).get('park')),
  videos: null,
  status: 'loading',
  view: 'both',
  layout: readLayout()
};
const listObservers = [];

function readLayout() {
  try {
    const saved = localStorage.getItem('videoLayout');
    return LAYOUTS.includes(saved) ? saved : 'stack';
  } catch {
    return 'stack';
  }
}

function saveLayout(layout) {
  try {
    localStorage.setItem('videoLayout', layout);
  } catch {
    // Préférence d'affichage non mémorisée : sans conséquence.
  }
}

// Les miniatures YouTube sont publiques ; TikTok ne fournit plus de miniature
// via oEmbed, les vidéos TikTok gardent donc une vignette neutre.
function revealThumbs(root) {
  root.querySelectorAll('.thumb img:not(.is-loaded)').forEach((img) => {
    if (img.complete && img.naturalWidth > 0) {
      img.classList.add('is-loaded');
    } else {
      img.addEventListener('load', () => img.classList.add('is-loaded'), { once: true });
    }
  });
}

/* ---------- Rendu ---------- */

function youtubeThumb(video) {
  const id = video.id || (video.url.match(/[?&]v=([\w-]{6,})/) || [])[1];
  return id ? `https://i.ytimg.com/vi/${id}/hqdefault.jpg` : '';
}

function renderVideoItem(video) {
  const hasLink = /^https?:\/\//i.test(video.url);
  const url = escapeHtml(video.url);
  const linkAttrs = `href="${url}" target="_blank" rel="noopener noreferrer"`;
  const image = video.platform === 'youtube' && youtubeThumb(video)
    ? `<img src="${escapeHtml(youtubeThumb(video))}" alt="" loading="lazy" decoding="async">`
    : '';
  const thumb = hasLink
    ? `<a class="thumb thumb--${video.platform}" ${linkAttrs} tabindex="-1" aria-hidden="true">${ICONS[video.platform] || ICONS.play}${image}</a>`
    : `<span class="thumb thumb--${video.platform}">${ICONS[video.platform] || ICONS.play}${image}</span>`;
  const title = hasLink
    ? `<a class="video-title" ${linkAttrs}>${escapeHtml(video.title)}</a>`
    : `<span class="video-title">${escapeHtml(video.title)}</span><span class="video-tags">${escapeHtml(t('videos.linkUnavailable'))}</span>`;
  const tags = video.hashtags.length > 0 ? video.hashtags.join(' ') : t('videos.noHashtag');

  return `
    <li class="video-item">
      ${thumb}
      <div>
        ${title}
        <p class="video-meta">
          <span><b>${formatNumber(video.views)}</b> ${escapeHtml(t('videos.viewsUnit'))}</span>
          <span><b>${formatNumber(video.likes)}</b> ${escapeHtml(t('videos.likesUnit'))}</span>
          <span><b>${formatNumber(video.comments)}</b> ${escapeHtml(t('videos.commentsUnit'))}</span>
        </p>
        <p class="video-tags">${escapeHtml(tags)}</p>
      </div>
    </li>`;
}

function setupLazyList(list, videos) {
  const sentinel = list.nextElementSibling;
  let offset = 0;

  const appendBatch = () => {
    const chunk = videos.slice(offset, offset + BATCH_SIZE);
    list.insertAdjacentHTML('beforeend', chunk.map(renderVideoItem).join(''));
    offset += chunk.length;
    revealThumbs(list);
    return offset < videos.length;
  };

  if (!appendBatch()) {
    sentinel.remove();
    return;
  }

  const observer = new IntersectionObserver((entries) => {
    if (entries.some((entry) => entry.isIntersecting) && !appendBatch()) {
      observer.disconnect();
      sentinel.remove();
    }
  }, { rootMargin: '600px 0px' });
  observer.observe(sentinel);
  listObservers.push(observer);
}

function renderSection(platform, videos) {
  const label = platform === 'tiktok' ? t('videos.viewTikTok') : t('videos.viewYouTube');
  const body = videos.length === 0
    ? `<p class="state-message">${escapeHtml(t('videos.noVideos'))}</p>`
    : `<ul class="video-list layout-${state.layout}" data-platform="${platform}"></ul><div class="list-sentinel" aria-hidden="true"></div>`;
  return `
    <section class="video-section">
      <h2>${ICONS[platform]}${escapeHtml(label)} <span class="count">(${videos.length})</span></h2>
      ${body}
    </section>`;
}

function renderControls() {
  const views = [
    ['both', t('videos.viewBoth')],
    ['tiktok', t('videos.viewTikTok')],
    ['youtube', t('videos.viewYouTube')]
  ];
  const layoutLabels = { stack: t('videos.layoutStack'), duo: t('videos.layoutDuo'), compact: t('videos.layoutCompact') };

  return `
    <div class="list-controls">
      <div class="segmented" role="group" aria-label="${escapeHtml(t('videos.platformFilter'))}">
        ${views.map(([value, label]) => `
          <button type="button" data-view="${value}" aria-pressed="${state.view === value}">${escapeHtml(label)}</button>`).join('')}
      </div>
      <div class="segmented segmented--icons" role="group" aria-label="${escapeHtml(t('videos.layoutLabel'))}">
        ${LAYOUTS.map((value) => `
          <button type="button" data-layout="${value}" aria-pressed="${state.layout === value}" aria-label="${escapeHtml(layoutLabels[value])}" title="${escapeHtml(layoutLabels[value])}">${LAYOUT_ICONS[value]}</button>`).join('')}
      </div>
    </div>`;
}

function renderContent() {
  listObservers.splice(0).forEach((observer) => observer.disconnect());

  if (!state.park) {
    content.innerHTML = `<p class="state-message">${escapeHtml(t('videos.noParkSelected'))}</p>`;
    return;
  }
  if (state.status === 'loading') {
    content.innerHTML = `<p class="state-message">${escapeHtml(t('videos.loading'))}</p>`;
    return;
  }
  if (state.status === 'error') {
    content.innerHTML = `<p class="state-message">${escapeHtml(t('videos.loadError'))}</p>`;
    return;
  }
  if (state.videos.tiktok.length + state.videos.youtube.length === 0) {
    content.innerHTML = `<p class="state-message">${escapeHtml(t('videos.comingSoon'))}</p>`;
    return;
  }

  const showTikTok = state.view !== 'youtube';
  const showYouTube = state.view !== 'tiktok';
  const totals = sumStats([
    ...(showTikTok ? state.videos.tiktok : []),
    ...(showYouTube ? state.videos.youtube : [])
  ]);

  content.innerHTML = `
    <div class="totals-board">
      <h2 class="totals-title">${escapeHtml(t('videos.totalsTitle'))}</h2>
      <div class="totals">
        <div><p class="flap" data-flap="${formatNumber(totals.videos)}"></p><span class="totals-label">${escapeHtml(t('videos.totalVideos'))}</span></div>
        <div><p class="flap" data-flap="${formatNumber(totals.views)}"></p><span class="totals-label">${escapeHtml(t('videos.totalViews'))}</span></div>
        <div><p class="flap" data-flap="${formatNumber(totals.likes)}"></p><span class="totals-label">${escapeHtml(t('videos.totalLikes'))}</span></div>
        <div><p class="flap" data-flap="${formatNumber(totals.comments)}"></p><span class="totals-label">${escapeHtml(t('videos.totalComments'))}</span></div>
      </div>
    </div>
    ${renderControls()}
    ${showTikTok ? renderSection('tiktok', state.videos.tiktok) : ''}
    ${showYouTube ? renderSection('youtube', state.videos.youtube) : ''}`;

  content.querySelectorAll('.flap').forEach((flap) => renderFlap(flap, flap.dataset.flap, true));
  content.querySelectorAll('.video-list').forEach((list) => {
    setupLazyList(list, state.videos[list.dataset.platform]);
  });
}

function renderParkHeader() {
  const title = document.getElementById('park-title');
  const logo = document.getElementById('park-logo');
  const mapWrap = document.getElementById('park-map');
  const hero = document.querySelector('.park-hero');

  if (!state.park) {
    title.textContent = t('videos.parkTitle');
    return;
  }

  title.textContent = state.park.name;
  document.title = `${state.park.name} | Clip2EP`;
  logo.src = state.park.logo;
  logo.alt = state.park.name;
  logo.hidden = false;

  const query = encodeURIComponent(state.park.map);
  const frame = mapWrap.querySelector('iframe');
  if (!frame.src) {
    frame.src = `https://www.google.com/maps?q=${query}&output=embed`;
  }
  mapWrap.querySelector('a').href = `https://www.google.com/maps/search/?api=1&query=${query}`;
  mapWrap.hidden = false;
  hero.classList.remove('no-map');
}

content.addEventListener('click', (event) => {
  const viewButton = event.target.closest('button[data-view]');
  const layoutButton = event.target.closest('button[data-layout]');
  if (viewButton && viewButton.dataset.view !== state.view) {
    state.view = viewButton.dataset.view;
    renderContent();
  } else if (layoutButton && layoutButton.dataset.layout !== state.layout) {
    state.layout = layoutButton.dataset.layout;
    saveLayout(state.layout);
    renderContent();
  }
});

onLanguageChange(() => {
  renderParkHeader();
  renderContent();
});
initPage({ mapBase: 'index.html', mapHere: 'videos' });
initRide();

if (state.park) {
  loadVideos()
    .then((all) => {
      const videos = videosForPark(all, state.park);
      state.videos = {
        tiktok: videos.filter((video) => video.platform === 'tiktok'),
        youtube: videos.filter((video) => video.platform === 'youtube')
      };
      state.status = 'ready';
    })
    .catch((error) => {
      console.error('Stats indisponibles :', error);
      state.status = 'error';
    })
    .finally(renderContent);
}
