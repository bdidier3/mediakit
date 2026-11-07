// Fiche d'un parc (vidéos, totaux, carte), affichée dans le panneau du jeu.
// Ouverte depuis un billet de « Mes vidéos » : #videos/<parc>.

import { formatNumber, onLanguageChange, t, translateDom } from './i18n.js';
import { findPark } from './parks.js';
import { loadVideos, sumStats, videosForPark } from './stats.js';
import { renderFlap } from './effects.js';
import { ICONS, escapeHtml } from './ui.js';

const BATCH_SIZE = 12;
const LAYOUTS = ['stack', 'duo', 'compact'];
const LAYOUT_ICONS = {
  stack: '<svg viewBox="0 0 24 24" fill="currentColor" aria-hidden="true"><path d="M4 6h16v3H4zm0 5h16v3H4zm0 5h16v3H4z"/></svg>',
  duo: '<svg viewBox="0 0 24 24" fill="currentColor" aria-hidden="true"><path d="M3 5h8v14H3zm10 0h8v14h-8z"/></svg>',
  compact: '<svg viewBox="0 0 24 24" fill="currentColor" aria-hidden="true"><path d="M3 4h5v7H3zm6.5 0h5v7h-5zM16 4h5v7h-5zM3 13h5v7H3zm6.5 0h5v7h-5zm6.5 0h5v7h-5z"/></svg>'
};

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

// Les miniatures YouTube sont publiques ; celles de TikTok expirent, le site en
// garde donc une copie (thumbs/tiktok). Sans copie, la vignette reste neutre.
function revealThumbs(root) {
  root.querySelectorAll('.thumb img:not(.is-loaded)').forEach((img) => {
    if (img.complete && img.naturalWidth > 0) {
      img.classList.add('is-loaded');
    } else {
      img.addEventListener('load', () => img.classList.add('is-loaded'), { once: true });
    }
  });
}

function thumbUrl(video) {
  if (video.platform === 'tiktok') {
    return video.thumb ? `thumbs/tiktok/${video.id}.webp` : '';
  }
  const id = video.id || (video.url.match(/[?&]v=([\w-]{6,})/) || [])[1];
  return id ? `https://i.ytimg.com/vi/${id}/hqdefault.jpg` : '';
}

function renderVideoItem(video) {
  const hasLink = /^https?:\/\//i.test(video.url);
  const url = escapeHtml(video.url);
  const linkAttrs = `href="${url}" target="_blank" rel="noopener noreferrer"`;
  const src = thumbUrl(video);
  const image = src ? `<img src="${escapeHtml(src)}" alt="" loading="lazy" decoding="async">` : '';
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

export function createParkView(root) {
  const state = { park: null, videos: null, status: 'idle', view: 'both', layout: readLayout() };
  const listObservers = [];

  root.innerHTML = `
    <a class="back-link park-view-back" href="#videos">
      <svg viewBox="0 0 16 16" aria-hidden="true"><path d="M10 3L5 8l5 5" fill="none" stroke="currentColor" stroke-width="1.8"/></svg>
      <span data-i18n="myVideos.title">Mes vidéos</span>
    </a>
    <header class="park-hero">
      <div>
        <img class="park-logo" alt="">
        <h2 class="park-title display"></h2>
        <p class="park-lead" data-i18n="videos.parkLead"></p>
      </div>
      <div class="park-map-frame">
        <iframe loading="lazy" referrerpolicy="no-referrer-when-downgrade"></iframe>
        <a target="_blank" rel="noopener noreferrer" data-i18n="videos.mapCta">Voir sur Google Maps</a>
      </div>
    </header>
    <div class="park-view-content"></div>`;
  translateDom(root);

  const content = root.querySelector('.park-view-content');
  const title = root.querySelector('.park-title');
  const logo = root.querySelector('.park-logo');
  const frame = root.querySelector('iframe');
  const mapLink = root.querySelector('.park-map-frame a');

  const setupLazyList = (list, videos) => {
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
    // Dans le panneau du jeu, c'est le panneau qui défile : la marge d'avance s'y applique.
    const observer = new IntersectionObserver((entries) => {
      if (!entries.some((entry) => entry.isIntersecting)) {
        return;
      }
      if (!appendBatch()) {
        observer.disconnect();
        sentinel.remove();
        return;
      }
      // Toujours en vue après ce lot (écran haut) : on relance l'observation pour le suivant.
      observer.unobserve(sentinel);
      observer.observe(sentinel);
    }, { root: list.closest('.game-panel-body'), rootMargin: '600px 0px' });
    observer.observe(sentinel);
    listObservers.push(observer);
  };

  const renderSection = (platform, videos) => {
    const label = platform === 'tiktok' ? t('videos.viewTikTok') : t('videos.viewYouTube');
    const body = videos.length === 0
      ? `<p class="state-message">${escapeHtml(t('videos.noVideos'))}</p>`
      : `<ul class="video-list layout-${state.layout}" data-platform="${platform}"></ul><div class="list-sentinel" aria-hidden="true"></div>`;
    return `
      <section class="video-section">
        <h3>${ICONS[platform]}${escapeHtml(label)} <span class="count">(${videos.length})</span></h3>
        ${body}
      </section>`;
  };

  const renderControls = () => {
    const views = [['both', t('videos.viewBoth')], ['tiktok', t('videos.viewTikTok')], ['youtube', t('videos.viewYouTube')]];
    const layoutLabels = { stack: t('videos.layoutStack'), duo: t('videos.layoutDuo'), compact: t('videos.layoutCompact') };
    return `
      <div class="list-controls">
        <div class="segmented" role="group" aria-label="${escapeHtml(t('videos.platformFilter'))}">
          ${views.map(([value, label]) => `<button type="button" data-view="${value}" aria-pressed="${state.view === value}">${escapeHtml(label)}</button>`).join('')}
        </div>
        <div class="segmented segmented--icons" role="group" aria-label="${escapeHtml(t('videos.layoutLabel'))}">
          ${LAYOUTS.map((value) => `<button type="button" data-layout="${value}" aria-pressed="${state.layout === value}" aria-label="${escapeHtml(layoutLabels[value])}" title="${escapeHtml(layoutLabels[value])}">${LAYOUT_ICONS[value]}</button>`).join('')}
        </div>
      </div>`;
  };

  const renderContent = () => {
    listObservers.splice(0).forEach((observer) => observer.disconnect());
    const message = (key) => { content.innerHTML = `<p class="state-message">${escapeHtml(t(key))}</p>`; };
    if (!state.park) {
      message('videos.noParkSelected');
      return;
    }
    if (state.status === 'loading') {
      message('videos.loading');
      return;
    }
    if (state.status === 'error') {
      message('videos.loadError');
      return;
    }
    if (state.videos.tiktok.length + state.videos.youtube.length === 0) {
      message('videos.comingSoon');
      return;
    }

    const showTikTok = state.view !== 'youtube';
    const showYouTube = state.view !== 'tiktok';
    const totals = sumStats([...(showTikTok ? state.videos.tiktok : []), ...(showYouTube ? state.videos.youtube : [])]);

    content.innerHTML = `
      <div class="totals-board">
        <h3 class="totals-title">${escapeHtml(t('videos.totalsTitle'))}</h3>
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
    content.querySelectorAll('.video-list').forEach((list) => setupLazyList(list, state.videos[list.dataset.platform]));
  };

  const renderHeader = () => {
    if (!state.park) {
      title.textContent = t('videos.parkTitle');
      return;
    }
    title.textContent = state.park.name;
    logo.src = state.park.logo;
    logo.alt = state.park.name;
    const query = encodeURIComponent(state.park.map);
    const src = `https://www.google.com/maps?q=${query}&output=embed`;
    if (frame.getAttribute('src') !== src) {
      frame.src = src;
    }
    frame.title = t('videos.mapTitle');
    mapLink.href = `https://www.google.com/maps/search/?api=1&query=${query}`;
  };

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
    if (state.park) {
      renderHeader();
      renderContent();
    }
  });

  return {
    // Affiche la fiche du parc ; renvoie son nom (ou null si le parc est inconnu).
    show(key) {
      const park = findPark(key);
      if (park === state.park && state.status !== 'error') {
        return park?.name ?? null;
      }
      state.park = park;
      state.view = 'both';
      state.status = park ? 'loading' : 'idle';
      renderHeader();
      renderContent();
      if (park) {
        loadVideos()
          .then((all) => {
            if (state.park !== park) {
              return;
            }
            const videos = videosForPark(all, park);
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
          .finally(() => {
            if (state.park === park) {
              renderContent();
            }
          });
      }
      return park?.name ?? null;
    }
  };
}
