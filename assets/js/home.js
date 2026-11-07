import { formatCompact, onLanguageChange, t, translateDom } from './i18n.js';
import { FILMED_LOGOS, PARKS, PARTNER_LOGOS } from './parks.js';
import { loadStats, loadVideos, sumStats, videosForPark } from './stats.js';
import { renderFlap, revealOnView } from './effects.js';
import { initGame } from './game.js';
import { escapeHtml, initPage, prefersReducedMotion } from './ui.js';

// Le plan 3D est la première chose vue : son module part avant tout le reste.
const park3dModule = import('./park3d.js');

const viewsByPark = new Map();

// Légère inclinaison « posée à la main », stable d'un rendu à l'autre.
const tilt = (index, spread = 4) => (((index * 37) % 9) - 4) * (spread / 4);

/* ---------- Station 4 : tampons du passeport ---------- */

function renderStamps(containerId, logos, labelKey) {
  const container = document.getElementById(containerId);
  if (!container) {
    return;
  }
  container.innerHTML = logos
    .map((park, index) => `
      <li><a class="stamp" href="${escapeHtml(park.url)}" target="_blank" rel="noopener noreferrer"
             style="--r:${tilt(index)}deg;--i:${index}">
        <img class="stamp-flag" src="img/flags/${escapeHtml(park.country)}.svg" alt="" width="24" height="16">
        <img src="${escapeHtml(park.logo)}" alt="${escapeHtml(park.name)}" loading="lazy" decoding="async">
        <span data-i18n="${labelKey}"></span>
      </a></li>`)
    .join('');
  translateDom(container);
}

function initStamps() {
  const groups = document.querySelectorAll('.stamps');
  if (prefersReducedMotion()) {
    return;
  }
  groups.forEach((group) => group.classList.add('is-pending'));
  revealOnView(groups, (group) => {
    group.classList.replace('is-pending', 'is-stamping');
  }, 0.25);
}

/* ---------- Station 5 : billets photo souvenir ---------- */

function renderReel() {
  const reel = document.getElementById('reel');
  if (!reel) {
    return;
  }
  const parks = [...PARKS].sort((a, b) => (viewsByPark.get(b.key) ?? 0) - (viewsByPark.get(a.key) ?? 0));
  reel.innerHTML = parks
    .map((park, index) => {
      const views = viewsByPark.has(park.key) ? formatCompact(viewsByPark.get(park.key)) : '…';
      const number = String(index + 1).padStart(3, '0');
      return `
        <li><a class="ticket" href="videos.html?park=${encodeURIComponent(park.key)}" style="--r:${tilt(index, 2)}deg">
          <div class="ticket-photo">
            <img src="${escapeHtml(park.card)}" alt="" loading="lazy" decoding="async">
            <span class="ticket-no">${escapeHtml(t('park.ticket'))} ${number}</span>
          </div>
          <div class="ticket-stub">
            <img class="ticket-logo" src="${escapeHtml(park.logo)}" alt="" loading="lazy" decoding="async">
            <h3>${escapeHtml(park.name)}</h3>
            <p><span class="views">${views}</span> ${escapeHtml(t('cases.viewsLabel'))}</p>
          </div>
        </a></li>`;
    })
    .join('');
  updateReelButtons();
}

function updateReelButtons() {
  const reel = document.getElementById('reel');
  const prev = document.getElementById('reel-prev');
  const next = document.getElementById('reel-next');
  if (!reel || !prev || !next) {
    return;
  }
  const max = reel.scrollWidth - reel.clientWidth - 2;
  prev.disabled = reel.scrollLeft <= 2;
  next.disabled = reel.scrollLeft >= max;
}

function initReelControls() {
  const reel = document.getElementById('reel');
  const prev = document.getElementById('reel-prev');
  const next = document.getElementById('reel-next');
  if (!reel || !prev || !next) {
    return;
  }

  const step = () => {
    const card = reel.querySelector('li');
    const gap = parseFloat(getComputedStyle(reel).columnGap) || 0;
    const perView = Math.max(1, Math.floor(reel.clientWidth / ((card?.offsetWidth || 240) + gap)) - 1);
    return perView * ((card?.offsetWidth || 240) + gap);
  };
  const behavior = () => (prefersReducedMotion() ? 'auto' : 'smooth');

  prev.addEventListener('click', () => reel.scrollBy({ left: -step(), behavior: behavior() }));
  next.addEventListener('click', () => reel.scrollBy({ left: step(), behavior: behavior() }));
  reel.addEventListener('scroll', updateReelButtons, { passive: true });
  window.addEventListener('resize', updateReelButtons, { passive: true });
}

async function loadParkViews() {
  try {
    const videos = await loadVideos();
    PARKS.forEach((park) => viewsByPark.set(park.key, sumStats(videosForPark(videos, park)).views));
  } catch (error) {
    console.error('Stats indisponibles :', error);
    PARKS.forEach((park) => viewsByPark.set(park.key, 0));
  }
  renderReel();
}

/* ---------- Station 2 : tableau des temps d'attente ---------- */

function initBoards() {
  const flaps = document.querySelectorAll('#stats .flap');
  flaps.forEach((flap) => renderFlap(flap, flap.dataset.flap));
  revealOnView(flaps, (flap) => {
    flap.dataset.revealed = '1';
    renderFlap(flap, flap.dataset.flap, true);
  }, 0.6);

  const breakdowns = document.querySelectorAll('#stats .breakdown');
  if (prefersReducedMotion()) {
    return;
  }
  breakdowns.forEach((group) => group.classList.add('is-pending'));
  revealOnView(breakdowns, (group) => group.classList.remove('is-pending'), 0.35);
}

// Abonnés et vues actualisés chaque jour par la GitHub Action. Si le fichier
// ne les contient pas encore, les valeurs écrites dans la page restent affichées.
async function loadLiveCounters() {
  let channels;
  try {
    ({ channels } = await loadStats());
  } catch {
    return;
  }
  document.querySelectorAll('#stats [data-live]').forEach((flap) => {
    const [platform, field] = flap.dataset.live.split('.');
    const value = Number(channels?.[platform]?.[field]);
    if (!value) {
      return;
    }
    const text = formatCompact(value);
    if (text === flap.dataset.flap) {
      return;
    }
    flap.dataset.flap = text;
    renderFlap(flap, text, flap.dataset.revealed === '1');
  });
}

/* ---------- Station 3 : le badge se balance au rythme du scroll ---------- */

function initBadgeSwing() {
  const badge = document.getElementById('badge');
  if (!badge || prefersReducedMotion()) {
    return;
  }
  let angle = 0;
  let velocity = 0;
  let lastY = window.scrollY;
  let frame = 0;

  const tick = () => {
    frame = 0;
    // Ressort amorti : le badge revient doucement à la verticale.
    velocity += -angle * 0.06;
    velocity *= 0.9;
    angle += velocity;
    badge.style.transform = `rotate(${angle.toFixed(2)}deg)`;
    if (Math.abs(angle) > 0.02 || Math.abs(velocity) > 0.02) {
      frame = requestAnimationFrame(tick);
    } else {
      badge.style.transform = '';
    }
  };

  window.addEventListener('scroll', () => {
    const delta = window.scrollY - lastY;
    lastY = window.scrollY;
    velocity += Math.max(-1.2, Math.min(1.2, delta * 0.03));
    if (!frame) {
      frame = requestAnimationFrame(tick);
    }
  }, { passive: true });
}

/* ---------- Station 1 : plan 3D (Three.js chargé à la demande, plan 2D sinon) ---------- */

let park = null;

async function initParkMap() {
  const root = document.getElementById('park3d');
  if (!root) {
    return;
  }
  try {
    const { initPark3d } = await park3dModule;
    // Entrer dans une attraction = changer l'adresse (#about…) ; le jeu ouvre le panneau.
    park = await initPark3d(root, { onSelect: (id) => { window.location.hash = id; } });
    onLanguageChange(park.relabel);
    game.refocus();
  } catch (error) {
    console.warn('Plan 3D indisponible, affichage du plan 2D :', error);
    root.classList.add('is-fallback');
  }
}

/* ---------- Station 5 : l'écran POV ne tourne que lorsqu'il est visible ---------- */

function initPovVideo() {
  const video = document.getElementById('pov-video');
  if (!video) {
    return;
  }
  if (prefersReducedMotion() || !('IntersectionObserver' in window)) {
    video.controls = true;
    return;
  }
  new IntersectionObserver(([entry]) => {
    if (entry.isIntersecting) {
      video.play().catch(() => {});
    } else {
      video.pause();
    }
  }, { threshold: 0.4 }).observe(video);
}

renderStamps('partner-logos', PARTNER_LOGOS, 'park.stampPartner');
renderStamps('filmed-logos', FILMED_LOGOS, 'park.stampVisited');
onLanguageChange(renderReel);
initPage({ mapBase: '' });
initReelControls();
initBoards();
initBadgeSwing();
initStamps();
initPovVideo();
// Tout se passe sur le ciel de nuit du plan : le bouton de langue reste sans cadre.
document.querySelector('.site-header')?.classList.add('over-hero');
const game = initGame(() => park);

// Le fichier de stats (plusieurs centaines de Ko) attend que le parc soit ouvert,
// pour ne pas ralentir son chargement sur mobile.
initParkMap().finally(() => {
  loadParkViews();
  loadLiveCounters();
});

