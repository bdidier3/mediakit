// Éléments communs aux deux pages : sélecteur de langue, plan 2D du parc, icônes.

import { LANGUAGES, currentLanguage, detectLanguage, onLanguageChange, setLanguage, t } from './i18n.js';
import { ISLAND_RADIUS, LAKE_RADIUS, RING, ZONE_LAYOUT } from './park-layout.js';

// Les six stations du parc, dans l'ordre de la visite (sections de l'accueil).
export const STATIONS = [
  { id: 'top', label: 'nav.home' },
  { id: 'about', label: 'nav.about' },
  { id: 'stats', label: 'nav.stats' },
  { id: 'partners', label: 'nav.partners' },
  { id: 'videos', label: 'nav.videos' },
  { id: 'contact', label: 'nav.contact' }
];

const pageOptions = { mapBase: '', mapHere: null };

export const ICONS = {
  tiktok: '<svg viewBox="0 0 24 24" fill="currentColor" aria-hidden="true"><path d="M16.6 5.82a4.83 4.83 0 0 0 2.82 1v3.05a7.8 7.8 0 0 1-2.8-.54v5.86a6.12 6.12 0 1 1-6.13-6.12c.23 0 .46.01.68.04v3.12a3.04 3.04 0 1 0 2.37 2.96V2.5h3.06c.02 1.14.46 2.2 1.2 3.32z"/></svg>',
  youtube: '<svg viewBox="0 0 24 24" fill="currentColor" aria-hidden="true"><path d="M21.8 8.06a2.9 2.9 0 0 0-2.03-2.04C17.98 5.5 12 5.5 12 5.5s-5.98 0-7.77.52A2.9 2.9 0 0 0 2.2 8.06C1.7 9.86 1.7 12 1.7 12s0 2.14.5 3.94a2.9 2.9 0 0 0 2.03 2.04c1.79.52 7.77.52 7.77.52s5.98 0 7.77-.52a2.9 2.9 0 0 0 2.03-2.04c.5-1.8.5-3.94.5-3.94s0-2.14-.5-3.94zM10.1 15.3V8.7L15.8 12l-5.7 3.3z"/></svg>',
  play: '<svg viewBox="0 0 24 24" fill="currentColor" aria-hidden="true"><path d="M8 5.5v13l10.5-6.5z"/></svg>'
};

const escapeMap = { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' };

export function escapeHtml(value) {
  return String(value ?? '').replace(/[&<>"']/g, (char) => escapeMap[char]);
}

// Plan 2D vu de dessus, tracé à partir de la même disposition que le plan 3D.
const toPercent = (value) => ((value + ISLAND_RADIUS + 2) / ((ISLAND_RADIUS + 2) * 2)) * 100;

function parkMapSvg() {
  const spokes = ZONE_LAYOUT.map(({ pos: [x, , z] }) => {
    const angle = Math.atan2(z, x);
    return `<line x1="${(Math.cos(angle) * RING.outer).toFixed(2)}" y1="${(Math.sin(angle) * RING.outer).toFixed(2)}" x2="${x}" y2="${z}"/>`;
  }).join('');
  const trees = Array.from({ length: 26 }, (_, i) => {
    const angle = (i / 26) * Math.PI * 2 + 0.11;
    const radius = 19.5 + ((i * 7) % 4);
    const x = Math.cos(angle) * radius;
    const z = Math.sin(angle) * radius;
    const clear = ZONE_LAYOUT.every(({ pos }) => Math.hypot(pos[0] - x, pos[2] - z) > 6);
    return clear ? `<circle cx="${x.toFixed(2)}" cy="${z.toFixed(2)}" r="1.4"/>` : '';
  }).join('');
  const plinths = ZONE_LAYOUT.map(({ pos: [x, , z], color }) => `<circle cx="${x}" cy="${z}" r="4.2" fill="${color}"/>`).join('');
  const r = ISLAND_RADIUS + 2;
  return `
    <svg viewBox="${-r} ${-r} ${r * 2} ${r * 2}" aria-hidden="true">
      <circle class="park-map-rim" r="${ISLAND_RADIUS + 1.5}"/>
      <circle class="park-map-grass" r="${ISLAND_RADIUS}"/>
      <g class="park-map-trees">${trees}</g>
      <g class="park-map-paths">${spokes}<circle r="${(RING.inner + RING.outer) / 2}"/></g>
      <circle class="park-map-lake" r="${LAKE_RADIUS}"/>
      <circle class="park-map-fountain" r="1.2"/>
      <g class="park-map-plinths">${plinths}</g>
    </svg>`;
}

function renderParkMap(container, here) {
  const hereId = here === 'top' ? 'entrance' : here;
  const links = ZONE_LAYOUT.map((zone) => {
    const [x, , z] = zone.pos;
    const style = `left:${toPercent(x).toFixed(1)}%;top:${toPercent(z).toFixed(1)}%`;
    const isHere = zone.id === hereId ? ' here' : '';
    const badge = zone.link ? ICONS[zone.link.icon] : zone.station;
    const name = zone.link ? zone.link.label : t(`nav.${zone.id}`);
    const attrs = zone.link
      ? `href="${zone.link.href}" target="_blank" rel="noopener noreferrer"`
      : `href="${pageOptions.mapBase}#${zone.id}"`;
    return `<a class="park-map-zone${zone.link ? ` is-${zone.link.icon}` : ''}${isHere}" ${attrs} style="${style}">
      <b>${badge}</b><span>${escapeHtml(name)}</span></a>`;
  }).join('');
  const hereZone = ZONE_LAYOUT.find((zone) => zone.id === hereId);
  const pin = hereZone
    ? `<span class="park-map-pin${hereZone.pos[0] < 8 ? ' is-right' : ''}" style="left:${toPercent(hereZone.pos[0]).toFixed(1)}%;top:${toPercent(hereZone.pos[2]).toFixed(1)}%"><span>${escapeHtml(t('park.here'))}</span></span>`
    : '';
  container.innerHTML = `<div class="park-map">${parkMapSvg()}${links}${pin}</div>`;
}

function renderParkMaps() {
  document.querySelectorAll('[data-park-map]').forEach((container) => {
    const here = container.dataset.here || pageOptions.mapHere;
    renderParkMap(container, here);
  });
}

export const prefersReducedMotion = () => window.matchMedia('(prefers-reduced-motion: reduce)').matches;

function initLanguagePicker() {
  const root = document.querySelector('.lang');
  if (!root) {
    return;
  }

  const button = root.querySelector('.lang-button');
  const menu = root.querySelector('.lang-menu');
  const flag = button.querySelector('.flag');
  const code = button.querySelector('.lang-code');

  menu.innerHTML = Object.entries(LANGUAGES)
    .map(([lang, meta]) => `
      <li><button type="button" data-lang="${lang}" lang="${lang}">
        <img class="flag" src="img/flags/${meta.flag}.svg" alt="" width="20" height="14">${meta.label}
      </button></li>`)
    .join('');

  const setOpen = (open) => {
    menu.classList.toggle('is-open', open);
    button.setAttribute('aria-expanded', String(open));
  };

  const syncButton = (lang) => {
    flag.src = `img/flags/${LANGUAGES[lang].flag}.svg`;
    code.textContent = lang.toUpperCase();
    menu.querySelectorAll('button').forEach((option) => {
      option.setAttribute('aria-current', String(option.dataset.lang === lang));
    });
  };

  button.addEventListener('click', () => setOpen(button.getAttribute('aria-expanded') !== 'true'));
  menu.addEventListener('click', (event) => {
    const option = event.target.closest('button[data-lang]');
    if (!option) {
      return;
    }
    setLanguage(option.dataset.lang);
    setOpen(false);
    button.focus();
  });
  document.addEventListener('click', (event) => {
    if (!root.contains(event.target)) {
      setOpen(false);
    }
  });
  root.addEventListener('keydown', (event) => {
    if (event.key === 'Escape') {
      setOpen(false);
      button.focus();
    }
  });

  onLanguageChange(syncButton);
}

// À appeler une fois par page, après avoir enregistré ses propres onLanguageChange.
// `mapBase` préfixe les liens du plan du parc, `mapHere` place le repère « Vous êtes ici ».
export function initPage(options = {}) {
  Object.assign(pageOptions, options);
  onLanguageChange(renderParkMaps);
  initLanguagePicker();
  setLanguage(detectLanguage());
  return currentLanguage();
}
