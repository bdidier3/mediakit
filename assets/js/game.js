// Mode jeu : tout se passe sur le plan 3D. Entrer dans une attraction zoome
// dessus et ouvre son panneau (le contenu de la section correspondante).
// L'adresse suit l'attraction ouverte (#about, #stats…) : liens directs et
// bouton « précédent » du navigateur fonctionnent.

import { onLanguageChange, t } from './i18n.js';
import { STATIONS, prefersReducedMotion } from './ui.js';

const ZONES = ['about', 'stats', 'partners', 'videos', 'contact'];

export function initGame(getPark) {
  const panel = document.getElementById('game-panel');
  if (!panel) {
    return { refocus() {} };
  }
  const body = panel.querySelector('.game-panel-body');
  const closeButton = panel.querySelector('.game-panel-close');
  const title = document.getElementById('game-panel-title');
  const section = document.getElementById('game-panel-section');
  const station = document.getElementById('game-panel-station');
  const sections = ZONES.map((id) => document.getElementById(id)).filter(Boolean);
  const reduced = prefersReducedMotion();
  let current = null;
  let timer = 0;

  const relabel = () => {
    if (!current) {
      return;
    }
    title.textContent = t(`park.zone.${current}`);
    section.textContent = t(`nav.${current}`);
    station.textContent = String(STATIONS.findIndex(({ id }) => id === current) + 1);
  };

  const open = (id) => {
    clearTimeout(timer);
    const switching = current !== null;
    current = id;
    sections.forEach((el) => { el.hidden = el.id !== id; });
    relabel();
    getPark()?.focusZone(id);
    panel.hidden = false;
    body.scrollTop = 0;
    // Le panneau arrive pendant que la caméra zoome sur l'attraction.
    timer = setTimeout(() => {
      panel.classList.add('is-open');
      body.focus({ preventScroll: true });
    }, reduced || switching ? 0 : 380);
  };

  const close = () => {
    if (current === null) {
      return;
    }
    clearTimeout(timer);
    const previous = current;
    current = null;
    panel.classList.remove('is-open');
    getPark()?.clearFocus();
    timer = setTimeout(() => {
      panel.hidden = true;
      sections.forEach((el) => { el.hidden = true; });
    }, reduced ? 0 : 450);
    document.querySelector(`.park3d-label[data-zone="${previous}"]`)?.focus({ preventScroll: true });
  };

  const route = () => {
    const id = decodeURIComponent(window.location.hash.slice(1));
    if (ZONES.includes(id)) {
      open(id);
    } else {
      close();
    }
  };

  const leave = () => {
    history.pushState(null, '', window.location.pathname + window.location.search);
    route();
  };

  closeButton.addEventListener('click', leave);
  document.addEventListener('keydown', (event) => {
    if (event.key === 'Escape' && current && !document.querySelector('.lang-menu.is-open')) {
      leave();
    }
  });
  window.addEventListener('hashchange', route);
  onLanguageChange(relabel);
  route();

  return {
    // Une fois le plan 3D prêt, il zoome sur l'attraction déjà ouverte (lien direct).
    refocus() {
      if (current) {
        getPark()?.focusZone(current);
      }
    }
  };
}
