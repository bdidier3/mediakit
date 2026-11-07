// Mode jeu : tout se passe sur le plan 3D. Entrer dans une attraction zoome
// dessus et ouvre son panneau (le contenu de la section correspondante).
// L'Entrée (TikTok) et la Boutique (YouTube) demandent avant de quitter le parc.
// L'adresse suit l'attraction ouverte (#about, #stats…) : liens directs et
// bouton « précédent » du navigateur fonctionnent.

import { onLanguageChange, t } from './i18n.js';
import { ZONE_LAYOUT } from './park-layout.js';
import { CAR_SVG } from './ride.js';
import { ICONS, prefersReducedMotion } from './ui.js';

const ZONES = ZONE_LAYOUT.map(({ id }) => id);

// Barre de défilement du panneau : un rail vertical sur lequel le wagon
// descend avec la lecture. Clic ou glisser sur le rail pour se déplacer.
// Le wagon glisse en douceur vers sa position (pas de saut quand des images
// se chargent et allongent le contenu).
function initPanelRail(body, rail) {
  const car = rail.querySelector('.panel-rail-car');
  car.innerHTML = CAR_SVG;
  const reduced = prefersReducedMotion();
  let lastTop = 0;
  let target = 0;
  let current = 0;
  let frame = 0;
  let dragging = false;

  const render = () => {
    frame = 0;
    current = reduced || dragging ? target : current + (target - current) * 0.25;
    if (Math.abs(target - current) < 0.3) {
      current = target;
    }
    car.style.transform = `translateY(${current.toFixed(1)}px)`;
    if (current !== target) {
      frame = requestAnimationFrame(render);
    }
  };

  const update = () => {
    const max = body.scrollHeight - body.clientHeight;
    rail.hidden = max <= 4;
    if (max <= 4) {
      return;
    }
    // Le wagon pointe dans le sens de la lecture : vers le bas, ou vers le haut en remontant.
    if (body.scrollTop !== lastTop) {
      car.classList.toggle('is-up', body.scrollTop < lastTop);
      lastTop = body.scrollTop;
    }
    target = (body.scrollTop / max) * (rail.clientHeight - car.offsetHeight);
    if (!frame) {
      frame = requestAnimationFrame(render);
    }
  };

  const scrollToPointer = (event) => {
    const rect = rail.getBoundingClientRect();
    const ratio = Math.min(1, Math.max(0, (event.clientY - rect.top - car.offsetHeight / 2) / (rect.height - car.offsetHeight)));
    body.scrollTop = ratio * (body.scrollHeight - body.clientHeight);
  };

  rail.addEventListener('pointerdown', (event) => {
    event.preventDefault();
    dragging = true;
    rail.setPointerCapture(event.pointerId);
    scrollToPointer(event);
  });
  rail.addEventListener('pointermove', (event) => {
    if (dragging) {
      scrollToPointer(event);
    }
  });
  const stop = () => { dragging = false; };
  rail.addEventListener('pointerup', stop);
  rail.addEventListener('pointercancel', stop);

  body.addEventListener('scroll', update, { passive: true });
  window.addEventListener('resize', update, { passive: true });
  if ('ResizeObserver' in window) {
    // Le contenu change de hauteur (images, vidéo chargées) : on recalcule la position.
    const observer = new ResizeObserver(update);
    observer.observe(body);
    [...body.children].forEach((child) => observer.observe(child));
  }
  return update;
}

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
  const updateRail = initPanelRail(body, panel.querySelector('.panel-rail'));
  const reduced = prefersReducedMotion();
  let current = null;
  let timer = 0;

  const relabel = () => {
    if (!current) {
      return;
    }
    const zone = ZONE_LAYOUT.find(({ id }) => id === current);
    title.textContent = t(`park.zone.${current}`);
    section.textContent = zone.link ? zone.link.label : t(`nav.${current}`);
    station.innerHTML = zone.link ? ICONS[zone.link.icon] : String(zone.station);
    station.classList.toggle('is-tiktok', zone.link?.icon === 'tiktok');
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
    requestAnimationFrame(updateRail);
    // Le panneau arrive pendant que la caméra zoome sur l'attraction.
    timer = setTimeout(() => {
      panel.classList.add('is-open');
      body.focus({ preventScroll: true });
      updateRail();
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
  // Confirmation TikTok / YouTube : « Non » ramène au parc, « Oui » ouvre le compte puis y ramène aussi.
  panel.querySelectorAll('.redirect-stay, .redirect-go').forEach((button) => {
    button.addEventListener('click', () => setTimeout(leave, 0));
  });
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
