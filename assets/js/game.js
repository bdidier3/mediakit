// Mode jeu : tout se passe sur le plan 3D. Entrer dans une attraction zoome
// dessus et ouvre son panneau (le contenu de la section correspondante).
// L'Entrée (TikTok) et la Boutique (YouTube) demandent avant de quitter le parc.
// L'adresse suit l'attraction ouverte (#about, #stats…) : liens directs et
// bouton « précédent » du navigateur fonctionnent.

import { onLanguageChange, t } from './i18n.js';
import { ZONE_LAYOUT } from './park-layout.js';
import { CAR_SVG } from './ride.js';
import { ICONS, prefersReducedMotion } from './ui.js';
import { createParkView } from './park-view.js';

const ZONES = ZONE_LAYOUT.map(({ id }) => id);

// Défilement doux du panneau : la molette et le rail fixent une cible, et le
// contenu glisse vers elle image par image (au lieu de sauter par paliers).
function initSmoothScroll(body) {
  const reduced = prefersReducedMotion();
  let target = 0;
  let frame = 0;

  const step = () => {
    const distance = target - body.scrollTop;
    if (Math.abs(distance) < 0.5) {
      body.scrollTop = target;
      frame = 0;
      return;
    }
    body.scrollTop += distance * 0.16;
    frame = requestAnimationFrame(step);
  };

  const scrollTo = (top) => {
    const max = body.scrollHeight - body.clientHeight;
    target = Math.min(max, Math.max(0, top));
    if (reduced) {
      body.scrollTop = target;
      return;
    }
    if (!frame) {
      frame = requestAnimationFrame(step);
    }
  };

  // Arrête la glisse en cours (glisser sur le rail, toucher, clavier) sans à-coup.
  const stop = () => {
    cancelAnimationFrame(frame);
    frame = 0;
    target = body.scrollTop;
  };

  body.addEventListener('wheel', (event) => {
    // Zoom (Ctrl) et défilement horizontal du carrousel : on laisse faire le navigateur.
    if (reduced || event.ctrlKey || Math.abs(event.deltaX) > Math.abs(event.deltaY)) {
      return;
    }
    event.preventDefault();
    const unit = event.deltaMode === 1 ? 16 : event.deltaMode === 2 ? body.clientHeight : 1;
    scrollTo((frame ? target : body.scrollTop) + event.deltaY * unit);
  }, { passive: false });
  body.addEventListener('touchstart', stop, { passive: true });
  body.addEventListener('keydown', stop);

  return { scrollTo, stop };
}

// Barre de défilement du panneau : un rail vertical sur lequel le wagon
// descend avec la lecture. Clic pour y glisser, glisser pour suivre le doigt.
// Le wagon suit sa position en douceur (pas de saut quand des images se
// chargent et allongent le contenu).
function initPanelRail(body, rail, smooth) {
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
    current = reduced || dragging ? target : current + (target - current) * 0.35;
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
    if (Math.abs(body.scrollTop - lastTop) > 1) {
      car.classList.toggle('is-up', body.scrollTop < lastTop);
      lastTop = body.scrollTop;
    }
    target = (body.scrollTop / max) * (rail.clientHeight - car.offsetHeight);
    if (!frame) {
      frame = requestAnimationFrame(render);
    }
  };

  const topAtPointer = (event) => {
    const rect = rail.getBoundingClientRect();
    const ratio = Math.min(1, Math.max(0, (event.clientY - rect.top - car.offsetHeight / 2) / (rect.height - car.offsetHeight)));
    return ratio * (body.scrollHeight - body.clientHeight);
  };

  rail.addEventListener('pointerdown', (event) => {
    event.preventDefault();
    dragging = true;
    try {
      rail.setPointerCapture(event.pointerId);
    } catch {
      // Pointeur déjà relâché : le clic suffit, sans capture.
    }
    smooth.scrollTo(topAtPointer(event));
  });
  rail.addEventListener('pointermove', (event) => {
    if (dragging && event.buttons) {
      smooth.stop();
      body.scrollTop = topAtPointer(event);
    }
  });
  const release = () => { dragging = false; };
  rail.addEventListener('pointerup', release);
  rail.addEventListener('pointercancel', release);

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
  // Fiche d'un parc : remplace « Mes vidéos » dans le panneau (#videos/<parc>).
  const detail = document.getElementById('park-detail');
  const parkView = detail ? createParkView(detail.querySelector('[data-park-view]')) : null;
  let parkName = null;
  const smooth = initSmoothScroll(body);
  const updateRail = initPanelRail(body, panel.querySelector('.panel-rail'), smooth);
  const reduced = prefersReducedMotion();
  let current = null;
  let timer = 0;

  const relabel = () => {
    if (!current) {
      return;
    }
    const zone = ZONE_LAYOUT.find(({ id }) => id === current);
    title.textContent = t(`park.zone.${current}`);
    section.textContent = parkName || (zone.link ? zone.link.label : t(`nav.${current}`));
    station.innerHTML = zone.link ? ICONS[zone.link.icon] : String(zone.station);
    station.classList.toggle('is-tiktok', zone.link?.icon === 'tiktok');
  };

  // Remplit le panneau avec le contenu de l'attraction (ou la fiche d'un parc).
  const fill = (id, sub) => {
    const showDetail = id === 'videos' && sub && parkView;
    parkName = showDetail ? parkView.show(sub) : null;
    sections.forEach((el) => { el.hidden = showDetail || el.id !== id; });
    if (detail) {
      detail.hidden = !showDetail;
    }
    relabel();
    smooth.stop();
    body.scrollTop = 0;
    requestAnimationFrame(updateRail);
  };

  const open = (id, sub = '') => {
    clearTimeout(timer);
    const switching = current !== null && panel.classList.contains('is-open');
    const sameZone = current === id;
    current = id;
    if (!sameZone) {
      getPark()?.focusZone(id);
    }
    panel.hidden = false;
    if (switching && !reduced) {
      // D'une attraction à l'autre : l'ancien contenu s'efface, le nouveau glisse en place
      // pendant que la caméra vole vers la nouvelle attraction.
      panel.classList.add('is-swapping');
      timer = setTimeout(() => {
        fill(id, sub);
        panel.classList.remove('is-swapping');
        body.focus({ preventScroll: true });
      }, 200);
      return;
    }
    fill(id, sub);
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
    panel.classList.remove('is-open', 'is-swapping');
    getPark()?.clearFocus();
    timer = setTimeout(() => {
      panel.hidden = true;
      sections.forEach((el) => { el.hidden = true; });
      if (detail) {
        detail.hidden = true;
      }
    }, reduced ? 0 : 450);
    document.querySelector(`.park3d-label[data-zone="${previous}"]`)?.focus({ preventScroll: true });
  };

  const route = () => {
    const [id, sub = ''] = decodeURIComponent(window.location.hash.slice(1)).split('/');
    if (ZONES.includes(id)) {
      open(id, sub);
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
