// Montagnes russes sur le bord droit, qui remplacent la barre de défilement :
// le wagon suit la progression du scroll, et on peut cliquer ou glisser sur le
// rail pour se déplacer dans la page. Une station par section.

import { t } from './i18n.js';
import { prefersReducedMotion } from './ui.js';

const SVG_NS = 'http://www.w3.org/2000/svg';
const WIDTH = 72;
const CENTER = 40;
const AMPLITUDE = 14;
const SEGMENT = 190;

export const CAR_SVG = `
<svg viewBox="0 0 48 34" aria-hidden="true">
  <circle cx="11" cy="4" r="3.6" class="ride-wheel"/>
  <circle cx="35" cy="4" r="3.6" class="ride-wheel"/>
  <path d="M3 8h36c4 0 7 3 7 7v3c0 2-1.5 3.5-3.5 3.5H6.5C4.5 21.5 3 20 3 18z" class="ride-body"/>
  <path d="M10 21.5v4M26 21.5v4" class="ride-bar"/>
  <circle cx="14" cy="28.5" r="3.4" class="ride-rider"/>
  <circle cx="30" cy="28.5" r="3.4" class="ride-rider"/>
  <path d="M10.5 26l-3 6M17.5 26l3 6M26.5 26l-3 6M33.5 26l3 6" class="ride-arms"/>
</svg>`;

function buildPath(height) {
  const count = Math.max(2, Math.round(height / SEGMENT));
  const step = height / count;
  let d = `M${CENTER} 0 C${CENTER} ${step * 0.35} `;
  for (let i = 1; i <= count; i += 1) {
    const x = i === count ? CENTER : CENTER + (i % 2 ? AMPLITUDE : -AMPLITUDE);
    const y = i * step;
    d += i === 1 ? `${x} ${y - step * 0.45} ${x} ${y} ` : `S${x} ${y - step * 0.45} ${x} ${y} `;
  }
  return d.trim();
}

function svgEl(name, attrs) {
  const el = document.createElementNS(SVG_NS, name);
  Object.entries(attrs).forEach(([key, value]) => el.setAttribute(key, value));
  return el;
}

export function initRide({ stations = [] } = {}) {
  const root = document.createElement('div');
  root.className = 'ride';
  root.setAttribute('aria-hidden', 'true');

  const svg = svgEl('svg', { class: 'ride-track' });
  const maskId = 'ride-rails-mask';
  const mask = svgEl('mask', { id: maskId, maskUnits: 'userSpaceOnUse' });
  const maskOuter = svgEl('path', { stroke: '#fff', 'stroke-width': 13, fill: 'none' });
  const maskInner = svgEl('path', { stroke: '#000', 'stroke-width': 8, fill: 'none' });
  mask.append(maskOuter, maskInner);
  const ties = svgEl('path', { class: 'ride-ties' });
  const rails = svgEl('path', { class: 'ride-rails', mask: `url(#${maskId})` });
  svg.append(svgEl('defs', {}), ties, rails);
  svg.firstChild.append(mask);

  const car = document.createElement('div');
  car.className = 'ride-car';
  car.innerHTML = CAR_SVG;

  const stationLayer = document.createElement('div');
  stationLayer.className = 'ride-stations';

  const hit = document.createElement('div');
  hit.className = 'ride-hit';

  root.append(svg, hit, stationLayer, car);

  // Version mobile : rail horizontal sous l'en-tête, même progression.
  const mini = document.createElement('div');
  mini.className = 'mini-ride';
  mini.setAttribute('aria-hidden', 'true');
  const miniCar = document.createElement('div');
  miniCar.className = 'mini-ride-car';
  miniCar.innerHTML = CAR_SVG;
  mini.append(miniCar);

  document.body.append(root, mini);

  const media = window.matchMedia('(min-width: 1100px)');
  let pathLength = 1;
  let samples = [];
  let dragging = false;
  let stationNodes = [];
  let target = 0;
  let current = 0;
  let frame = 0;

  const progress = () => {
    const max = document.documentElement.scrollHeight - window.innerHeight;
    return max > 0 ? Math.min(1, Math.max(0, window.scrollY / max)) : 0;
  };

  const render = () => {
    frame = 0;
    const smooth = prefersReducedMotion() ? 1 : dragging ? 0.35 : 0.14;
    current += (target - current) * smooth;
    if (Math.abs(target - current) < 0.0005) {
      current = target;
    }
    car.style.offsetDistance = `${(current * 100).toFixed(3)}%`;
    miniCar.style.transform = `translateX(${(current * (mini.clientWidth - miniCar.offsetWidth)).toFixed(1)}px)`;
    stationNodes.forEach((node) => node.classList.toggle('is-passed', current >= Number(node.dataset.at) - 0.002));
    if (current !== target) {
      frame = requestAnimationFrame(render);
    }
  };

  const onScroll = () => {
    target = progress();
    mini.classList.toggle('is-visible', window.scrollY > 8 || document.body.dataset.page !== 'home');
    if (!frame) {
      frame = requestAnimationFrame(render);
    }
  };

  const layout = () => {
    if (!media.matches) {
      return;
    }
    const height = root.clientHeight;
    const d = buildPath(height);
    svg.setAttribute('viewBox', `0 0 ${WIDTH} ${height}`);
    Object.entries({ x: 0, y: 0, width: WIDTH, height }).forEach(([key, value]) => mask.setAttribute(key, value));
    [maskOuter, maskInner, ties, rails].forEach((path) => path.setAttribute('d', d));
    car.style.offsetPath = `path("${d}")`;
    pathLength = rails.getTotalLength();
    samples = Array.from({ length: 241 }, (_, i) => {
      const length = (i / 240) * pathLength;
      return { length, y: rails.getPointAtLength(length).y };
    });

    const max = document.documentElement.scrollHeight - window.innerHeight;
    stationLayer.innerHTML = '';
    stationNodes = stations
      .map(({ id, label }) => {
        const section = document.getElementById(id);
        if (!section || max <= 0) {
          return null;
        }
        const top = section.getBoundingClientRect().top + window.scrollY;
        const at = Math.min(1, Math.max(0, top / max));
        const point = rails.getPointAtLength(at * pathLength);
        const node = document.createElement('span');
        node.className = 'ride-station';
        node.dataset.at = String(at);
        node.style.left = `${point.x}px`;
        node.style.top = `${point.y}px`;
        node.innerHTML = `<span>${t(label)}</span>`;
        stationLayer.append(node);
        return node;
      })
      .filter(Boolean);

    onScroll();
  };

  // Position verticale sur le rail -> progression (0..1). Le tracé descend
  // toujours, donc y croît avec la longueur : interpolation dans la table.
  const fractionAtY = (y) => {
    if (samples.length === 0) {
      return 0;
    }
    if (y <= samples[0].y) {
      return 0;
    }
    const last = samples[samples.length - 1];
    if (y >= last.y) {
      return 1;
    }
    let i = 1;
    while (samples[i].y < y) {
      i += 1;
    }
    const a = samples[i - 1];
    const b = samples[i];
    const ratio = (y - a.y) / (b.y - a.y || 1);
    return (a.length + ratio * (b.length - a.length)) / pathLength;
  };

  const scrollToPointer = (event, behavior) => {
    const rect = root.getBoundingClientRect();
    const max = document.documentElement.scrollHeight - window.innerHeight;
    window.scrollTo({ top: fractionAtY(event.clientY - rect.top) * max, behavior });
  };

  root.addEventListener('pointerdown', (event) => {
    if (event.button !== 0) {
      return;
    }
    event.preventDefault();
    dragging = true;
    root.classList.add('is-dragging');
    event.target.setPointerCapture(event.pointerId);
    scrollToPointer(event, prefersReducedMotion() ? 'instant' : 'smooth');
  });
  root.addEventListener('pointermove', (event) => {
    if (dragging) {
      scrollToPointer(event, 'instant');
    }
  });
  const endDrag = (event) => {
    if (!dragging) {
      return;
    }
    dragging = false;
    root.classList.remove('is-dragging');
    if (event.target.hasPointerCapture?.(event.pointerId)) {
      event.target.releasePointerCapture(event.pointerId);
    }
  };
  root.addEventListener('pointerup', endDrag);
  root.addEventListener('pointercancel', endDrag);

  let resizeTimer = 0;
  const scheduleLayout = () => {
    clearTimeout(resizeTimer);
    resizeTimer = setTimeout(layout, 120);
  };

  window.addEventListener('scroll', onScroll, { passive: true });
  window.addEventListener('resize', scheduleLayout, { passive: true });
  window.addEventListener('load', layout);
  media.addEventListener('change', layout);
  if ('ResizeObserver' in window) {
    new ResizeObserver(scheduleLayout).observe(document.body);
  }
  layout();
  onScroll();

  return { relabel: layout };
}
