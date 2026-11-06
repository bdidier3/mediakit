// Monorail entre les stations : sur la poutre en haut de chaque section, une
// rame traverse l'écran au rythme du scroll. On descend : elle va vers la
// droite ; on remonte : elle repart vers la gauche. Elle disparaît dans un
// tunnel avant le rail vertical, pour ne jamais le masquer.

import { prefersReducedMotion } from './ui.js';

const TRAIN_SVG = `
<svg viewBox="0 0 230 46" aria-hidden="true">
  <path class="mono-body" d="M6 14c0-6 5-10 11-10h150c22 0 40 9 52 22l5 6c2 3 0 6-3 6H10c-2 0-4-2-4-4z"/>
  <path class="mono-stripe" d="M6 26h222l2 4H6z"/>
  <path class="mono-skirt" d="M10 34h214v8H10z"/>
  <g class="mono-windows">
    <rect x="18" y="10" width="22" height="11" rx="3"/><rect x="46" y="10" width="22" height="11" rx="3"/>
    <rect x="80" y="10" width="22" height="11" rx="3"/><rect x="108" y="10" width="22" height="11" rx="3"/>
    <rect x="142" y="10" width="22" height="11" rx="3"/>
    <path d="M172 10h12c10 0 20 5 26 11h-38z"/>
  </g>
  <path class="mono-joint" d="M73 6v28M136 6v28"/>
  <circle class="mono-light" cx="222" cy="29" r="2.4"/>
</svg>`;

export function initMonorails(sections) {
  const reduced = prefersReducedMotion();
  const lines = [...sections].map((section) => {
    const line = document.createElement('div');
    line.className = 'monorail';
    line.setAttribute('aria-hidden', 'true');
    line.innerHTML = `<div class="monorail-train">${TRAIN_SVG}</div>
      <span class="monorail-portal monorail-portal--start"></span>
      <span class="monorail-portal monorail-portal--end"></span>`;
    section.prepend(line);
    return { line, train: line.querySelector('.monorail-train'), target: 0, current: 0, facing: 1 };
  });

  let frame = 0;

  const measure = () => {
    const vh = window.innerHeight;
    lines.forEach((item) => {
      const top = item.line.getBoundingClientRect().bottom;
      // 0 quand la poutre entre par le bas de l'écran, 1 quand elle atteint le haut.
      const progress = (vh * 0.95 - top) / (vh * 0.85);
      item.target = Math.min(1, Math.max(0, progress));
    });
  };

  const render = () => {
    frame = 0;
    let moving = false;
    lines.forEach((item) => {
      const delta = item.target - item.current;
      if (Math.abs(delta) > 0.0004) {
        item.facing = delta > 0 ? 1 : -1;
        item.current = reduced ? item.target : item.current + delta * 0.12;
        moving = moving || !reduced;
      } else {
        item.current = item.target;
      }
      const trackWidth = item.line.clientWidth;
      const trainWidth = item.train.offsetWidth;
      const x = -trainWidth + item.current * (trackWidth + trainWidth);
      item.train.style.transform = `translateX(${x.toFixed(1)}px) scaleX(${item.facing})`;
    });
    if (moving) {
      frame = requestAnimationFrame(render);
    }
  };

  const update = () => {
    measure();
    if (!frame) {
      frame = requestAnimationFrame(render);
    }
  };

  window.addEventListener('scroll', update, { passive: true });
  window.addEventListener('resize', update, { passive: true });
  update();
}
