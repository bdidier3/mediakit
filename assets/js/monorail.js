// Monorail entre les stations : sur la poutre en haut de chaque section, une
// rame traverse l'écran au rythme du scroll. On descend : elle va vers la
// droite ; on remonte : elle repart vers la gauche. Elle disparaît dans un
// tunnel avant le rail vertical, pour ne jamais le masquer.

import { prefersReducedMotion } from './ui.js';

// Une rame différente à chaque station (attribut data-mono de la section).
const TRAINS = {
  // Classique : trois voitures blanches à bande rouge.
  classic: `
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
</svg>`,
  // Bolide : une seule voiture profilée bleu ciel, nez effilé, bande jaune.
  bullet: `
<svg viewBox="0 0 230 46" aria-hidden="true">
  <path class="mono-body" d="M8 34V20C8 11 16 6 28 6h118c34 0 62 12 80 28z"/>
  <path class="mono-stripe" d="M8 26h196c8 2 14 5 20 8H8z"/>
  <path class="mono-skirt" d="M12 34h212v8H12z"/>
  <path class="mono-windows" d="M22 11h132c18 0 34 4 46 10H22z"/>
  <path class="mono-joint" d="M60 11v10M100 11v10M140 11v10"/>
  <circle class="mono-light" cx="220" cy="31" r="2.4"/>
</svg>`,
  // Fête foraine : petits wagons jaunes à toit rouge et hublots ronds.
  fair: `
<svg viewBox="0 0 230 46" aria-hidden="true">
  <g class="mono-body">
    <rect x="6" y="10" width="64" height="26" rx="7"/>
    <rect x="80" y="10" width="64" height="26" rx="7"/>
    <path d="M154 10h44c14 0 26 10 26 26h-70z"/>
  </g>
  <g class="mono-stripe">
    <path d="M4 12c0-6 4-9 9-9h50c5 0 9 3 9 9z"/><path d="M78 12c0-6 4-9 9-9h50c5 0 9 3 9 9z"/><path d="M152 12c0-6 4-9 9-9h40c8 0 14 3 18 9z"/>
  </g>
  <path class="mono-skirt" d="M10 34h214v8H10z"/>
  <g class="mono-windows">
    <circle cx="24" cy="23" r="6"/><circle cx="52" cy="23" r="6"/><circle cx="98" cy="23" r="6"/><circle cx="126" cy="23" r="6"/><circle cx="172" cy="23" r="6"/><path d="M190 17h8c8 0 14 4 16 10h-24z"/>
  </g>
  <path class="mono-joint" d="M70 26h10M144 26h10"/>
  <circle class="mono-light" cx="221" cy="31" r="2.4"/>
</svg>`,
  // Nuit : rame bleu nuit aux fenêtres allumées.
  night: `
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
</svg>`
};

export function initMonorails(sections) {
  const reduced = prefersReducedMotion();
  const lines = [...sections].map((section) => {
    const line = document.createElement('div');
    line.className = 'monorail';
    line.setAttribute('aria-hidden', 'true');
    const variant = TRAINS[section.dataset.mono] ? section.dataset.mono : 'classic';
    line.innerHTML = `<div class="monorail-train monorail-train--${variant}">${TRAINS[variant]}</div>
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
