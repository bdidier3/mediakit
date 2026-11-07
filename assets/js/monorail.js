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
  // Fête foraine : trois wagons jaunes à toit rouge, hublots ronds, attelés entre eux.
  fair: `
<svg viewBox="0 0 230 46" aria-hidden="true">
  <path class="mono-joint" d="M70 27h8M144 27h8"/>
  <g class="mono-skirt">
    <rect x="8" y="34" width="58" height="7" rx="2"/><rect x="82" y="34" width="58" height="7" rx="2"/><rect x="156" y="34" width="64" height="7" rx="2"/>
  </g>
  <g class="mono-body">
    <rect x="4" y="12" width="66" height="23" rx="5"/>
    <rect x="78" y="12" width="66" height="23" rx="5"/>
    <path d="M152 17c0-3 2-5 5-5h45c14 0 24 9 24 23h-74z"/>
  </g>
  <g class="mono-stripe">
    <rect x="6" y="5" width="62" height="8" rx="4"/>
    <rect x="80" y="5" width="62" height="8" rx="4"/>
    <path d="M154 13c0-4 3-8 8-8h36c8 0 14 3 18 8z"/>
  </g>
  <g class="mono-windows">
    <circle cx="23" cy="23.5" r="5.5"/><circle cx="51" cy="23.5" r="5.5"/>
    <circle cx="97" cy="23.5" r="5.5"/><circle cx="125" cy="23.5" r="5.5"/>
    <circle cx="171" cy="23.5" r="5.5"/>
    <path d="M190 18h10c9 0 16 4 19 11h-29z"/>
  </g>
  <circle class="mono-light" cx="222" cy="31" r="2.4"/>
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
